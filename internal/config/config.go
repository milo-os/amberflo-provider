/*
Copyright 2026 Datum Technology Inc.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, version 3.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.
*/

package config

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:defaulter-gen=true

// AmberfloProvider is the runtime configuration for the amberflo-provider
// controller. It is decoded from the YAML file referenced by
// --server-config.
type AmberfloProvider struct {
	metav1.TypeMeta

	// MetricsServer configures the controller-runtime metrics server.
	MetricsServer MetricsServerConfig `json:"metricsServer"`

	// WebhookServer configures the admission webhook server. When unset,
	// the manager runs without an admission webhook server and no serving
	// cert is required. The provider does not currently ship admission
	// webhooks, but the hook is wired so future validation webhooks can
	// be added without changing main.go.
	WebhookServer *WebhookServerConfig `json:"webhookServer,omitempty"`

	// KubeconfigPath is the path to a kubeconfig for the control plane
	// that serves the billing CRDs (BillingAccount,
	// BillingAccountBinding). When empty the controller falls back to
	// the standard controller-runtime resolution (in-cluster service
	// account or $KUBECONFIG) so local development against a single
	// cluster still works.
	KubeconfigPath string `json:"kubeconfigPath,omitempty"`

	// AmberfloBaseURL is the base URL for the Amberflo control-plane API.
	// Defaults to https://app.amberflo.io. Override for sandbox, dev
	// fakes, or on-prem/regional endpoints.
	AmberfloBaseURL string `json:"amberfloBaseURL,omitempty"`

	// AmberfloAPIKeyPath is the filesystem path where the Amberflo
	// X-API-KEY header value is mounted, typically from a Kubernetes
	// Secret projected into the pod. Defaults to
	// /var/run/secrets/amberflo/api-key.
	AmberfloAPIKeyPath string `json:"amberfloAPIKeyPath,omitempty"`

	// AmberfloRateLimitPerSec caps the token-bucket rate used by the
	// internal Amberflo client. Amberflo does not publish an explicit
	// rate limit; 10 req/s is a conservative default that can be tuned
	// per deployment.
	AmberfloRateLimitPerSec int `json:"amberfloRateLimitPerSec,omitempty"`

	// AllowCustomerDelete permits the controller to hard-delete an
	// Amberflo customer when the corresponding BillingAccount is deleted
	// in Milo. The default is false: the provider soft-disables the
	// customer via a trait and leaves the historical record in place so
	// invoices can still be retrieved. Enable only in non-production
	// environments where data loss is acceptable.
	AllowCustomerDelete bool `json:"allowCustomerDelete,omitempty"`

	// NATSConfig configures the NATS JetStream connection for the
	// submission consumer. When nil, the consumer is not registered and
	// the binary runs in its existing mode without any NATS dependency.
	NATSConfig *NATSConfig `json:"natsConfig,omitempty"`
}

// RestConfig returns the *rest.Config used to connect to the control
// plane that hosts the billing CRDs. When KubeconfigPath is empty it
// falls back to the standard controller-runtime config resolution
// (in-cluster / $KUBECONFIG).
func (c *AmberfloProvider) RestConfig() (*rest.Config, error) {
	if c.KubeconfigPath == "" {
		return ctrl.GetConfig()
	}
	return clientcmd.BuildConfigFromFlags("", c.KubeconfigPath)
}

// SetDefaults_AmberfloProvider sets top-level defaults. Nested defaults
// are applied by the generated SetObjectDefaults_AmberfloProvider.
func SetDefaults_AmberfloProvider(obj *AmberfloProvider) {
	if obj.AmberfloBaseURL == "" {
		obj.AmberfloBaseURL = "https://app.amberflo.io"
	}
	if obj.AmberfloAPIKeyPath == "" {
		obj.AmberfloAPIKeyPath = "/var/run/secrets/amberflo/api-key"
	}
	if obj.AmberfloRateLimitPerSec == 0 {
		obj.AmberfloRateLimitPerSec = 10
	}
}

// +k8s:deepcopy-gen=true

// WebhookServerConfig configures the admission webhook server.
type WebhookServerConfig struct {
	// Host is the address that the server will listen on.
	// Defaults to "" - all addresses.
	Host string `json:"host"`

	// Port is the port number that the server will serve.
	// It will be defaulted to 9443 if unspecified.
	Port int `json:"port"`

	// TLS is the TLS configuration for the webhook server.
	TLS TLSConfig `json:"tls"`

	// ClientCAName is the CA certificate name which server used to verify
	// remote (client)'s certificate.
	ClientCAName string `json:"clientCAName"`
}

func SetDefaults_WebhookServerConfig(obj *WebhookServerConfig) {
	if obj.TLS.CertDir == "" {
		obj.TLS.CertDir = filepath.Join(os.TempDir(), "k8s-webhook-server", "serving-certs")
	}
}

func (c *WebhookServerConfig) Options(ctx context.Context, secretsClient client.Client) webhook.Options {
	opts := webhook.Options{
		Host:     c.Host,
		Port:     c.Port,
		CertDir:  c.TLS.CertDir,
		CertName: c.TLS.CertName,
		KeyName:  c.TLS.KeyName,
	}

	if secretRef := c.TLS.SecretRef; secretRef != nil {
		opts.TLSOpts = c.TLS.Options(ctx, secretsClient)
	}

	return opts
}

// +k8s:deepcopy-gen=true

// MetricsServerConfig configures the metrics server.
type MetricsServerConfig struct {
	// SecureServing enables serving metrics via https.
	SecureServing *bool `json:"secureServing,omitempty"`

	// BindAddress is the bind address for the metrics server.
	BindAddress string `json:"bindAddress"`

	// TLS is the TLS configuration for the metrics server.
	TLS TLSConfig `json:"tls"`
}

func SetDefaults_MetricsServerConfig(obj *MetricsServerConfig) {
	if obj.SecureServing == nil {
		obj.SecureServing = ptr.To(true)
	}

	if obj.BindAddress == "" {
		obj.BindAddress = "0"
	}

	if len(obj.TLS.CertDir) == 0 {
		obj.TLS.CertDir = filepath.Join(os.TempDir(), "k8s-metrics-server", "serving-certs")
	}
}

func (c *MetricsServerConfig) Options(ctx context.Context, secretsClient client.Client) metricsserver.Options {
	secureServing := c.SecureServing != nil && *c.SecureServing

	opts := metricsserver.Options{
		SecureServing: secureServing,
		BindAddress:   c.BindAddress,
		CertDir:       c.TLS.CertDir,
		CertName:      c.TLS.CertName,
		KeyName:       c.TLS.KeyName,
	}

	if secureServing {
		opts.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	if secretRef := c.TLS.SecretRef; secretRef != nil {
		opts.TLSOpts = c.TLS.Options(ctx, secretsClient)
	}

	return opts
}

// +k8s:deepcopy-gen=true

// TLSConfig configures TLS certificate management for either the metrics
// or admission-webhook server.
type TLSConfig struct {
	// SecretRef is a reference to a secret that contains the server key
	// and certificate.
	SecretRef *corev1.ObjectReference `json:"secretRef,omitempty"`

	// CertDir is the directory that contains the server key and
	// certificate.
	CertDir string `json:"certDir"`

	// CertName is the server certificate name. Defaults to tls.crt.
	CertName string `json:"certName"`

	// KeyName is the server key name. Defaults to tls.key.
	KeyName string `json:"keyName"`
}

func (c *TLSConfig) Options(ctx context.Context, secretsClient client.Client) []func(*tls.Config) {
	var tlsOpts []func(*tls.Config)

	if secretRef := c.SecretRef; secretRef != nil {
		tlsOpts = append(tlsOpts, func(c *tls.Config) {
			logger := ctrl.Log.WithName("tls-client")
			c.GetCertificate = func(clientHello *tls.ClientHelloInfo) (*tls.Certificate, error) {
				logger.Info("getting certificate")

				var secret corev1.Secret
				secretObjectKey := types.NamespacedName{
					Name:      secretRef.Name,
					Namespace: secretRef.Namespace,
				}
				if err := secretsClient.Get(ctx, secretObjectKey, &secret); err != nil {
					return nil, fmt.Errorf("failed to get secret: %w", err)
				}

				cert, err := tls.X509KeyPair(secret.Data["tls.crt"], secret.Data["tls.key"])
				if err != nil {
					return nil, fmt.Errorf("failed to parse certificate: %w", err)
				}

				return &cert, nil
			}
		})
	}

	return tlsOpts
}

func SetDefaults_TLSConfig(obj *TLSConfig) {
	if len(obj.CertName) == 0 {
		obj.CertName = "tls.crt"
	}

	if len(obj.KeyName) == 0 {
		obj.KeyName = "tls.key"
	}
}

// +k8s:deepcopy-gen=true

// NATSConfig configures the NATS connection for the submission consumer.
type NATSConfig struct {
	// URL is the NATS server URL, e.g. nats://nats:4222.
	URL string `json:"url"`

	// CAFile is the path to the NATS CA certificate file.
	CAFile string `json:"caFile,omitempty"`

	// CertFile is the path to the NATS client certificate file.
	CertFile string `json:"certFile,omitempty"`

	// KeyFile is the path to the NATS client private key file.
	KeyFile string `json:"keyFile,omitempty"`

	// CredentialsPath is the filesystem path to the NATS credentials
	// file (NKey or JWT credentials). When empty, no credentials are
	// used (anonymous, for local development).
	CredentialsPath string `json:"credentialsPath,omitempty"`
}

func init() {
	SchemeBuilder.Register(&AmberfloProvider{})
}
