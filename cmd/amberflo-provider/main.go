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

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strings"

	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	billingv1alpha1 "go.miloapis.com/billing/api/v1alpha1"
	stripev1alpha1 "go.miloapis.com/stripe-provider/api/v1alpha1"

	"go.miloapis.com/amberflo-provider/internal/amberflo"
	"go.miloapis.com/amberflo-provider/internal/config"
	"go.miloapis.com/amberflo-provider/internal/controller"
	"go.miloapis.com/amberflo-provider/internal/invoice"
	amberflowebhook "go.miloapis.com/amberflo-provider/internal/webhook"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
	codecs   = serializer.NewCodecFactory(scheme, serializer.EnableStrict)

	// Build metadata, set via -ldflags at build time. See Dockerfile and
	// Taskfile.yaml.
	version      = "dev"
	gitCommit    = "unknown"
	gitTreeState = "unknown"
	buildDate    = "unknown"
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(config.AddToScheme(scheme))
	utilruntime.Must(config.RegisterDefaults(scheme))

	utilruntime.Must(billingv1alpha1.AddToScheme(scheme))
	utilruntime.Must(stripev1alpha1.AddToScheme(scheme))

	// +kubebuilder:scaffold:scheme
}

func main() {
	var enableLeaderElection bool
	var leaderElectionNamespace string
	var probeAddr string
	var serverConfigFile string

	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.StringVar(&leaderElectionNamespace, "leader-elect-namespace", "", "The namespace to use for leader election.")
	flag.StringVar(&serverConfigFile, "server-config", "", "Path to the server config file (YAML).")

	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	setupLog.Info("starting amberflo-provider",
		"version", version,
		"gitCommit", gitCommit,
		"gitTreeState", gitTreeState,
		"buildDate", buildDate,
	)

	var serverConfig config.AmberfloProvider
	var configData []byte
	if len(serverConfigFile) > 0 {
		var err error
		configData, err = os.ReadFile(serverConfigFile)
		if err != nil {
			setupLog.Error(fmt.Errorf("unable to read server config from %q", serverConfigFile), "")
			os.Exit(1)
		}
	}

	if err := runtime.DecodeInto(codecs.UniversalDecoder(), configData, &serverConfig); err != nil {
		setupLog.Error(err, "unable to decode server config")
		os.Exit(1)
	}

	setupLog.Info("server config", "config", serverConfig)

	// Billing and services resources live in the Milo control plane, not
	// necessarily in the cluster that hosts this controller pod. Connect
	// to Milo using the configured kubeconfig path, falling back to
	// ctrl.GetConfig() for local / in-cluster development where the two
	// clusters happen to be the same.
	cfg, err := serverConfig.RestConfig()
	if err != nil {
		setupLog.Error(err, "unable to load rest config")
		os.Exit(1)
	}

	ctx := ctrl.SetupSignalHandler()

	// Build a direct (non-cached) client so metrics/webhook TLS option
	// builders that need a Secret reader have one available before the
	// manager cache has started. The actual Get calls happen lazily from
	// inside TLS GetCertificate callbacks.
	bootstrapClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		setupLog.Error(err, "unable to create bootstrap client")
		os.Exit(1)
	}

	// The metrics endpoint's authn/authz filter must validate scrape
	// requests against the LOCAL cluster (where the scraper and this pod
	// live), not the Milo control plane that `cfg` may point at. Resolve
	// the local config via the standard in-cluster / KUBECONFIG resolution.
	metricsAuthConfig, err := ctrl.GetConfig()
	if err != nil {
		setupLog.Error(err, "unable to load local rest config for metrics auth")
		os.Exit(1)
	}

	metricsServerOptions := serverConfig.MetricsServer.Options(ctx, bootstrapClient, metricsAuthConfig)

	var webhookServer webhook.Server
	if serverConfig.WebhookServer != nil {
		webhookServer = webhook.NewServer(
			serverConfig.WebhookServer.Options(ctx, bootstrapClient),
		)
	} else {
		setupLog.Info("webhookServer not configured; admission webhook server disabled")
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                  scheme,
		Metrics:                 metricsServerOptions,
		WebhookServer:           webhookServer,
		HealthProbeBindAddress:  probeAddr,
		LeaderElection:          enableLeaderElection,
		LeaderElectionID:        "amberflo-provider.providers.datum.net",
		LeaderElectionNamespace: leaderElectionNamespace,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Load the Amberflo API key once at startup.
	//
	// Precedence:
	//   1. File path (AmberfloAPIKeyPath in server config) — the standard
	//      production path; the file is typically mounted from a
	//      Kubernetes Secret.
	//   2. Env var AMBERFLO_API_KEY — a local-dev / sandbox ergonomics
	//      escape hatch used by `task test:create-amberflo-credentials`
	//      and direct `go run` invocations.
	//   3. Error out so misconfiguration is loud.
	//
	// The sha256 prefix of the key is logged so rotations are auditable
	// without leaking the secret itself.
	apiKey, keySource, err := loadAPIKey(serverConfig.AmberfloAPIKeyPath, os.Getenv("AMBERFLO_API_KEY"))
	if err != nil {
		setupLog.Error(err, "unable to load Amberflo API key",
			"path", serverConfig.AmberfloAPIKeyPath)
		os.Exit(1)
	}
	setupLog.Info("loaded Amberflo API key",
		"source", keySource,
		"sha256Prefix", keyFingerprint(apiKey),
	)

	amberfloClient, err := amberflo.NewClient(amberflo.ClientOptions{
		BaseURL:         serverConfig.AmberfloBaseURL,
		APIKey:          apiKey,
		RateLimitPerSec: serverConfig.AmberfloRateLimitPerSec,
		UserAgent:       fmt.Sprintf("amberflo-provider/%s", version),
	})
	if err != nil {
		setupLog.Error(err, "unable to construct Amberflo client")
		os.Exit(1)
	}
	// Wrap in the metrics-instrumented client so every outbound call is
	// counted and timed via Prometheus.
	amberfloClient = amberflo.NewInstrumentedClient(amberfloClient)

	invoiceSyncer := &invoice.Syncer{
		Client: mgr.GetClient(),
		Log:    setupLog.WithName("invoice-syncer"),
	}

	if err = (&controller.BillingAccountReconciler{
		Client:                     mgr.GetClient(),
		AmberfloClient:             amberfloClient,
		AllowCustomerDelete:        serverConfig.AllowCustomerDelete,
		InvoiceSyncer:              invoiceSyncer,
		StripePaymentSettingID:     serverConfig.AmberfloStripePaymentSettingID,
		PaymentSwitchMinFutureSkew: serverConfig.AmberfloPaymentSwitchMinFutureSkew.Duration,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "BillingAccount")
		os.Exit(1)
	}

	if err = (&controller.MeterDefinitionReconciler{
		Client:         mgr.GetClient(),
		AmberfloClient: amberfloClient,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "MeterDefinition")
		os.Exit(1)
	}

	if err = controller.AddIndexers(ctx, mgr.GetFieldIndexer()); err != nil {
		setupLog.Error(err, "unable to add indexers")
		os.Exit(1)
	}

	if webhookServer != nil {
		webhookSecret, secretSource, secretErr := loadWebhookSecret(
			serverConfig.AmberfloWebhookSigningSecretPath,
			os.Getenv("AMBERFLO_WEBHOOK_SECRET"),
		)
		if secretErr != nil {
			setupLog.Error(secretErr, "unable to load Amberflo webhook signing secret; invoice webhook disabled",
				"path", serverConfig.AmberfloWebhookSigningSecretPath)
		} else {
			setupLog.Info("loaded Amberflo webhook signing secret",
				"source", secretSource,
				"sha256Prefix", keyFingerprint(webhookSecret),
			)
			if err = (&amberflowebhook.InvoiceHandler{
				Client:         mgr.GetClient(),
				AmberfloClient: amberfloClient,
				Syncer:         invoiceSyncer,
				Secret:         webhookSecret,
				SecretHeader:   serverConfig.AmberfloWebhookSecretHeader,
			}).SetupWithManager(mgr); err != nil {
				setupLog.Error(err, "unable to register Amberflo invoice webhook")
				os.Exit(1)
			}
		}
	} else {
		setupLog.Info("webhookServer not configured; Amberflo invoice webhook disabled")
	}

	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}
}

// loadAPIKey resolves the Amberflo API key using the documented
// precedence:
//
//  1. File at path (production / Secret mount) — used when the path is
//     set and the file is present and non-empty.
//  2. Env var AMBERFLO_API_KEY — used when the file path is unset OR
//     when the file is not present. This is the local-dev / sandbox
//     escape hatch and lets the task system inject the key via a
//     `dotenv:` loaded .env file.
//  3. Error — neither a readable file nor a non-empty env var.
//
// If the path is set and the file exists but is empty/malformed we
// return the error rather than silently falling back — a misconfigured
// mount should fail loudly, not mask production credentials with a
// local one.
//
// Returns the trimmed key and a short source label for logging
// ("file:<path>" or "env:AMBERFLO_API_KEY").
func loadAPIKey(path, envValue string) (string, string, error) {
	return loadSecretValue(path, envValue, "AMBERFLO_API_KEY", "Amberflo API key")
}

// loadWebhookSecret resolves the shared secret used to authenticate
// inbound Amberflo invoice webhooks. Precedence mirrors loadAPIKey with
// env var AMBERFLO_WEBHOOK_SECRET.
func loadWebhookSecret(path, envValue string) (string, string, error) {
	return loadSecretValue(path, envValue, "AMBERFLO_WEBHOOK_SECRET", "Amberflo webhook signing secret")
}

// loadSecretValue is the shared file-then-env loader used for Amberflo
// credentials. See loadAPIKey for the precedence rules.
func loadSecretValue(path, envValue, envName, label string) (string, string, error) {
	envKey := strings.TrimSpace(envValue)

	if path != "" {
		raw, err := os.ReadFile(path)
		switch {
		case err == nil:
			key := strings.TrimSpace(string(raw))
			if key == "" {
				return "", "", fmt.Errorf("%q contains no %s after trimming whitespace", path, label)
			}
			return key, "file:" + path, nil
		case os.IsNotExist(err):
			// Fall through to env var — common for local dev where the
			// default path points to a Secret mount that doesn't exist.
		default:
			return "", "", fmt.Errorf("read %q: %w", path, err)
		}
	}

	if envKey != "" {
		return envKey, "env:" + envName, nil
	}
	return "", "", fmt.Errorf(
		"no %s available: set server-config path to a readable file or export %s",
		label, envName,
	)
}

// keyFingerprint returns the first 8 hex chars of sha256(key). Intended
// for audit logs so operators can correlate a running pod with which
// version of the Secret it loaded, without leaking the key itself.
func keyFingerprint(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])[:8]
}
