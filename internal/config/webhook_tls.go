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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"sync"
	"time"
)

var (
	ephemeralWebhookCertOnce sync.Once
	ephemeralWebhookCert     tls.Certificate
	ephemeralWebhookCertErr  error
)

// ephemeralSelfSignedTLS returns a TLS option that serves a process-local
// self-signed certificate. Used when webhookServer is enabled without
// tls.secretRef or on-disk certs so controller-runtime can Start.
func ephemeralSelfSignedTLS() func(*tls.Config) {
	return func(c *tls.Config) {
		c.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			ephemeralWebhookCertOnce.Do(func() {
				ephemeralWebhookCert, ephemeralWebhookCertErr = generateEphemeralWebhookCert()
			})
			if ephemeralWebhookCertErr != nil {
				return nil, ephemeralWebhookCertErr
			}
			return &ephemeralWebhookCert, nil
		}
	}
}

func generateEphemeralWebhookCert() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate webhook TLS key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate webhook TLS serial: %w", err)
	}

	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			Organization: []string{"amberflo-provider"},
			CommonName:   "amberflo-provider-webhook",
		},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(10 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost", "amberflo-provider-webhook"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create webhook TLS certificate: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("marshal webhook TLS key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("parse webhook TLS key pair: %w", err)
	}
	return cert, nil
}
