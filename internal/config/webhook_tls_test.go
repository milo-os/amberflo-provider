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
	"os"
	"path/filepath"
	"testing"
)

func TestWebhookServerOptions_EphemeralWhenNoCerts(t *testing.T) {
	t.Parallel()
	cfg := &WebhookServerConfig{Port: 9443}
	SetDefaults_WebhookServerConfig(cfg)
	SetDefaults_TLSConfig(&cfg.TLS)

	opts := cfg.Options(context.Background(), nil)
	if opts.CertDir != "" {
		t.Fatalf("CertDir should be empty when using ephemeral TLS, got %q", opts.CertDir)
	}
	if len(opts.TLSOpts) == 0 {
		t.Fatal("expected TLSOpts for ephemeral self-signed cert")
	}

	tlsCfg := &tls.Config{}
	for _, o := range opts.TLSOpts {
		o(tlsCfg)
	}
	if tlsCfg.GetCertificate == nil {
		t.Fatal("expected GetCertificate")
	}
	cert, err := tlsCfg.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatalf("GetCertificate: %v", err)
	}
	if cert == nil || len(cert.Certificate) == 0 {
		t.Fatal("expected non-empty certificate")
	}
}

func TestWebhookServerOptions_UsesCertDirWhenFilesExist(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "tls.crt"), []byte("not-a-real-cert"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tls.key"), []byte("not-a-real-key"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := &WebhookServerConfig{
		Port: 9443,
		TLS: TLSConfig{
			CertDir:  dir,
			CertName: "tls.crt",
			KeyName:  "tls.key",
		},
	}
	opts := cfg.Options(context.Background(), nil)
	if opts.CertDir != dir {
		t.Fatalf("CertDir = %q, want %q", opts.CertDir, dir)
	}
	if len(opts.TLSOpts) != 0 {
		t.Fatalf("expected no TLSOpts when CertDir files exist, got %d", len(opts.TLSOpts))
	}
}
