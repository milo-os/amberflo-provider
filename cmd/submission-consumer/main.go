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

	natsgo "github.com/nats-io/nats.go"

	billingv1alpha1 "go.miloapis.com/billing/api/v1alpha1"

	"go.miloapis.com/amberflo-provider/internal/amberflo"
	"go.miloapis.com/amberflo-provider/internal/config"
	"go.miloapis.com/amberflo-provider/internal/submission"
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
}

func main() {
	var probeAddr string
	var serverConfigFile string

	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.StringVar(&serverConfigFile, "server-config", "", "Path to the server config file (YAML).")

	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	setupLog.Info("starting submission-consumer",
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

	cfg, err := serverConfig.RestConfig()
	if err != nil {
		setupLog.Error(err, "unable to load rest config")
		os.Exit(1)
	}

	ctx := ctrl.SetupSignalHandler()

	bootstrapClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		setupLog.Error(err, "unable to create bootstrap client")
		os.Exit(1)
	}

	metricsServerOptions := serverConfig.MetricsServer.Options(ctx, bootstrapClient)

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         false, // Leader election disabled for consumer instances
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

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
		UserAgent:       fmt.Sprintf("submission-consumer/%s", version),
	})
	if err != nil {
		setupLog.Error(err, "unable to construct Amberflo client")
		os.Exit(1)
	}
	amberfloClient = amberflo.NewInstrumentedClient(amberfloClient)

	if serverConfig.Nats.URL == "" {
		setupLog.Error(fmt.Errorf("NATS URL must be configured"), "")
		os.Exit(1)
	}

	nc, natsErr := connectNATS(&serverConfig.Nats)
	if natsErr != nil {
		setupLog.Error(natsErr, "unable to connect to NATS",
			"url", serverConfig.Nats.URL)
		os.Exit(1)
	}
	defer nc.Drain() //nolint:errcheck

	baCache, cacheErr := submission.NewBillingAccountCache(ctx, mgr.GetCache())
	if cacheErr != nil {
		setupLog.Error(cacheErr, "unable to create BillingAccountCache")
		os.Exit(1)
	}

	meterCache, cacheErr := submission.NewMeterDefinitionCache(ctx, mgr.GetCache())
	if cacheErr != nil {
		setupLog.Error(cacheErr, "unable to create MeterDefinitionCache")
		os.Exit(1)
	}

	submissionConsumer := &submission.SubmissionConsumer{
		Cache:               mgr.GetCache(),
		NC:                  nc,
		IngestClient:        amberfloClient,
		BillingAccountCache: baCache,
		MeterCache:          meterCache,
		Logger:              ctrl.Log.WithName("submission-consumer"),
		FetchBatch:          serverConfig.SubmissionBatchSize,
		RetryAfter:          serverConfig.SubmissionRetryAfter.Duration,
		AckWait:             serverConfig.SubmissionAckWait.Duration,
		FetchTimeout:        serverConfig.SubmissionFetchTimeout.Duration,
	}
	if addErr := mgr.Add(submissionConsumer); addErr != nil {
		setupLog.Error(addErr, "unable to add submission consumer to manager")
		os.Exit(1)
	}
	setupLog.Info("submission consumer registered", "natsURL", serverConfig.Nats.URL)

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

func connectNATS(cfg *config.NATSConfig) (*natsgo.Conn, error) {
	opts := []natsgo.Option{natsgo.Name("submission-consumer")}
	if cfg.CAFile != "" || cfg.CertFile != "" || cfg.KeyFile != "" {
		opts = append(opts, natsgo.RootCAs(cfg.CAFile))
		opts = append(opts, natsgo.ClientCert(cfg.CertFile, cfg.KeyFile))
	}
	return natsgo.Connect(cfg.URL, opts...)
}

func loadAPIKey(path, envValue string) (string, string, error) {
	envKey := strings.TrimSpace(envValue)

	if path != "" {
		raw, err := os.ReadFile(path)
		switch {
		case err == nil:
			key := strings.TrimSpace(string(raw))
			if key == "" {
				return "", "", fmt.Errorf("%q contains no API key after trimming whitespace", path)
			}
			return key, "file:" + path, nil
		case os.IsNotExist(err):
			// Fall through to env var
		default:
			return "", "", fmt.Errorf("read %q: %w", path, err)
		}
	}

	if envKey != "" {
		return envKey, "env:AMBERFLO_API_KEY", nil
	}
	return "", "", fmt.Errorf(
		"no Amberflo API key available: set server-config.amberfloAPIKeyPath to a " +
			"readable file or export AMBERFLO_API_KEY",
	)
}

func keyFingerprint(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])[:8]
}
