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

package controller

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	billingv1alpha1 "go.miloapis.com/billing/api/v1alpha1"

	"go.miloapis.com/amberflo-provider/internal/amberflo"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// billingCRDPath returns the directory containing Milo billing CRD bases.
// It resolves the path of the (possibly replaced) go.miloapis.com/billing
// module via `go list`, so it works both when the module is fetched from
// the proxy and when it is replaced by a local checkout.
func billingCRDPath() string {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "go.miloapis.com/billing").Output()
	Expect(err).NotTo(HaveOccurred(), "failed to resolve go.miloapis.com/billing module path")
	dir := strings.TrimSpace(string(out))
	Expect(dir).NotTo(BeEmpty())
	return filepath.Join(dir, "config", "base", "crd", "bases")
}

// Globals shared across tests. envtest bootstraps exactly one apiserver
// for the whole suite; individual specs reset the fake HTTP server's
// state and create uniquely-named resources to avoid cross-test leakage.
var (
	cfg       *rest.Config
	k8sClient client.Client
	testEnv   *envtest.Environment
	ctx       context.Context
	cancel    context.CancelFunc

	// fakeHTTP is the in-process httptest-backed Amberflo stand-in. The
	// controller's Amberflo client is wired to this server's URL.
	fakeHTTP        *recordingFakeServer
	amberfloClient  amberflo.Client
	reconciler      *BillingAccountReconciler
	meterReconciler *MeterDefinitionReconciler

	// fakeRecorder captures k8s events emitted by the reconciler so tests
	// can assert Synced/SyncFailed/etc. by reason without reaching into
	// the apiserver's Events collection.
	fakeRecorder *record.FakeRecorder
)

func TestControllers(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "amberflo-provider controller suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	ctx, cancel = context.WithCancel(context.TODO())

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths: []string{
			// Milo's BillingAccount + BillingAccountBinding + MeterDefinition CRDs.
			// We resolve the billing module path dynamically so the same
			// suite works against both a sibling checkout (via go.mod
			// replace) and a module-cache copy fetched from the proxy.
			billingCRDPath(),
		},
		ErrorIfCRDPathMissing: true,
	}

	var err error
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	Expect(billingv1alpha1.AddToScheme(scheme.Scheme)).To(Succeed())

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	// Bring up the httptest fake. Each test resets it in a BeforeEach;
	// server lifecycle is suite-scoped so a single port is reused.
	fakeHTTP = newRecordingFakeServer()
	DeferCleanup(fakeHTTP.Close)

	amberfloClient, err = amberflo.NewClient(amberflo.ClientOptions{
		BaseURL: fakeHTTP.URL(),
		APIKey:  fakeHTTP.APIKey(),
		// Disable rate-limiting and truncate retries so tests run fast.
		RateLimitPerSec: 1000,
		RetryAttempts:   2,
	})
	Expect(err).NotTo(HaveOccurred())

	// Build a manager with the controller registered.
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme.Scheme,
	})
	Expect(err).NotTo(HaveOccurred())

	Expect(AddIndexers(ctx, mgr.GetFieldIndexer())).To(Succeed())

	// Capacity chosen generously so a slow test doesn't drop events.
	fakeRecorder = record.NewFakeRecorder(256)

	reconciler = &BillingAccountReconciler{
		Client:         mgr.GetClient(),
		AmberfloClient: amberfloClient,
		Recorder:       fakeRecorder,
	}
	Expect(reconciler.SetupWithManager(mgr)).To(Succeed())

	meterReconciler = &MeterDefinitionReconciler{
		Client:         mgr.GetClient(),
		AmberfloClient: amberfloClient,
		Recorder:       fakeRecorder,
	}
	Expect(meterReconciler.SetupWithManager(mgr)).To(Succeed())

	go func() {
		defer GinkgoRecover()
		Expect(mgr.Start(ctx)).To(Succeed())
	}()

	Eventually(func() bool {
		return mgr.GetCache().WaitForCacheSync(ctx)
	}, 10*time.Second, 100*time.Millisecond).Should(BeTrue())
})

var _ = AfterSuite(func() {
	cancel()
	By("tearing down the test environment")
	Expect(testEnv.Stop()).To(Succeed())
})
