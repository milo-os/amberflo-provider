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
	"net/http"
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	billingv1alpha1 "go.miloapis.com/billing/api/v1alpha1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// meterKey builds the (cluster-scoped) ObjectKey for a MeterDefinition.
func meterKey(name string) client.ObjectKey { return client.ObjectKey{Name: name} }

// newMeterDefinition returns a valid, usage-categorized, sum-aggregated
// MeterDefinition with a reverse-DNS meterName.
// The caller supplies only the short name suffix so specs do not collide
// across the shared envtest.
func newMeterDefinition(nameSuffix string) *billingv1alpha1.MeterDefinition {
	return &billingv1alpha1.MeterDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: nameSuffix,
		},
		Spec: billingv1alpha1.MeterDefinitionSpec{
			MeterName:   "compute.miloapis.com/" + nameSuffix,
			Phase:       billingv1alpha1.PhasePublished,
			DisplayName: "Display " + nameSuffix,
			Measurement: billingv1alpha1.MeterMeasurement{
				Aggregation: billingv1alpha1.MeterAggregationSum,
				Unit:        "s",
				Dimensions:  []string{"region", "tier"},
			},
			Billing: billingv1alpha1.MeterBilling{
				ConsumedUnit: "s",
				PricingUnit:  "h",
			},
			MonitoredResourceTypes: []string{"compute.miloapis.com/Instance"},
		},
	}
}

// waitForMeterFinalizer polls until the given MeterDefinition carries
// MeterFinalizer. Returns the fetched object.
func waitForMeterFinalizer(key client.ObjectKey) *billingv1alpha1.MeterDefinition {
	GinkgoHelper()
	var md billingv1alpha1.MeterDefinition
	Eventually(func(g Gomega) {
		g.Expect(k8sClient.Get(ctx, key, &md)).To(Succeed())
		g.Expect(slices.Contains(md.Finalizers, MeterFinalizer)).To(BeTrue(),
			"finalizer %q missing on %s", MeterFinalizer, key.String())
	}, envtestTimeout, envtestInterval).Should(Succeed())
	return &md
}

// waitForMeterGone polls until the MeterDefinition is gone from the
// apiserver — which only happens after the finalizer has been released.
func waitForMeterGone(key client.ObjectKey) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		var md billingv1alpha1.MeterDefinition
		err := k8sClient.Get(ctx, key, &md)
		g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
			"meter %s still present (finalizers=%v err=%v)",
			key.String(), md.Finalizers, err)
	}, envtestTimeout, envtestInterval).Should(Succeed())
}

// waitForMeterInFake polls until the fake has a stored meter under the
// given MeterDefinition UID.
func waitForMeterInFake(key client.ObjectKey, predicate func(storedMeter) bool) storedMeter {
	GinkgoHelper()
	var last storedMeter
	Eventually(func(g Gomega) {
		var md billingv1alpha1.MeterDefinition
		g.Expect(k8sClient.Get(ctx, key, &md)).To(Succeed())
		m, ok := fakeHTTP.FetchMeter(string(md.UID))
		g.Expect(ok).To(BeTrue(), "no meter yet for UID=%s", md.UID)
		if predicate != nil {
			g.Expect(predicate(m)).To(BeTrue(), "predicate not satisfied; m=%+v", m)
		}
		last = m
	}, envtestTimeout, envtestInterval).Should(Succeed())
	return last
}

var _ = Describe("MeterDefinitionReconciler", func() {
	BeforeEach(func() {
		fakeHTTP.Reset()
		drainEvents()
	})

	Context("CreateMeter_CreatesMeterInAmberflo", func() {
		It("adds finalizer, POSTs the meter with sum_of_all_usage meterType, and emits Synced", func() {
			md := newMeterDefinition("cpu-seconds-create")
			mustCreate(ctx, md)
			DeferCleanup(func() {
				mustDelete(ctx, md)
				waitForMeterGone(meterKey(md.Name))
			})

			got := waitForMeterFinalizer(meterKey(md.Name))
			Expect(got.UID).NotTo(BeEmpty())

			waitForEventReason(EventReasonSynced)

			m := waitForMeterInFake(meterKey(md.Name), func(m storedMeter) bool {
				return m.MeterType == "sum_of_all_usage" && m.UseInBilling
			})
			Expect(m.Label).To(Equal("Display cpu-seconds-create"))
			Expect(m.Unit).To(Equal("s"))
			Expect(m.Dimensions).To(Equal([]string{"region", "tier"}))
			// sum_of_all_usage does not use aggregationDimensions.
			Expect(m.AggregationDimensions).To(BeEmpty())

			// POST (not PUT) on initial create.
			sawPost := false
			for _, req := range fakeHTTP.Requests() {
				if req.Method == http.MethodPost && req.Path == "/meters" {
					sawPost = true
				}
			}
			Expect(sawPost).To(BeTrue(), "expected a POST /meters on create")
		})
	})

	Context("CreateMeter_UniqueCount_MapsToActiveUsers", func() {
		It("maps UniqueCount with dimensions onto active_users with aggregationDimensions", func() {
			md := newMeterDefinition("active-projects")
			md.Spec.Measurement.Aggregation = billingv1alpha1.MeterAggregationUniqueCount
			md.Spec.Measurement.Dimensions = []string{"project_id"}
			mustCreate(ctx, md)
			DeferCleanup(func() {
				mustDelete(ctx, md)
				waitForMeterGone(meterKey(md.Name))
			})

			waitForEventReason(EventReasonSynced)

			m := waitForMeterInFake(meterKey(md.Name), func(m storedMeter) bool {
				return m.MeterType == "active_users"
			})
			Expect(m.Dimensions).To(Equal([]string{"project_id"}))
			Expect(m.AggregationDimensions).To(Equal([]string{"project_id"}))
		})
	})

	Context("UniqueCount_NoDimensions_SkipsSync", func() {
		It("emits SyncSkipped MissingDimensions and writes nothing to Amberflo", func() {
			md := newMeterDefinition("active-no-dims")
			md.Spec.Measurement.Aggregation = billingv1alpha1.MeterAggregationUniqueCount
			md.Spec.Measurement.Dimensions = nil
			mustCreate(ctx, md)
			DeferCleanup(func() {
				mustDelete(ctx, md)
				waitForMeterGone(meterKey(md.Name))
			})

			ev := waitForEventReason(EventReasonSyncSkipped)
			Expect(ev).To(ContainSubstring(skipReasonMissingDimensions))

			time.Sleep(500 * time.Millisecond)
			for _, req := range fakeHTTP.Requests() {
				Expect(req.Path).NotTo(HavePrefix("/meters"),
					"unexpected /meters request: %s %s", req.Method, req.Path)
			}
		})
	})

	Context("UpdateMeter_PutsOnDisplayNameChange", func() {
		It("re-syncs the meter when spec.displayName changes", func() {
			md := newMeterDefinition("cpu-seconds-update")
			mustCreate(ctx, md)
			DeferCleanup(func() {
				mustDelete(ctx, md)
				waitForMeterGone(meterKey(md.Name))
			})

			waitForMeterInFake(meterKey(md.Name), func(m storedMeter) bool {
				return m.Label == "Display cpu-seconds-update"
			})

			Eventually(func(g Gomega) {
				var fresh billingv1alpha1.MeterDefinition
				g.Expect(k8sClient.Get(ctx, meterKey(md.Name), &fresh)).To(Succeed())
				fresh.Spec.DisplayName = "New Display"
				g.Expect(k8sClient.Update(ctx, &fresh)).To(Succeed())
			}, envtestTimeout, envtestInterval).Should(Succeed())

			waitForMeterInFake(meterKey(md.Name), func(m storedMeter) bool {
				return m.Label == "New Display"
			})

			// A PUT should have been observed after the initial POST.
			sawPut := false
			for _, req := range fakeHTTP.Requests() {
				if req.Method == http.MethodPut && req.Path == "/meters" {
					sawPut = true
				}
			}
			Expect(sawPut).To(BeTrue(), "expected a PUT /meters on display-name change")
		})
	})

	Context("DeleteMeter_RemovesFromAmberflo", func() {
		It("DELETEs the meter and releases the finalizer", func() {
			md := newMeterDefinition("cpu-seconds-delete")
			mustCreate(ctx, md)

			waitForMeterInFake(meterKey(md.Name), nil)

			Expect(k8sClient.Delete(ctx, md)).To(Succeed())
			waitForMeterGone(meterKey(md.Name))

			sawDelete := false
			for _, req := range fakeHTTP.Requests() {
				if req.Method == http.MethodDelete {
					sawDelete = true
				}
			}
			Expect(sawDelete).To(BeTrue(), "expected a DELETE /meters/{id}")
		})
	})

	Context("DeleteMeter_ToleratesNotFound", func() {
		It("releases the finalizer even when Amberflo 404s on delete", func() {
			md := newMeterDefinition("cpu-seconds-404")
			mustCreate(ctx, md)

			var fresh billingv1alpha1.MeterDefinition
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, meterKey(md.Name), &fresh)).To(Succeed())
				g.Expect(fresh.UID).NotTo(BeEmpty())
			}, envtestTimeout, envtestInterval).Should(Succeed())

			waitForMeterInFake(meterKey(md.Name), nil)

			// Simulate the meter being gone out-of-band before the
			// reconciler issues its DELETE.
			Expect(fakeHTTP.DeleteMeter(string(fresh.UID))).To(BeTrue())

			Expect(k8sClient.Delete(ctx, md)).To(Succeed())
			waitForMeterGone(meterKey(md.Name))
		})
	})

	Context("UnsupportedAggregation_SkipsSync", func() {
		It("emits SyncSkipped and issues no /meters writes for Max", func() {
			md := newMeterDefinition("max-skipped")
			md.Spec.Measurement.Aggregation = billingv1alpha1.MeterAggregationMax
			mustCreate(ctx, md)
			DeferCleanup(func() {
				mustDelete(ctx, md)
				waitForMeterGone(meterKey(md.Name))
			})

			ev := waitForEventReason(EventReasonSyncSkipped)
			Expect(ev).To(ContainSubstring(skipReasonUnsupportedAggregation))

			// Pause briefly to let any stray reconcile trip the fake.
			time.Sleep(500 * time.Millisecond)
			for _, req := range fakeHTTP.Requests() {
				Expect(req.Path).NotTo(HavePrefix("/meters"),
					"unexpected /meters request: %s %s", req.Method, req.Path)
			}
		})
	})

})
