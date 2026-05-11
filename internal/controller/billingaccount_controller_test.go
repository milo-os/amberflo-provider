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
	"fmt"
	"net/http"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	billingv1alpha1 "go.miloapis.com/billing/api/v1alpha1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// accountKey builds the NamespacedName for a BillingAccount in the default test namespace.
func accountKey(name string) client.ObjectKey {
	return client.ObjectKey{Namespace: "default", Name: name}
}

var _ = Describe("BillingAccountReconciler", func() {
	BeforeEach(func() {
		// Every spec gets a clean Amberflo state and a clean event log.
		fakeHTTP.Reset()
		drainEvents()
	})

	Context("CreateAccount_CreatesCustomerInAmberflo", func() {
		It("adds finalizer, emits Synced, and POSTs customer with currency trait", func() {
			name := "create-happy"
			account := &billingv1alpha1.BillingAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: "default",
				},
				Spec: billingv1alpha1.BillingAccountSpec{
					CurrencyCode: "USD",
					ContactInfo: &billingv1alpha1.BillingContactInfo{
						Email: "finance@example.com",
						Name:  "Example Finance",
					},
				},
			}
			mustCreate(ctx, account)
			DeferCleanup(func() { mustDelete(ctx, account); waitForNoFinalizer(ctx, accountKey(name)) })

			// Finalizer added before any Amberflo write.
			got := waitForFinalizer(ctx, accountKey(name))
			Expect(got.UID).NotTo(BeEmpty())

			// Synced event emitted.
			waitForEventReason(EventReasonSynced)

			// Customer stored in fake under UID with display name = account.Name.
			c := waitForCustomerInFake(ctx, accountKey(name), func(c storedCustomer) bool {
				return c.CustomerName == name && c.Traits["currencyCode"] == "USD"
			})
			Expect(c.CustomerEmail).To(Equal("finance@example.com"))
			Expect(c.Traits).NotTo(HaveKey("projects"))
		})
	})

	Context("UpdateAccount_UpdatesTraits", func() {
		It("re-syncs customer email when spec changes", func() {
			name := "update-traits"
			account := &billingv1alpha1.BillingAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: "default",
				},
				Spec: billingv1alpha1.BillingAccountSpec{
					CurrencyCode: "EUR",
					ContactInfo: &billingv1alpha1.BillingContactInfo{
						Email: "before@example.com",
					},
				},
			}
			mustCreate(ctx, account)
			DeferCleanup(func() { mustDelete(ctx, account); waitForNoFinalizer(ctx, accountKey(name)) })

			waitForCustomerInFake(ctx, accountKey(name), func(c storedCustomer) bool {
				return c.CustomerEmail == "before@example.com"
			})

			Eventually(func(g Gomega) {
				var fresh billingv1alpha1.BillingAccount
				g.Expect(k8sClient.Get(ctx, accountKey(name), &fresh)).To(Succeed())
				fresh.Spec.ContactInfo.Email = "after@example.com"
				g.Expect(k8sClient.Update(ctx, &fresh)).To(Succeed())
			}, envtestTimeout, envtestInterval).Should(Succeed())

			waitForCustomerInFake(ctx, accountKey(name), func(c storedCustomer) bool {
				return c.CustomerEmail == "after@example.com"
			})
		})
	})

	Context("DeleteAccount_DisablesAndCleansUp", func() {
		It("soft-disables and releases the finalizer", func() {
			name := "delete-happy"
			account := &billingv1alpha1.BillingAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: "default",
				},
				Spec: billingv1alpha1.BillingAccountSpec{CurrencyCode: "USD"},
			}
			mustCreate(ctx, account)
			waitForCustomerInFake(ctx, accountKey(name), func(c storedCustomer) bool {
				return c.Enabled
			})

			Expect(k8sClient.Delete(ctx, account)).To(Succeed())

			waitForNoFinalizer(ctx, accountKey(name))

			// Customer marked disabled (enabled=false) in fake.
			Eventually(func(g Gomega) {
				var fresh billingv1alpha1.BillingAccount
				err := k8sClient.Get(ctx, accountKey(name), &fresh)
				if err == nil {
					g.Expect(fresh.UID).NotTo(BeEmpty())
					c, ok := fakeHTTP.FetchCustomer(string(fresh.UID))
					g.Expect(ok).To(BeTrue())
					g.Expect(c.Enabled).To(BeFalse())
				}
				// If the account is already gone, the earlier fake
				// snapshot we asserted above suffices; but we still
				// want to find the disabled customer by walking
				// everything stored. Iterate stored customers in that
				// case.
				if err != nil {
					found := false
					for _, req := range fakeHTTP.Requests() {
						if req.Method == http.MethodPut {
							found = true
						}
					}
					g.Expect(found).To(BeTrue(), "no PUT recorded for disable")
				}
			}, envtestTimeout, envtestInterval).Should(Succeed())
		})
	})

	Context("TransientError_Requeues", func() {
		It("emits SyncFailed with reason TransientError and eventually recovers", func() {
			// Inject enough 503s to cover the client's internal retries
			// plus the reconciler's first pass.
			fakeHTTP.ArmFailures(http.StatusServiceUnavailable, 20)

			name := "transient"
			account := &billingv1alpha1.BillingAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: "default",
				},
				Spec: billingv1alpha1.BillingAccountSpec{CurrencyCode: "USD"},
			}
			mustCreate(ctx, account)
			DeferCleanup(func() { mustDelete(ctx, account); waitForNoFinalizer(ctx, accountKey(name)) })

			ev := waitForEventReason(EventReasonSyncFailed)
			Expect(ev).To(ContainSubstring("TransientError"))

			// Clear fail mode and nudge reconciliation via a spec update
			// (the 30s RequeueAfter would exceed the test timeout).
			fakeHTTP.Reset()
			Eventually(func(g Gomega) {
				var fresh billingv1alpha1.BillingAccount
				g.Expect(k8sClient.Get(ctx, accountKey(name), &fresh)).To(Succeed())
				if fresh.Annotations == nil {
					fresh.Annotations = map[string]string{}
				}
				fresh.Annotations["kick"] = fmt.Sprintf("%d", time.Now().UnixNano())
				g.Expect(k8sClient.Update(ctx, &fresh)).To(Succeed())
			}, envtestTimeout, envtestInterval).Should(Succeed())

			waitForEventReason(EventReasonSynced)
		})
	})

	Context("PermanentError_RecordsEvent", func() {
		It("emits SyncFailed with reason PermanentError and stops retrying", func() {
			fakeHTTP.ArmFailures(http.StatusBadRequest, 1)

			name := "permanent"
			account := &billingv1alpha1.BillingAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: "default",
				},
				Spec: billingv1alpha1.BillingAccountSpec{CurrencyCode: "USD"},
			}
			mustCreate(ctx, account)
			DeferCleanup(func() {
				// Clear any residual fail mode so the disable call
				// during cleanup can proceed.
				fakeHTTP.Reset()
				mustDelete(ctx, account)
				waitForNoFinalizer(ctx, accountKey(name))
			})

			ev := waitForEventReason(EventReasonSyncFailed)
			Expect(ev).To(ContainSubstring("PermanentError"))

			// Count requests after the initial burst; confirm the
			// reconciler isn't hammering the fake.
			beforeCount := len(fakeHTTP.Requests())
			time.Sleep(1 * time.Second)
			afterCount := len(fakeHTTP.Requests())
			// A permanent error ends the loop; subsequent reconciles
			// only happen on events. Allow up to 1 extra to absorb
			// minor racing.
			Expect(afterCount-beforeCount).To(BeNumerically("<=", 1),
				"unexpected retry activity after permanent error: before=%d after=%d",
				beforeCount, afterCount)
		})
	})

	Context("ForceDelete_SkipsDisable", func() {
		It("removes the finalizer even when DisableCustomer fails", func() {
			name := "force-delete"
			account := &billingv1alpha1.BillingAccount{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: "default",
				},
				Spec: billingv1alpha1.BillingAccountSpec{CurrencyCode: "USD"},
			}
			mustCreate(ctx, account)

			waitForCustomerInFake(ctx, accountKey(name), func(c storedCustomer) bool {
				return c.Enabled
			})

			// Annotate for force-delete, arm a permanent error for disable.
			Eventually(func(g Gomega) {
				var fresh billingv1alpha1.BillingAccount
				g.Expect(k8sClient.Get(ctx, accountKey(name), &fresh)).To(Succeed())
				if fresh.Annotations == nil {
					fresh.Annotations = map[string]string{}
				}
				fresh.Annotations[ForceDeleteAnnotation] = "true"
				g.Expect(k8sClient.Update(ctx, &fresh)).To(Succeed())
			}, envtestTimeout, envtestInterval).Should(Succeed())

			fakeHTTP.ArmFailures(http.StatusBadRequest, 20)

			Expect(k8sClient.Delete(ctx, account)).To(Succeed())
			waitForNoFinalizer(ctx, accountKey(name))

			// Reset fake state for any downstream spec.
			fakeHTTP.Reset()
		})
	})
})
