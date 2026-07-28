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

package invoice

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	billingv1alpha1 "go.miloapis.com/billing/api/v1alpha1"

	"go.miloapis.com/amberflo-provider/internal/amberflo"
)

const (
	// InvoiceKeyAnnotation stores the Amberflo composite invoice key on a
	// Milo Invoice for reconciliation and support lookup.
	InvoiceKeyAnnotation = "amberflo.billing.miloapis.com/invoiceKey"
)

// Syncer upserts Milo Invoice resources from Amberflo customer-product
// invoices. Shared by the BillingAccount reconcile fallback and the
// Amberflo invoice webhook receiver.
type Syncer struct {
	Client client.Client

	// Now overrides time.Now for deterministic tests. Optional.
	Now func() time.Time

	// Log is optional; when unset MapPhase warnings are skipped.
	Log logr.Logger
}

// Upsert creates or updates a Milo Invoice for the given Amberflo invoice
// and BillingAccount. Spec is set only on create (immutable thereafter);
// status and the invoiceKey annotation are always reconciled.
func (s *Syncer) Upsert(
	ctx context.Context,
	account *billingv1alpha1.BillingAccount,
	inv amberflo.CustomerProductInvoice,
) error {
	if account == nil {
		return fmt.Errorf("invoice sync: BillingAccount is required")
	}
	if inv.InvoiceStartTimeInSeconds <= 0 {
		return fmt.Errorf("invoice sync: invoiceStartTimeInSeconds is required")
	}

	now := time.Now
	if s.Now != nil {
		now = s.Now
	}
	t := now()

	name := InvoiceName(account.Name, inv.InvoiceStartTimeInSeconds)
	nn := types.NamespacedName{Name: name, Namespace: account.Namespace}

	phase := MapPhase(inv, t, s.Log)
	total, paid, due := Amounts(inv.TotalBill.TotalPrice, phase)
	currency := account.Spec.CurrencyCode

	key := inv.InvoiceKey
	if key.CustomerID == "" {
		key.CustomerID = string(account.UID)
	}
	keyAnnotation := amberflo.FormatInvoiceKey(key)

	var existing billingv1alpha1.Invoice
	err := s.Client.Get(ctx, nn, &existing)
	switch {
	case apierrors.IsNotFound(err):
		return s.create(ctx, account, inv, name, phase, total, paid, due, currency, keyAnnotation, t)
	case err != nil:
		return fmt.Errorf("get Invoice %s: %w", nn, err)
	}

	return s.patchStatus(ctx, &existing, account, inv, phase, total, paid, due, currency, keyAnnotation, t)
}

func (s *Syncer) create(
	ctx context.Context,
	account *billingv1alpha1.BillingAccount,
	inv amberflo.CustomerProductInvoice,
	name string,
	phase billingv1alpha1.InvoicePhase,
	total, paid, due, currency, keyAnnotation string,
	now time.Time,
) error {
	start := metav1.NewTime(time.Unix(inv.InvoiceStartTimeInSeconds, 0).UTC())
	end := metav1.NewTime(time.Unix(inv.InvoiceEndTimeInSeconds, 0).UTC())

	obj := &billingv1alpha1.Invoice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: account.Namespace,
			Annotations: map[string]string{
				InvoiceKeyAnnotation: keyAnnotation,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         billingv1alpha1.GroupVersion.String(),
				Kind:               "BillingAccount",
				Name:               account.Name,
				UID:                account.UID,
				Controller:         ptr.To(false),
				// false so BillingAccount deletion is not blocked while
				// invoices still exist; GC still cascades when the BA is
				// deleted. Matches the billing Invoice ownership contract
				// (non-controller ownerRef).
				BlockOwnerDeletion: ptr.To(false),
			}},
		},
		Spec: billingv1alpha1.InvoiceSpec{
			BillingAccountRef: billingv1alpha1.BillingAccountRef{Name: account.Name},
			Period: billingv1alpha1.InvoicePeriod{
				Start: start,
				End:   end,
			},
		},
	}

	if err := s.Client.Create(ctx, obj); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// Race with webhook / concurrent reconcile: re-fetch and patch.
			var existing billingv1alpha1.Invoice
			if getErr := s.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: account.Namespace}, &existing); getErr != nil {
				return fmt.Errorf("create race re-get Invoice %s/%s: %w", account.Namespace, name, getErr)
			}
			return s.patchStatus(ctx, &existing, account, inv, phase, total, paid, due, currency, keyAnnotation, now)
		}
		return fmt.Errorf("create Invoice %s/%s: %w", account.Namespace, name, err)
	}

	return s.patchStatus(ctx, obj, account, inv, phase, total, paid, due, currency, keyAnnotation, now)
}

func (s *Syncer) patchStatus(
	ctx context.Context,
	obj *billingv1alpha1.Invoice,
	account *billingv1alpha1.BillingAccount,
	inv amberflo.CustomerProductInvoice,
	phase billingv1alpha1.InvoicePhase,
	total, paid, due, currency, keyAnnotation string,
	now time.Time,
) error {
	// Ensure annotation + non-controller ownerRef stay present even if
	// something stripped them. Spec is immutable; we do not touch it.
	base := obj.DeepCopy()
	if obj.Annotations == nil {
		obj.Annotations = map[string]string{}
	}
	obj.Annotations[InvoiceKeyAnnotation] = keyAnnotation
	if err := controllerutil.SetOwnerReference(account, obj, s.Client.Scheme()); err != nil {
		return fmt.Errorf("set owner reference: %w", err)
	}
	// Force Controller=false — SetOwnerReference may set it true when
	// called with the wrong helper; we want non-controller ownership.
	for i := range obj.OwnerReferences {
		if obj.OwnerReferences[i].UID == account.UID {
			obj.OwnerReferences[i].Controller = ptr.To(false)
			obj.OwnerReferences[i].BlockOwnerDeletion = ptr.To(false)
		}
	}
	if err := s.Client.Patch(ctx, obj, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("patch Invoice metadata %s/%s: %w", obj.Namespace, obj.Name, err)
	}

	statusBase := obj.DeepCopy()
	obj.Status.Phase = phase
	obj.Status.CurrencyCode = currency
	obj.Status.Total = total
	obj.Status.AmountPaid = paid
	obj.Status.AmountDue = due
	obj.Status.DocumentURI = inv.InvoiceURI
	obj.Status.ObservedGeneration = obj.Generation

	if dueAt := DueTime(inv); dueAt != nil {
		obj.Status.DueDate = &metav1.Time{Time: *dueAt}
	}
	if phase == billingv1alpha1.InvoicePhasePaid {
		paidAt := now.UTC()
		if inv.PaymentCreatedInSeconds > 0 {
			paidAt = time.Unix(inv.PaymentCreatedInSeconds, 0).UTC()
		}
		obj.Status.PaidAt = &metav1.Time{Time: paidAt}
	} else {
		obj.Status.PaidAt = nil
	}

	ready := metav1.Condition{
		Type:               billingv1alpha1.InvoiceConditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             string(phase),
		Message:            fmt.Sprintf("Invoice projected from Amberflo with phase %s", phase),
		ObservedGeneration: obj.Generation,
	}
	if currency != "" && account.Spec.CurrencyCode != "" && currency != account.Spec.CurrencyCode {
		ready.Status = metav1.ConditionFalse
		ready.Reason = "CurrencyMismatch"
		ready.Message = fmt.Sprintf(
			"invoice currency %q does not match BillingAccount currency %q",
			currency, account.Spec.CurrencyCode,
		)
	}
	apimeta.SetStatusCondition(&obj.Status.Conditions, ready)

	if err := s.Client.Status().Patch(ctx, obj, client.MergeFrom(statusBase)); err != nil {
		return fmt.Errorf("patch Invoice status %s/%s: %w", obj.Namespace, obj.Name, err)
	}
	return nil
}
