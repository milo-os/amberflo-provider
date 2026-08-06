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
	"fmt"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	billingv1alpha1 "go.miloapis.com/billing/api/v1alpha1"
)

func TestRemoveFinalizer_FallsBackToUpdateWhenApplyForbidden(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := billingv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("billing scheme: %v", err)
	}

	account := &billingv1alpha1.BillingAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "ba-terminating",
			Namespace:  "organization-org-deadbeef",
			Finalizers: []string{CustomerLinkFinalizer},
		},
		Spec: billingv1alpha1.BillingAccountSpec{CurrencyCode: "USD"},
	}

	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(account.DeepCopy()).Build()
	applyCalls := 0
	c := interceptor.NewClient(base, interceptor.Funcs{
		Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
			applyCalls++
			return apierrors.NewForbidden(
				schema.GroupResource{Group: "billing.miloapis.com", Resource: "billingaccounts"},
				account.Name,
				fmt.Errorf("unable to create new content in namespace %q because it is being terminated", account.Namespace),
			)
		},
	})

	var live billingv1alpha1.BillingAccount
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(account), &live); err != nil {
		t.Fatalf("get live account: %v", err)
	}

	r := &BillingAccountReconciler{Client: c}
	if err := r.removeFinalizer(context.Background(), &live); err != nil {
		t.Fatalf("removeFinalizer: %v", err)
	}
	if applyCalls != 1 {
		t.Fatalf("expected one Apply attempt, got %d", applyCalls)
	}
	if controllerutil.ContainsFinalizer(&live, CustomerLinkFinalizer) {
		t.Fatal("expected finalizer cleared on local object")
	}

	var stored billingv1alpha1.BillingAccount
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(account), &stored); err != nil {
		t.Fatalf("get stored account: %v", err)
	}
	if controllerutil.ContainsFinalizer(&stored, CustomerLinkFinalizer) {
		t.Fatal("expected finalizer cleared on stored object via Update fallback")
	}
}

func TestRemoveFinalizer_PropagatesNonForbiddenApplyErrors(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := billingv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("billing scheme: %v", err)
	}

	account := &billingv1alpha1.BillingAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "ba-conflict",
			Namespace:  "default",
			Finalizers: []string{CustomerLinkFinalizer},
		},
		Spec: billingv1alpha1.BillingAccountSpec{CurrencyCode: "USD"},
	}

	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(account.DeepCopy()).Build()
	c := interceptor.NewClient(base, interceptor.Funcs{
		Apply: func(ctx context.Context, c client.WithWatch, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
			return apierrors.NewConflict(
				schema.GroupResource{Group: "billing.miloapis.com", Resource: "billingaccounts"},
				account.Name,
				fmt.Errorf("conflict"),
			)
		},
	})

	r := &BillingAccountReconciler{Client: c}
	err := r.removeFinalizer(context.Background(), account)
	if err == nil {
		t.Fatal("expected error")
	}
	if !apierrors.IsConflict(err) {
		t.Fatalf("expected conflict error, got %v", err)
	}
	if !controllerutil.ContainsFinalizer(account, CustomerLinkFinalizer) {
		t.Fatal("finalizer should remain when Apply fails non-Forbidden")
	}
}
