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

	"sigs.k8s.io/controller-runtime/pkg/client"

	billingv1alpha1 "go.miloapis.com/billing/api/v1alpha1"
)

// BindingBillingAccountRefField is the field index for listing
// BillingAccountBindings by the billing account they reference. The
// reconciler uses it to re-fetch the project set for a given account so
// link.Status.Projects stays consistent as bindings come and go.
const BindingBillingAccountRefField = ".spec.billingAccountRef.name"

// BindingProjectRefField is the field index for looking up
// BillingAccountBindings by their referenced project. Mirrors billing's
// own indexers.go; currently unused by this provider's reconciler but
// exposed for parity with upstream and for future supersession tooling.
const BindingProjectRefField = ".spec.projectRef.name"

// BillingAccountUIDField is the field index for looking up a
// BillingAccount by metadata.uid. The Amberflo invoice webhook resolves
// customerId (== BillingAccount UID) through this index.
const BillingAccountUIDField = ".metadata.uid"

// AddIndexers installs the field indexers used by the amberflo-provider
// reconcilers. It accepts a FieldIndexer (rather than a Manager) so envtest
// setup can share the same wiring.
func AddIndexers(ctx context.Context, fi client.FieldIndexer) error {
	if err := fi.IndexField(
		ctx,
		&billingv1alpha1.BillingAccountBinding{},
		BindingBillingAccountRefField,
		func(obj client.Object) []string {
			binding, ok := obj.(*billingv1alpha1.BillingAccountBinding)
			if !ok {
				return nil
			}
			return []string{binding.Spec.BillingAccountRef.Name}
		},
	); err != nil {
		return err
	}
	if err := fi.IndexField(
		ctx,
		&billingv1alpha1.BillingAccountBinding{},
		BindingProjectRefField,
		func(obj client.Object) []string {
			binding, ok := obj.(*billingv1alpha1.BillingAccountBinding)
			if !ok {
				return nil
			}
			return []string{binding.Spec.ProjectRef.Name}
		},
	); err != nil {
		return err
	}
	return fi.IndexField(
		ctx,
		&billingv1alpha1.BillingAccount{},
		BillingAccountUIDField,
		func(obj client.Object) []string {
			account, ok := obj.(*billingv1alpha1.BillingAccount)
			if !ok {
				return nil
			}
			if account.UID == "" {
				return nil
			}
			return []string{string(account.UID)}
		},
	)
}
