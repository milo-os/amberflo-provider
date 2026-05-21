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

package submission

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	billingv1alpha1 "go.miloapis.com/billing/api/v1alpha1"
)

// makeBillingAccount is a test helper.
func makeBillingAccount(name string, uid types.UID) *billingv1alpha1.BillingAccount {
	ba := &billingv1alpha1.BillingAccount{}
	ba.Name = name
	ba.UID = uid
	ba.ObjectMeta = metav1.ObjectMeta{Name: name, UID: uid}
	return ba
}

// TestBillingAccountCache_UpsertAndGet verifies that an added account is
// retrievable by name.
func TestBillingAccountCache_UpsertAndGet(t *testing.T) {
	bc := &BillingAccountCache{uidByName: make(map[string]types.UID)}
	ba := makeBillingAccount("acct-abc", "uid-abc")

	bc.upsert(ba)

	uid, ok := bc.GetUID("acct-abc")
	if !ok {
		t.Fatal("expected account to be found after upsert")
	}
	if uid != "uid-abc" {
		t.Errorf("UID: got %q want %q", uid, "uid-abc")
	}
}

// TestBillingAccountCache_MissReturnsFalse verifies that a lookup for an
// unknown account returns false with an empty UID.
func TestBillingAccountCache_MissReturnsFalse(t *testing.T) {
	bc := &BillingAccountCache{uidByName: make(map[string]types.UID)}

	uid, ok := bc.GetUID("unknown-acct")
	if ok {
		t.Error("expected miss to return false")
	}
	if uid != "" {
		t.Errorf("expected empty UID on miss, got %q", uid)
	}
}

// TestBillingAccountCache_DeleteRemovesEntry verifies that a deleted account
// is no longer retrievable.
func TestBillingAccountCache_DeleteRemovesEntry(t *testing.T) {
	bc := &BillingAccountCache{uidByName: make(map[string]types.UID)}
	ba := makeBillingAccount("acct-del", "uid-del")

	bc.upsert(ba)
	bc.delete(ba)

	_, ok := bc.GetUID("acct-del")
	if ok {
		t.Error("expected account to be absent after delete")
	}
}

// TestBillingAccountCache_UpsertUpdatesUID verifies that a second upsert with
// a different UID (e.g. after a recreate) overwrites the old entry.
func TestBillingAccountCache_UpsertUpdatesUID(t *testing.T) {
	bc := &BillingAccountCache{uidByName: make(map[string]types.UID)}

	bc.upsert(makeBillingAccount("acct-x", "uid-old"))
	bc.upsert(makeBillingAccount("acct-x", "uid-new"))

	uid, ok := bc.GetUID("acct-x")
	if !ok {
		t.Fatal("expected account to be found after second upsert")
	}
	if uid != "uid-new" {
		t.Errorf("expected updated UID %q, got %q", "uid-new", uid)
	}
}
