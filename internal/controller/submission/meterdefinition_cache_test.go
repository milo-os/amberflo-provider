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

	"k8s.io/apimachinery/pkg/types"

	billingv1alpha1 "go.miloapis.com/billing/api/v1alpha1"
)

// makeMeterDefinition is a test helper that constructs a MeterDefinition with
// the given name, UID, and phase.
func makeMeterDefinition(name string, uid types.UID, phase billingv1alpha1.Phase) *billingv1alpha1.MeterDefinition {
	md := &billingv1alpha1.MeterDefinition{}
	md.Name = name
	md.UID = uid
	md.Spec.MeterName = name
	md.Spec.Phase = phase
	return md
}

// TestMeterDefinitionCache_PublishedIsIndexed verifies that a MeterDefinition
// in the Published phase is reachable via GetUID.
func TestMeterDefinitionCache_PublishedIsIndexed(t *testing.T) {
	mc := &MeterDefinitionCache{uidByMeterName: make(map[string]types.UID)}
	md := makeMeterDefinition("compute.miloapis.com/cpu", "uid-published", billingv1alpha1.PhasePublished)

	mc.upsert(md)

	uid, ok := mc.GetUID("compute.miloapis.com/cpu")
	if !ok {
		t.Fatal("expected Published meter to be found in cache")
	}
	if uid != "uid-published" {
		t.Errorf("UID: got %q want %q", uid, "uid-published")
	}
}

// TestMeterDefinitionCache_DeprecatedIsIndexed verifies that a MeterDefinition
// in the Deprecated phase is also reachable (events for deprecated meters must
// still be submittable until the meter is removed entirely).
func TestMeterDefinitionCache_DeprecatedIsIndexed(t *testing.T) {
	mc := &MeterDefinitionCache{uidByMeterName: make(map[string]types.UID)}
	md := makeMeterDefinition("compute.miloapis.com/mem", "uid-deprecated", billingv1alpha1.PhaseDeprecated)

	mc.upsert(md)

	_, ok := mc.GetUID("compute.miloapis.com/mem")
	if !ok {
		t.Error("expected Deprecated meter to be found in cache")
	}
}

// TestMeterDefinitionCache_DraftIsNotIndexed verifies that a MeterDefinition
// that is not Published or Deprecated is excluded from the index so that
// events for not-yet-active meters are nacked rather than forwarded to Amberflo.
func TestMeterDefinitionCache_DraftIsNotIndexed(t *testing.T) {
	mc := &MeterDefinitionCache{uidByMeterName: make(map[string]types.UID)}
	md := makeMeterDefinition("compute.miloapis.com/gpu", "uid-draft", billingv1alpha1.PhaseDraft)

	mc.upsert(md)

	_, ok := mc.GetUID("compute.miloapis.com/gpu")
	if ok {
		t.Error("expected Draft meter NOT to be found in cache")
	}
}

// TestMeterDefinitionCache_DeleteRemovesEntry verifies that deleting a
// MeterDefinition removes it from the index.
func TestMeterDefinitionCache_DeleteRemovesEntry(t *testing.T) {
	mc := &MeterDefinitionCache{uidByMeterName: make(map[string]types.UID)}
	md := makeMeterDefinition("compute.miloapis.com/net", "uid-net", billingv1alpha1.PhasePublished)

	mc.upsert(md)
	if _, ok := mc.GetUID("compute.miloapis.com/net"); !ok {
		t.Fatal("expected meter to be present before delete")
	}

	mc.delete(md)
	if _, ok := mc.GetUID("compute.miloapis.com/net"); ok {
		t.Error("expected meter to be absent after delete")
	}
}

// TestMeterDefinitionCache_TransitionToNonIndexedPhaseRemovesEntry verifies
// that upserting a previously-Published meter whose phase changes to Draft
// removes it from the index (the upsert call handles the transition to
// non-indexed phases by calling delete internally).
func TestMeterDefinitionCache_TransitionToNonIndexedPhaseRemovesEntry(t *testing.T) {
	mc := &MeterDefinitionCache{uidByMeterName: make(map[string]types.UID)}

	published := makeMeterDefinition("compute.miloapis.com/disk", "uid-disk", billingv1alpha1.PhasePublished)
	mc.upsert(published)
	if _, ok := mc.GetUID("compute.miloapis.com/disk"); !ok {
		t.Fatal("expected Published meter to be present")
	}

	// Transition to Draft — should be evicted.
	draft := makeMeterDefinition("compute.miloapis.com/disk", "uid-disk", billingv1alpha1.PhaseDraft)
	mc.upsert(draft)
	if _, ok := mc.GetUID("compute.miloapis.com/disk"); ok {
		t.Error("expected meter to be removed after transitioning to Draft phase")
	}
}
