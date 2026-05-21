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
	"context"
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/types"
	toolscache "k8s.io/client-go/tools/cache"
	runtimecache "sigs.k8s.io/controller-runtime/pkg/cache"

	billingv1alpha1 "go.miloapis.com/billing/api/v1alpha1"
)

// MeterDefinitionCache maintains a thread-safe in-memory index of
// MeterDefinition UIDs keyed by spec.meterName. Only Published and Deprecated
// meters are indexed; entries in other phases are removed.
type MeterDefinitionCache struct {
	mu             sync.RWMutex
	uidByMeterName map[string]types.UID
}

// NewMeterDefinitionCache registers event handlers on the MeterDefinition
// informer and returns a cache ready to use once the manager cache syncs.
func NewMeterDefinitionCache(ctx context.Context, c runtimecache.Cache) (*MeterDefinitionCache, error) {
	mc := &MeterDefinitionCache{
		uidByMeterName: make(map[string]types.UID),
	}

	informer, err := c.GetInformer(ctx, &billingv1alpha1.MeterDefinition{})
	if err != nil {
		return nil, fmt.Errorf("getting MeterDefinition informer: %w", err)
	}

	if _, err := informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if md, ok := obj.(*billingv1alpha1.MeterDefinition); ok {
				mc.upsert(md)
			}
		},
		UpdateFunc: func(_, newObj any) {
			if md, ok := newObj.(*billingv1alpha1.MeterDefinition); ok {
				mc.upsert(md)
			}
		},
		DeleteFunc: func(obj any) {
			if tombstone, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
				obj = tombstone.Obj
			}
			if md, ok := obj.(*billingv1alpha1.MeterDefinition); ok {
				mc.delete(md)
			}
		},
	}); err != nil {
		return nil, fmt.Errorf("adding MeterDefinition event handler: %w", err)
	}

	return mc, nil
}

func (m *MeterDefinitionCache) upsert(md *billingv1alpha1.MeterDefinition) {
	if md.Spec.Phase != billingv1alpha1.PhasePublished && md.Spec.Phase != billingv1alpha1.PhaseDeprecated {
		m.delete(md)
		return
	}
	m.mu.Lock()
	m.uidByMeterName[md.Spec.MeterName] = md.UID
	m.mu.Unlock()
}

func (m *MeterDefinitionCache) delete(md *billingv1alpha1.MeterDefinition) {
	m.mu.Lock()
	delete(m.uidByMeterName, md.Spec.MeterName)
	m.mu.Unlock()
}

// GetUID returns the UID of the MeterDefinition with the given meter name, or
// false if not found or not in Published/Deprecated phase.
func (m *MeterDefinitionCache) GetUID(meterName string) (types.UID, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	uid, ok := m.uidByMeterName[meterName]
	return uid, ok
}
