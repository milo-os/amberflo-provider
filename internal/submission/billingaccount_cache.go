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

// BillingAccountCache maintains a thread-safe in-memory index of BillingAccount
// UIDs keyed by metadata.name. The UID is the Amberflo customerId used when
// submitting usage records.
type BillingAccountCache struct {
	mu        sync.RWMutex
	uidByName map[string]types.UID
}

// NewBillingAccountCache registers event handlers on the BillingAccount
// informer and returns a cache ready to use once the manager cache syncs.
func NewBillingAccountCache(ctx context.Context, c runtimecache.Cache) (*BillingAccountCache, error) {
	bc := &BillingAccountCache{
		uidByName: make(map[string]types.UID),
	}

	informer, err := c.GetInformer(ctx, &billingv1alpha1.BillingAccount{})
	if err != nil {
		return nil, fmt.Errorf("getting BillingAccount informer: %w", err)
	}

	if _, err := informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if a, ok := obj.(*billingv1alpha1.BillingAccount); ok {
				bc.upsert(a)
			}
		},
		UpdateFunc: func(_, newObj any) {
			if a, ok := newObj.(*billingv1alpha1.BillingAccount); ok {
				bc.upsert(a)
			}
		},
		DeleteFunc: func(obj any) {
			if tombstone, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
				obj = tombstone.Obj
			}
			if a, ok := obj.(*billingv1alpha1.BillingAccount); ok {
				bc.delete(a)
			}
		},
	}); err != nil {
		return nil, fmt.Errorf("adding BillingAccount event handler: %w", err)
	}

	return bc, nil
}

func (b *BillingAccountCache) upsert(a *billingv1alpha1.BillingAccount) {
	b.mu.Lock()
	b.uidByName[a.Name] = a.UID
	b.mu.Unlock()
}

func (b *BillingAccountCache) delete(a *billingv1alpha1.BillingAccount) {
	b.mu.Lock()
	delete(b.uidByName, a.Name)
	b.mu.Unlock()
}

// GetUID returns the UID of the BillingAccount with the given name, or false
// if not found.
func (b *BillingAccountCache) GetUID(name string) (types.UID, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	uid, ok := b.uidByName[name]
	return uid, ok
}
