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
	"strconv"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	billingv1alpha1 "go.miloapis.com/billing/api/v1alpha1"

	"go.miloapis.com/amberflo-provider/internal/amberflo"
)

const (
	ProductPlanFinalizer = "amberflo.miloapis.com/product-plan"
	offerControllerName  = "offer"
)

// OfferReconciler syncs GA Offers into Amberflo Product Plans.
type OfferReconciler struct {
	client.Client
	AmberfloClient amberflo.Client
	Recorder      record.EventRecorder
	Log           logr.Logger
}

// +kubebuilder:rbac:groups=billing.miloapis.com,resources=offers,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=billing.miloapis.com,resources=offers/finalizers,verbs=update
// +kubebuilder:rbac:groups=billing.miloapis.com,resources=meterdefinitions,verbs=get;list;watch

// Reconcile syncs a single Offer.
func (r *OfferReconciler) Reconcile(ctx context.Context, req reconcile.Request) (ctrl.Result, error) {
	start := time.Now()
	logger := log.FromContext(ctx).WithValues("offer", req.Name)

	var result ctrl.Result
	var reconcileErr error
	defer func() {
		observeReconcileFor(offerControllerName, start, reconcileResult(result, reconcileErr))
	}()

	var offer billingv1alpha1.Offer
	if err := r.Get(ctx, req.NamespacedName, &offer); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		reconcileErr = err
		return ctrl.Result{}, err
	}

	planID := string(offer.UID)
	logger = logger.WithValues("uid", planID, "launchStage", offer.Spec.LaunchStage)

	if !offer.DeletionTimestamp.IsZero() {
		result, reconcileErr = r.reconcileDelete(ctx, logger, &offer, planID)
		return result, reconcileErr
	}

	if !controllerutil.ContainsFinalizer(&offer, ProductPlanFinalizer) {
		controllerutil.AddFinalizer(&offer, ProductPlanFinalizer)
		if err := r.Update(ctx, &offer); err != nil {
			reconcileErr = fmt.Errorf("add finalizer: %w", err)
			return ctrl.Result{}, reconcileErr
		}
		return ctrl.Result{}, nil
	}

	// Only GA Offers with a non-empty snapshot are synced. Draft Offers
	// keep the finalizer so a later publish can clean up on delete.
	if offer.Spec.LaunchStage != billingv1alpha1.OfferLaunchStageGA {
		logger.V(1).Info("skipping sync: offer is not GA")
		return ctrl.Result{}, nil
	}
	if len(offer.Spec.ServicePricings) == 0 {
		logger.Info("skipping sync: GA offer has empty servicePricings snapshot")
		if r.Recorder != nil {
			r.Recorder.Eventf(&offer, "Normal", EventReasonSyncSkipped,
				"GA Offer has no servicePricings snapshot yet")
		}
		return ctrl.Result{RequeueAfter: transientRequeueAfter}, nil
	}

	desired, err := r.desiredProductPlan(ctx, &offer)
	if err != nil {
		logger.Error(err, "build DesiredProductPlan")
		if r.Recorder != nil {
			r.Recorder.Eventf(&offer, "Warning", EventReasonSyncFailed, "%v", err)
		}
		return ctrl.Result{RequeueAfter: transientRequeueAfter}, nil
	}

	plan, err := r.AmberfloClient.EnsureProductPlan(ctx, desired)
	if err != nil {
		return r.handleAmberfloError(logger, &offer, err)
	}

	// Touch an annotation after successful sync so BillingEntitlement
	// watches re-enqueue when the plan first becomes available in Amberflo
	// (EnsureProductPlan may be a no-op that would not otherwise change the CR).
	const planSyncedAnnotation = "amberflo.miloapis.com/product-plan-id"
	if offer.Annotations == nil {
		offer.Annotations = map[string]string{}
	}
	if offer.Annotations[planSyncedAnnotation] != plan.ID {
		offer.Annotations[planSyncedAnnotation] = plan.ID
		if err := r.Update(ctx, &offer); err != nil {
			reconcileErr = fmt.Errorf("annotate product-plan-id: %w", err)
			return ctrl.Result{}, reconcileErr
		}
	}

	logger.Info("reconciled offer product plan", "planID", plan.ID, "items", len(desired.Items))
	if r.Recorder != nil {
		r.Recorder.Eventf(&offer, "Normal", EventReasonSynced,
			"Amberflo product plan %s synced", plan.ID)
	}
	return ctrl.Result{}, nil
}

func (r *OfferReconciler) reconcileDelete(
	ctx context.Context,
	logger logr.Logger,
	offer *billingv1alpha1.Offer,
	planID string,
) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(offer, ProductPlanFinalizer) {
		return ctrl.Result{}, nil
	}

	if err := r.AmberfloClient.DeleteProductPlan(ctx, planID); err != nil {
		switch {
		case amberflo.IsTransient(err):
			logger.Info("DeleteProductPlan transient failure; requeueing", "err", err.Error())
			if r.Recorder != nil {
				r.Recorder.Eventf(offer, "Warning", EventReasonDeleteFailed, "transient: %v", err)
			}
			return ctrl.Result{RequeueAfter: transientRequeueAfter}, nil
		default:
			logger.Error(err, "DeleteProductPlan permanent failure; finalizer blocks deletion")
			if r.Recorder != nil {
				r.Recorder.Eventf(offer, "Warning", EventReasonDeleteFailed, "permanent: %v", err)
			}
			return ctrl.Result{RequeueAfter: permanentDisableRequeueAfter}, nil
		}
	}

	logger.Info("Amberflo product plan deleted")
	if r.Recorder != nil {
		r.Recorder.Eventf(offer, "Normal", EventReasonDeleted, "Amberflo product plan %s deleted", planID)
	}

	controllerutil.RemoveFinalizer(offer, ProductPlanFinalizer)
	if err := r.Update(ctx, offer); err != nil {
		return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
	}
	return ctrl.Result{}, nil
}

func (r *OfferReconciler) handleAmberfloError(
	logger logr.Logger,
	offer *billingv1alpha1.Offer,
	err error,
) (ctrl.Result, error) {
	switch {
	case amberflo.IsPermanent(err):
		logger.Error(err, "Amberflo EnsureProductPlan permanent failure")
		if r.Recorder != nil {
			r.Recorder.Eventf(offer, "Warning", EventReasonSyncFailed, "%s: %v", syncReasonPermanent, err)
		}
		return ctrl.Result{}, nil
	case amberflo.IsTransient(err):
		logger.Info("Amberflo EnsureProductPlan transient failure; requeueing",
			"err", err.Error(), "requeueAfter", transientRequeueAfter.String())
		if r.Recorder != nil {
			r.Recorder.Eventf(offer, "Warning", EventReasonSyncFailed, "%s: %v", syncReasonTransient, err)
		}
		return ctrl.Result{RequeueAfter: transientRequeueAfter}, nil
	default:
		logger.Error(err, "Amberflo EnsureProductPlan unclassified failure; treating as transient")
		if r.Recorder != nil {
			r.Recorder.Eventf(offer, "Warning", EventReasonSyncFailed, "%s: %v", syncReasonInvalid, err)
		}
		return ctrl.Result{RequeueAfter: transientRequeueAfter}, nil
	}
}

func (r *OfferReconciler) desiredProductPlan(
	ctx context.Context,
	offer *billingv1alpha1.Offer,
) (amberflo.DesiredProductPlan, error) {
	name := offer.Annotations[billingv1alpha1.DisplayNameAnnotation]
	if name == "" {
		name = offer.Name
	}

	meterByName, err := r.meterAPINameIndex(ctx)
	if err != nil {
		return amberflo.DesiredProductPlan{}, err
	}

	items := make([]amberflo.DesiredPlanItem, 0, len(offer.Spec.ServicePricings))
	for _, snap := range offer.Spec.ServicePricings {
		item, err := planItemFromSnapshot(snap, meterByName)
		if err != nil {
			return amberflo.DesiredProductPlan{}, err
		}
		items = append(items, item)
	}

	return amberflo.DesiredProductPlan{
		ID:          string(offer.UID),
		Name:        name,
		Description: offer.Name,
		Items:       items,
	}, nil
}

func (r *OfferReconciler) meterAPINameIndex(ctx context.Context) (map[string]string, error) {
	var list billingv1alpha1.MeterDefinitionList
	if err := r.List(ctx, &list); err != nil {
		return nil, fmt.Errorf("list MeterDefinitions: %w", err)
	}
	out := make(map[string]string, len(list.Items))
	for i := range list.Items {
		md := &list.Items[i]
		if md.Spec.MeterName == "" {
			continue
		}
		out[md.Spec.MeterName] = string(md.UID)
	}
	return out, nil
}

func planItemFromSnapshot(
	snap billingv1alpha1.ServicePricingSnapshot,
	meterByName map[string]string,
) (amberflo.DesiredPlanItem, error) {
	label := snap.Spec.DisplayName
	if label == "" {
		label = snap.Name
	}
	item := amberflo.DesiredPlanItem{
		ID:    snap.Name,
		Label: label,
	}

	switch snap.Spec.ChargeType {
	case billingv1alpha1.ChargeTypeUsage:
		item.ChargeType = amberflo.PlanChargeTypeUsage
		meterAPIName := meterByName[snap.Spec.Metric]
		if meterAPIName == "" {
			return item, fmt.Errorf("no MeterDefinition for metric %q (ServicePricing %q)", snap.Spec.Metric, snap.Name)
		}
		item.MeterAPIName = meterAPIName
		rates, err := desiredRatesFromPricing(snap.Spec.Rates)
		if err != nil {
			return item, fmt.Errorf("ServicePricing %q: %w", snap.Name, err)
		}
		item.Rates = rates
	case billingv1alpha1.ChargeTypeOneTime:
		item.ChargeType = amberflo.PlanChargeTypeOneTime
		amount, err := amberflo.ParseDecimalFloat(snap.Spec.Amount)
		if err != nil {
			return item, fmt.Errorf("ServicePricing %q amount: %w", snap.Name, err)
		}
		item.Amount = amount
	case billingv1alpha1.ChargeTypeRecurring:
		item.ChargeType = amberflo.PlanChargeTypeRecurring
		amount, err := amberflo.ParseDecimalFloat(snap.Spec.Amount)
		if err != nil {
			return item, fmt.Errorf("ServicePricing %q amount: %w", snap.Name, err)
		}
		item.Amount = amount
	default:
		return item, fmt.Errorf("ServicePricing %q: unsupported chargeType %q", snap.Name, snap.Spec.ChargeType)
	}
	return item, nil
}

func desiredRatesFromPricing(rates []billingv1alpha1.PricingRate) ([]amberflo.DesiredPlanRate, error) {
	out := make([]amberflo.DesiredPlanRate, 0, len(rates))
	for i, rate := range rates {
		dr := amberflo.DesiredPlanRate{}
		if rate.Match != nil {
			dr.Match = &amberflo.DimensionFilter{
				Dimension: rate.Match.Dimension,
				Value:     rate.Match.Value,
			}
		}
		switch {
		case rate.Flat != "":
			v, err := amberflo.ParseDecimalFloat(rate.Flat)
			if err != nil {
				return nil, fmt.Errorf("rates[%d].flat: %w", i, err)
			}
			dr.Flat = &v
		case len(rate.Tiered) > 0:
			var startAfter int64
			for j, band := range rate.Tiered {
				price, err := amberflo.ParseDecimalFloat(band.Rate)
				if err != nil {
					return nil, fmt.Errorf("rates[%d].tiered[%d].rate: %w", i, j, err)
				}
				dr.Tiers = append(dr.Tiers, amberflo.DesiredPriceTier{
					StartAfterUnit:    startAfter,
					BatchSize:         1,
					PricePerBatch:     price,
					AllowPartialBatch: true,
				})
				if band.UpTo == "" {
					continue
				}
				upTo, err := parseUpToInt(band.UpTo)
				if err != nil {
					return nil, fmt.Errorf("rates[%d].tiered[%d].upTo: %w", i, j, err)
				}
				startAfter = upTo
			}
		default:
			return nil, fmt.Errorf("rates[%d]: exactly one of flat or tiered must be set", i)
		}
		out = append(out, dr)
	}
	return out, nil
}

func parseUpToInt(s string) (int64, error) {
	// upTo may be an integer decimal string ("100" or "100.0"). Truncate
	// toward zero for Amberflo startAfterUnit which is an integer.
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	return int64(f), nil
}

// SetupWithManager registers the Offer reconciler.
func (r *OfferReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Client == nil {
		r.Client = mgr.GetClient()
	}
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor("amberflo-provider") //nolint:staticcheck // SA1019: GetEventRecorder (events/v1) is a larger migration.
	}
	if r.Log.GetSink() == nil {
		r.Log = mgr.GetLogger().WithName("offer-controller")
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named(offerControllerName).
		For(&billingv1alpha1.Offer{}).
		Complete(r)
}
