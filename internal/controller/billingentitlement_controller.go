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
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	billingv1alpha1 "go.miloapis.com/billing/api/v1alpha1"

	"go.miloapis.com/amberflo-provider/internal/amberflo"
)

const (
	CustomerPlanFinalizer = "amberflo.miloapis.com/customer-plan"
	beControllerName      = "billingentitlement"

	// Annotation tracking the last product plan assigned so a swap can
	// cancel the previous assignment even if ListCustomerPlans races.
	lastProductPlanAnnotation = "amberflo.miloapis.com/last-product-plan-id"
	// Annotation tracking the Amberflo customer id (= BillingAccount.UID)
	// so delete can cancel when the BillingAccount is already gone.
	lastCustomerIDAnnotation = "amberflo.miloapis.com/last-customer-id"
)

// BillingEntitlementReconciler syncs BillingEntitlements into Amberflo
// customer-plan assignments.
type BillingEntitlementReconciler struct {
	client.Client
	AmberfloClient amberflo.Client
	Recorder      record.EventRecorder
	Log           logr.Logger
}

// +kubebuilder:rbac:groups=billing.miloapis.com,resources=billingentitlements,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=billing.miloapis.com,resources=billingentitlements/finalizers,verbs=update
// +kubebuilder:rbac:groups=billing.miloapis.com,resources=billingaccounts,verbs=get;list;watch
// +kubebuilder:rbac:groups=billing.miloapis.com,resources=offers,verbs=get;list;watch

// Reconcile syncs a single BillingEntitlement.
func (r *BillingEntitlementReconciler) Reconcile(ctx context.Context, req reconcile.Request) (ctrl.Result, error) {
	start := time.Now()
	logger := log.FromContext(ctx).WithValues(
		"billingEntitlement", req.Name,
		"namespace", req.Namespace,
	)

	var result ctrl.Result
	var reconcileErr error
	defer func() {
		observeReconcileFor(beControllerName, start, reconcileResult(result, reconcileErr))
	}()

	var be billingv1alpha1.BillingEntitlement
	if err := r.Get(ctx, req.NamespacedName, &be); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		reconcileErr = err
		return ctrl.Result{}, err
	}

	if !be.DeletionTimestamp.IsZero() {
		result, reconcileErr = r.reconcileDelete(ctx, logger, &be)
		return result, reconcileErr
	}

	if !controllerutil.ContainsFinalizer(&be, CustomerPlanFinalizer) {
		controllerutil.AddFinalizer(&be, CustomerPlanFinalizer)
		if err := r.Update(ctx, &be); err != nil {
			reconcileErr = fmt.Errorf("add finalizer: %w", err)
			return ctrl.Result{}, reconcileErr
		}
		return ctrl.Result{}, nil
	}

	var account billingv1alpha1.BillingAccount
	if err := r.Get(ctx, types.NamespacedName{
		Name:      be.Spec.BillingAccountRef.Name,
		Namespace: be.Namespace,
	}, &account); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("BillingAccount not found; requeueing")
			return ctrl.Result{RequeueAfter: transientRequeueAfter}, nil
		}
		reconcileErr = err
		return ctrl.Result{}, err
	}

	var offer billingv1alpha1.Offer
	if err := r.Get(ctx, types.NamespacedName{Name: be.Spec.OfferRef.Name}, &offer); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Offer not found; requeueing")
			return ctrl.Result{RequeueAfter: transientRequeueAfter}, nil
		}
		reconcileErr = err
		return ctrl.Result{}, err
	}

	if offer.Spec.LaunchStage != billingv1alpha1.OfferLaunchStageGA {
		logger.Info("Offer not GA yet; requeueing", "offer", offer.Name)
		if r.Recorder != nil {
			r.Recorder.Eventf(&be, "Normal", EventReasonSyncSkipped,
				"Offer %s is not GA yet", offer.Name)
		}
		return ctrl.Result{RequeueAfter: transientRequeueAfter}, nil
	}
	if len(offer.Spec.ServicePricings) == 0 {
		logger.Info("Offer has empty servicePricings; requeueing", "offer", offer.Name)
		return ctrl.Result{RequeueAfter: transientRequeueAfter}, nil
	}

	customerID := string(account.UID)
	productPlanID := string(offer.UID)
	logger = logger.WithValues("customerID", customerID, "productPlanID", productPlanID)

	// Wait for the Offer reconciler to create the Amberflo product plan.
	// Assigning before the plan exists returns a permanent 4xx that would
	// otherwise stop reconcile with no retry, and Offer sync does not
	// always mutate the Offer CR so the watch may not re-enqueue us.
	if _, err := r.AmberfloClient.GetProductPlan(ctx, productPlanID); err != nil {
		if errors.Is(err, amberflo.ErrProductPlanNotFound) {
			logger.Info("product plan not in Amberflo yet; requeueing")
			return ctrl.Result{RequeueAfter: transientRequeueAfter}, nil
		}
		return r.handleAmberfloError(logger, &be, err)
	}

	// Best-effort cancel of the previous plan when offerRef changed.
	// EnsureCustomerPlan also cancels stray active plans, so a permanent
	// cancel failure must not block assignment.
	if prev := be.Annotations[lastProductPlanAnnotation]; prev != "" && prev != productPlanID {
		if err := r.AmberfloClient.CancelCustomerPlan(ctx, customerID, prev); err != nil {
			logger.Info("CancelCustomerPlan for previous plan failed; continuing with Ensure",
				"prevPlan", prev, "err", err.Error())
			if r.Recorder != nil {
				r.Recorder.Eventf(&be, "Warning", EventReasonSyncFailed,
					"cancel previous plan %s: %v (continuing)", prev, err)
			}
		}
	}

	cp, err := r.AmberfloClient.EnsureCustomerPlan(ctx, amberflo.DesiredCustomerPlan{
		CustomerID:    customerID,
		ProductPlanID: productPlanID,
	})
	if err != nil {
		return r.handleAmberfloError(logger, &be, err)
	}

	if be.Annotations == nil {
		be.Annotations = map[string]string{}
	}
	needAnnotate := be.Annotations[lastProductPlanAnnotation] != productPlanID ||
		be.Annotations[lastCustomerIDAnnotation] != customerID
	if needAnnotate {
		be.Annotations[lastProductPlanAnnotation] = productPlanID
		be.Annotations[lastCustomerIDAnnotation] = customerID
		if err := r.Update(ctx, &be); err != nil {
			reconcileErr = fmt.Errorf("update last-product-plan annotation: %w", err)
			return ctrl.Result{}, reconcileErr
		}
	}

	logger.Info("reconciled billing entitlement customer plan",
		"relationID", cp.RelationID)
	if r.Recorder != nil {
		r.Recorder.Eventf(&be, "Normal", EventReasonSynced,
			"Amberflo customer plan %s→%s synced", customerID, productPlanID)
	}
	return ctrl.Result{}, nil
}

func (r *BillingEntitlementReconciler) reconcileDelete(
	ctx context.Context,
	logger logr.Logger,
	be *billingv1alpha1.BillingEntitlement,
) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(be, CustomerPlanFinalizer) {
		return ctrl.Result{}, nil
	}

	customerID, productPlanID, resolveErr := r.resolveIDs(ctx, be)
	annotatedCustomer := be.Annotations[lastCustomerIDAnnotation]
	annotatedPlan := be.Annotations[lastProductPlanAnnotation]

	if customerID == "" {
		customerID = annotatedCustomer
	}
	if productPlanID == "" {
		productPlanID = annotatedPlan
	}
	if customerID == "" || productPlanID == "" {
		logger.Info("unable to resolve customer/plan for cancel; releasing finalizer",
			"resolveErr", errString(resolveErr))
		controllerutil.RemoveFinalizer(be, CustomerPlanFinalizer)
		if updateErr := r.Update(ctx, be); updateErr != nil {
			return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", updateErr)
		}
		return ctrl.Result{}, nil
	}

	plansToCancel := map[string]struct{}{productPlanID: {}}
	if annotatedPlan != "" {
		plansToCancel[annotatedPlan] = struct{}{}
	}
	for planID := range plansToCancel {
		if err := r.AmberfloClient.CancelCustomerPlan(ctx, customerID, planID); err != nil {
			switch {
			case amberflo.IsTransient(err):
				logger.Info("CancelCustomerPlan transient failure; requeueing", "plan", planID, "err", err.Error())
				if r.Recorder != nil {
					r.Recorder.Eventf(be, "Warning", EventReasonDeleteFailed, "transient: %v", err)
				}
				return ctrl.Result{RequeueAfter: transientRequeueAfter}, nil
			default:
				logger.Error(err, "CancelCustomerPlan permanent failure; finalizer blocks deletion", "plan", planID)
				if r.Recorder != nil {
					r.Recorder.Eventf(be, "Warning", EventReasonDeleteFailed, "permanent: %v", err)
				}
				return ctrl.Result{RequeueAfter: permanentDisableRequeueAfter}, nil
			}
		}
	}

	logger.Info("Amberflo customer plan cancelled")
	if r.Recorder != nil {
		r.Recorder.Eventf(be, "Normal", EventReasonDeleted,
			"Amberflo customer plan %s→%s cancelled", customerID, productPlanID)
	}

	controllerutil.RemoveFinalizer(be, CustomerPlanFinalizer)
	if err := r.Update(ctx, be); err != nil {
		return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
	}
	return ctrl.Result{}, nil
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (r *BillingEntitlementReconciler) resolveIDs(
	ctx context.Context,
	be *billingv1alpha1.BillingEntitlement,
) (customerID, productPlanID string, err error) {
	var account billingv1alpha1.BillingAccount
	if getErr := r.Get(ctx, types.NamespacedName{
		Name:      be.Spec.BillingAccountRef.Name,
		Namespace: be.Namespace,
	}, &account); getErr != nil {
		return "", "", getErr
	}
	customerID = string(account.UID)

	var offer billingv1alpha1.Offer
	if getErr := r.Get(ctx, types.NamespacedName{Name: be.Spec.OfferRef.Name}, &offer); getErr != nil {
		return customerID, "", getErr
	}
	return customerID, string(offer.UID), nil
}

func (r *BillingEntitlementReconciler) handleAmberfloError(
	logger logr.Logger,
	be *billingv1alpha1.BillingEntitlement,
	err error,
) (ctrl.Result, error) {
	switch {
	case amberflo.IsPermanent(err):
		logger.Error(err, "Amberflo EnsureCustomerPlan permanent failure")
		if r.Recorder != nil {
			r.Recorder.Eventf(be, "Warning", EventReasonSyncFailed, "%s: %v", syncReasonPermanent, err)
		}
		return ctrl.Result{}, nil
	case amberflo.IsTransient(err):
		logger.Info("Amberflo EnsureCustomerPlan transient failure; requeueing",
			"err", err.Error(), "requeueAfter", transientRequeueAfter.String())
		if r.Recorder != nil {
			r.Recorder.Eventf(be, "Warning", EventReasonSyncFailed, "%s: %v", syncReasonTransient, err)
		}
		return ctrl.Result{RequeueAfter: transientRequeueAfter}, nil
	default:
		logger.Error(err, "Amberflo EnsureCustomerPlan unclassified failure; treating as transient")
		if r.Recorder != nil {
			r.Recorder.Eventf(be, "Warning", EventReasonSyncFailed, "%s: %v", syncReasonInvalid, err)
		}
		return ctrl.Result{RequeueAfter: transientRequeueAfter}, nil
	}
}

// SetupWithManager registers the BillingEntitlement reconciler and enqueues
// on Offer changes so GA publish / snapshot completion re-triggers BEs.
func (r *BillingEntitlementReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Client == nil {
		r.Client = mgr.GetClient()
	}
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor("amberflo-provider") //nolint:staticcheck // SA1019: GetEventRecorder (events/v1) is a larger migration.
	}
	if r.Log.GetSink() == nil {
		r.Log = mgr.GetLogger().WithName("billingentitlement-controller")
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named(beControllerName).
		For(&billingv1alpha1.BillingEntitlement{}).
		Watches(&billingv1alpha1.Offer{},
			handler.EnqueueRequestsFromMapFunc(r.mapOfferToEntitlements),
		).
		Complete(r)
}

func (r *BillingEntitlementReconciler) mapOfferToEntitlements(
	ctx context.Context,
	obj client.Object,
) []reconcile.Request {
	offer, ok := obj.(*billingv1alpha1.Offer)
	if !ok {
		return nil
	}
	var list billingv1alpha1.BillingEntitlementList
	if err := r.List(ctx, &list); err != nil {
		log.FromContext(ctx).Error(err, "list BillingEntitlements for Offer fan-out", "offer", offer.Name)
		return nil
	}
	var reqs []reconcile.Request
	for i := range list.Items {
		be := &list.Items[i]
		if be.Spec.OfferRef.Name == offer.Name {
			reqs = append(reqs, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      be.Name,
					Namespace: be.Namespace,
				},
			})
		}
	}
	return reqs
}
