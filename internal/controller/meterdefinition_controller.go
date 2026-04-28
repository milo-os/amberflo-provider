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
	// MeterFinalizer blocks deletion of a MeterDefinition until the
	// matching Amberflo meter has been removed (or a 404 confirms it is
	// already gone). The name intentionally shares the
	// amberflo.miloapis.com domain with sibling finalizers so operators
	// can grep a single prefix.
	MeterFinalizer = "amberflo.miloapis.com/meter"

	// Event reasons specific to the meter-definition reconciler.
	EventReasonSyncSkipped = "SyncSkipped"
	EventReasonDeleted     = "Deleted"
	EventReasonDeleteFailed = "DeleteFailed"

	// SyncSkipped reason tags surfaced in event messages.
	skipReasonUnsupportedAggregation = "UnsupportedAggregation"
	// skipReasonMissingDimensions is emitted when a UniqueCount meter
	// declares no dimensions — Amberflo's active_users meterType
	// requires at least one aggregationDimension or it 400s on POST.
	skipReasonMissingDimensions = "MissingDimensions"

	// Amberflo meterType wire values empirically accepted on POST
	// /meters today. Other documented values (e.g. event_duration,
	// per_event, ...) return 400 "Invalid meter type".
	amberfloMeterTypeSumOfAllUsage = "sum_of_all_usage"
	amberfloMeterTypeActiveUsers   = "active_users"

	// meterControllerName is the controller label value used for metrics
	// and the SetupWithManager "Named" call.
	meterControllerName = "meterdefinition"
)

// MeterDefinitionReconciler reconciles a services.miloapis.com/v1alpha1
// MeterDefinition into an Amberflo meter via the amberflo.Client. State
// surfacing happens via Kubernetes Events and the shared
// amberflo_provider_reconcile_duration_seconds metric — the reconciler
// does NOT write to MeterDefinition.status: billing's own controller
// owns the Ready condition there, and we deliberately avoid a dual-writer
// race on that field.
type MeterDefinitionReconciler struct {
	client.Client

	// AmberfloClient is the typed wrapper around the Amberflo REST API.
	// Shared with BillingAccountReconciler at startup.
	AmberfloClient amberflo.Client

	// Recorder emits Kubernetes events onto reconciled MeterDefinitions.
	Recorder record.EventRecorder

	// Log is the reconciler-scoped logger. Each Reconcile call derives a
	// per-reconcile logger with meter/UID/apiName values.
	Log logr.Logger
}

// +kubebuilder:rbac:groups=billing.miloapis.com,resources=meterdefinitions,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=billing.miloapis.com,resources=meterdefinitions/finalizers,verbs=update

// Reconcile runs a single sync iteration for a MeterDefinition.
func (r *MeterDefinitionReconciler) Reconcile(ctx context.Context, req reconcile.Request) (ctrl.Result, error) {
	start := time.Now()
	logger := log.FromContext(ctx).WithValues("meter", req.Name)

	var result ctrl.Result
	var reconcileErr error
	defer func() {
		observeReconcileFor(meterControllerName, start, reconcileResult(result, reconcileErr))
	}()

	var md billingv1alpha1.MeterDefinition
	if err := r.Get(ctx, req.NamespacedName, &md); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		reconcileErr = err
		return ctrl.Result{}, err
	}

	apiName := string(md.UID)
	logger = logger.WithValues("uid", apiName, "meterName", md.Spec.MeterName)

	// Deletion path: release the Amberflo meter, then drop the finalizer.
	if !md.DeletionTimestamp.IsZero() {
		result, reconcileErr = r.reconcileDelete(ctx, logger, &md, apiName)
		return result, reconcileErr
	}

	// Ensure the finalizer is attached before we touch Amberflo so we
	// never create a meter we cannot clean up. A plain Update is fine —
	// billing has its own controller that owns status, so we are only
	// contending on metadata here.
	if !controllerutil.ContainsFinalizer(&md, MeterFinalizer) {
		controllerutil.AddFinalizer(&md, MeterFinalizer)
		if err := r.Update(ctx, &md); err != nil {
			reconcileErr = fmt.Errorf("add finalizer: %w", err)
			return ctrl.Result{}, reconcileErr
		}
		// Requeue naturally via the watch on the updated object.
		return ctrl.Result{}, nil
	}

	// Amberflo's POST /meters only accepts a narrow subset of meterTypes
	// today (sum_of_all_usage, active_users, event_duration). We map
	// Sum/Count→sum_of_all_usage and UniqueCount→active_users;
	// Max/Min/Latest/Average have no native Amberflo equivalent and are
	// skipped. Revisit if amberflo-provider grows derived-meter-group
	// support for the unmapped values.
	meterType, ok := amberfloMeterType(md.Spec.Measurement.Aggregation)
	if !ok {
		logger.Info("skipping sync: unsupported aggregation",
			"aggregation", md.Spec.Measurement.Aggregation)
		if r.Recorder != nil {
			r.Recorder.Eventf(&md, "Normal", EventReasonSyncSkipped,
				"%s: aggregation=%s is not supported by Amberflo",
				skipReasonUnsupportedAggregation, md.Spec.Measurement.Aggregation)
		}
		// Return nil (no error) so the reconciler does not retry —
		// spec.measurement.aggregation is immutable, so a retry would
		// yield the same outcome.
		return ctrl.Result{}, nil
	}

	// active_users requires ≥1 aggregationDimension. Skip with a
	// distinct reason so operators can spot the misconfiguration before
	// Amberflo would reject it.
	if meterType == amberfloMeterTypeActiveUsers && len(md.Spec.Measurement.Dimensions) == 0 {
		logger.Info("skipping sync: UniqueCount requires at least one dimension")
		if r.Recorder != nil {
			r.Recorder.Eventf(&md, "Normal", EventReasonSyncSkipped,
				"%s: aggregation=%s requires spec.measurement.dimensions to have at least one entry",
				skipReasonMissingDimensions, md.Spec.Measurement.Aggregation)
		}
		return ctrl.Result{}, nil
	}

	desired := desiredMeterFromDefinition(&md, meterType)
	logger = logger.WithValues("apiName", desired.APIName)

	meter, err := r.AmberfloClient.EnsureMeter(ctx, desired)
	if err != nil {
		return r.handleAmberfloError(logger, &md, err)
	}

	logger.Info("reconciled meter definition",
		"meterType", desired.MeterType,
		"unit", desired.Unit,
		"dimensions", len(desired.Dimensions),
		"aggregationDimensions", len(desired.AggregationDimensions),
	)
	if r.Recorder != nil {
		r.Recorder.Eventf(&md, "Normal", EventReasonSynced,
			"Amberflo meter %s synced (active)", meter.APIName)
	}
	return ctrl.Result{}, nil
}

// reconcileDelete removes the Amberflo meter and then releases the
// finalizer. 404s from the Amberflo side are treated as success by the
// client, mirroring DisableCustomer's tolerant-not-found pattern.
func (r *MeterDefinitionReconciler) reconcileDelete(
	ctx context.Context,
	logger logr.Logger,
	md *billingv1alpha1.MeterDefinition,
	apiName string,
) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(md, MeterFinalizer) {
		return ctrl.Result{}, nil
	}

	if err := r.AmberfloClient.DeleteMeter(ctx, apiName); err != nil {
		switch {
		case amberflo.IsTransient(err):
			logger.Info("DeleteMeter transient failure; requeueing",
				"err", err.Error())
			if r.Recorder != nil {
				r.Recorder.Eventf(md, "Warning", EventReasonDeleteFailed,
					"transient: %v", err)
			}
			return ctrl.Result{RequeueAfter: transientRequeueAfter}, nil
		default:
			logger.Error(err, "DeleteMeter permanent failure; finalizer blocks deletion")
			if r.Recorder != nil {
				r.Recorder.Eventf(md, "Warning", EventReasonDeleteFailed,
					"permanent: %v", err)
			}
			return ctrl.Result{RequeueAfter: permanentDisableRequeueAfter}, nil
		}
	}

	logger.Info("Amberflo meter deleted")
	if r.Recorder != nil {
		r.Recorder.Eventf(md, "Normal", EventReasonDeleted,
			"Amberflo meter %s deleted", apiName)
	}

	controllerutil.RemoveFinalizer(md, MeterFinalizer)
	if err := r.Update(ctx, md); err != nil {
		return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
	}
	return ctrl.Result{}, nil
}

// handleAmberfloError mirrors the BillingAccount variant — classify the
// error, emit an event, and decide whether to requeue. Unclassified
// errors are treated as transient so a misclassification never wedges
// the reconcile loop.
func (r *MeterDefinitionReconciler) handleAmberfloError(
	logger logr.Logger,
	md *billingv1alpha1.MeterDefinition,
	err error,
) (ctrl.Result, error) {
	switch {
	case amberflo.IsPermanent(err):
		logger.Error(err, "Amberflo EnsureMeter permanent failure")
		if r.Recorder != nil {
			r.Recorder.Eventf(md, "Warning", EventReasonSyncFailed,
				"%s: %v", syncReasonPermanent, err)
		}
		return ctrl.Result{}, nil
	case amberflo.IsTransient(err):
		logger.Info("Amberflo EnsureMeter transient failure; requeueing",
			"err", err.Error(),
			"requeueAfter", transientRequeueAfter.String())
		if r.Recorder != nil {
			r.Recorder.Eventf(md, "Warning", EventReasonSyncFailed,
				"%s: %v", syncReasonTransient, err)
		}
		return ctrl.Result{RequeueAfter: transientRequeueAfter}, nil
	default:
		logger.Error(err, "Amberflo EnsureMeter unclassified failure; treating as transient")
		if r.Recorder != nil {
			r.Recorder.Eventf(md, "Warning", EventReasonSyncFailed,
				"%s: %v", syncReasonInvalid, err)
		}
		return ctrl.Result{RequeueAfter: transientRequeueAfter}, nil
	}
}

// amberfloMeterType maps a Milo MeterAggregation onto the Amberflo
// `meterType` wire value. Returns false for aggregations Amberflo does
// not expose — Max, Min, Latest, and Average — so the caller can skip
// the sync.
//
// Mapping (empirically verified against app.amberflo.io):
//   - Sum         → sum_of_all_usage
//   - Count       → sum_of_all_usage (relies on producer sending
//     value=1 per event; documented for ingest-side authors)
//   - UniqueCount → active_users (requires ≥1 aggregationDimension)
//   - Max/Min/Latest/Average → unsupported, return false
//
// Revisit if/when amberflo-provider grows support for derived-meter-group
// rollups.
func amberfloMeterType(a billingv1alpha1.MeterAggregation) (string, bool) {
	switch a {
	case billingv1alpha1.MeterAggregationSum,
		billingv1alpha1.MeterAggregationCount:
		return amberfloMeterTypeSumOfAllUsage, true
	case billingv1alpha1.MeterAggregationUniqueCount:
		return amberfloMeterTypeActiveUsers, true
	default:
		return "", false
	}
}

// desiredMeterFromDefinition translates a MeterDefinition into the
// amberflo-client DesiredMeter shape.
//
// APIName is string(UID) — the design brief locks this in for
// charset/length safety: spec.meterName is a reverse-DNS path that can
// exceed Amberflo's id character set. Label is spec.displayName with a
// fallback to spec.meterName so the Amberflo UI always has a
// non-empty human-readable label.
//
// AggregationDimensions handling: active_users requires the full
// dimension list to identify the "thing" being uniquely counted (e.g.
// distinct user_id by region). sum_of_all_usage does not use it; we
// send `[]` to be explicit about the intent.
func desiredMeterFromDefinition(
	md *billingv1alpha1.MeterDefinition,
	meterType string,
) amberflo.DesiredMeter {
	label := md.Spec.DisplayName
	if label == "" {
		label = md.Spec.MeterName
	}
	dims := append([]string(nil), md.Spec.Measurement.Dimensions...)
	var aggDims []string
	if meterType == amberfloMeterTypeActiveUsers {
		aggDims = append([]string(nil), md.Spec.Measurement.Dimensions...)
	}
	return amberflo.DesiredMeter{
		APIName:               string(md.UID),
		Label:                 label,
		MeterType:             meterType,
		AggregationDimensions: aggDims,
		Unit:                  md.Spec.Measurement.Unit,
		Dimensions:            dims,
	}
}

// SetupWithManager registers the reconciler with mgr, wiring a watch on
// MeterDefinition. There is no periodic resync and no cross-resource
// watch — spec fields we care about are immutable (except DisplayName
// and Description), so relying on the object watch is sufficient.
func (r *MeterDefinitionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Client == nil {
		r.Client = mgr.GetClient()
	}
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor("amberflo-provider")
	}
	if r.Log.GetSink() == nil {
		r.Log = mgr.GetLogger().WithName("meterdefinition-controller")
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named(meterControllerName).
		For(&billingv1alpha1.MeterDefinition{}).
		Complete(r)
}
