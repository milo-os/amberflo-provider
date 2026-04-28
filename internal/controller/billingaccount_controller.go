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
	"sort"
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
	// CustomerLinkFinalizer blocks deletion of a BillingAccount until the
	// matching Amberflo customer has been soft-disabled (or until an operator
	// adds the force-delete annotation below). The name is preserved from
	// the status-CRD era for operator-facing backwards compatibility.
	CustomerLinkFinalizer = "amberflo.providers.datum.net/customer-link"

	// ForceDeleteAnnotation, when present on a BillingAccount being deleted,
	// removes CustomerLinkFinalizer even if the Amberflo disable call fails.
	// The disable attempt still runs; failures are logged as warnings and a
	// ForceDeleted event is emitted.
	ForceDeleteAnnotation = "amberflo.providers.datum.net/force-delete"

	// Event reasons emitted by the reconciler. These replace the Status CRD
	// that earlier drafts surfaced state through.
	EventReasonSynced        = "Synced"
	EventReasonSyncFailed    = "SyncFailed"
	EventReasonDisabled      = "Disabled"
	EventReasonDisableFailed = "DisableFailed"
	EventReasonForceDeleted  = "ForceDeleted"

	// SyncFailed reason tags.
	syncReasonTransient = "TransientError"
	syncReasonPermanent = "PermanentError"
	syncReasonInvalid   = "InvalidSpec"

	// transientRequeueAfter is the backoff used after a transient error
	// from Amberflo. Not using Result{Requeue:true} because controller-
	// runtime's requeue has no backoff.
	transientRequeueAfter = 30 * time.Second

	// permanentDisableRequeueAfter is the wait after a permanent disable
	// error. The finalizer still blocks deletion; operators can apply the
	// force-delete annotation to unstick.
	permanentDisableRequeueAfter = 5 * time.Minute
)

// BillingAccountReconciler reconciles a billing.miloapis.com/v1alpha1
// BillingAccount into an Amberflo customer via the amberflo.Client. State
// surfacing happens via Kubernetes Events and Prometheus metrics; there is
// no provider-owned status CRD — per-reconcile state is derived fresh from
// listing Active bindings and issuing a GET-then-diff to Amberflo.
type BillingAccountReconciler struct {
	client.Client

	// AmberfloClient is the typed wrapper around the Amberflo REST API.
	// Supplied at startup from main.go; tests construct a new client
	// against a local httptest.Server.
	AmberfloClient amberflo.Client

	// Recorder emits Kubernetes events onto reconciled BillingAccounts.
	Recorder record.EventRecorder

	// AllowCustomerDelete mirrors the server-config flag of the same name.
	// When false (default) the reconciler always soft-disables; hard delete
	// remains reserved for a future operator-guardrail flow.
	AllowCustomerDelete bool

	// Log is the reconciler-scoped logger. Each Reconcile call derives a
	// per-reconcile logger with account/namespace/customerID values.
	Log logr.Logger
}

// +kubebuilder:rbac:groups=billing.miloapis.com,resources=billingaccounts,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=billing.miloapis.com,resources=billingaccounts/finalizers,verbs=update
// +kubebuilder:rbac:groups=billing.miloapis.com,resources=billingaccountbindings,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile runs a single sync iteration for a BillingAccount.
func (r *BillingAccountReconciler) Reconcile(ctx context.Context, req reconcile.Request) (ctrl.Result, error) {
	start := time.Now()
	logger := log.FromContext(ctx).WithValues(
		"account", req.Name,
		"namespace", req.Namespace,
	)

	var result ctrl.Result
	var reconcileErr error
	defer func() {
		observeReconcile(start, reconcileResult(result, reconcileErr))
	}()

	var account billingv1alpha1.BillingAccount
	if err := r.Get(ctx, req.NamespacedName, &account); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		reconcileErr = err
		return ctrl.Result{}, err
	}

	// Deletion path: work the disable flow before releasing the finalizer.
	if !account.DeletionTimestamp.IsZero() {
		result, reconcileErr = r.reconcileDelete(ctx, logger, &account)
		return result, reconcileErr
	}

	// Ensure the finalizer is attached before we touch Amberflo so we
	// never create a customer we cannot clean up.
	if !controllerutil.ContainsFinalizer(&account, CustomerLinkFinalizer) {
		controllerutil.AddFinalizer(&account, CustomerLinkFinalizer)
		if err := r.Update(ctx, &account); err != nil {
			reconcileErr = fmt.Errorf("add finalizer: %w", err)
			return ctrl.Result{}, reconcileErr
		}
		// Requeue naturally via the watch on the updated object.
		return ctrl.Result{}, nil
	}

	// Aggregate the project list from Active BillingAccountBindings
	// referencing this account. Recomputed every reconcile — there is no
	// stashed intermediate state.
	projects, err := r.aggregateActiveBindingProjects(ctx, &account)
	if err != nil {
		reconcileErr = fmt.Errorf("aggregate active binding projects: %w", err)
		return ctrl.Result{}, reconcileErr
	}

	desired := desiredCustomerFromAccount(&account, projects)
	logger = logger.WithValues("customerID", desired.ID)

	customer, err := r.AmberfloClient.EnsureCustomer(ctx, desired)
	if err != nil {
		return r.handleAmberfloError(logger, &account, err)
	}

	logger.Info("reconciled billing account", "projects", len(desired.Projects))
	if r.Recorder != nil {
		r.Recorder.Eventf(&account, "Normal", EventReasonSynced,
			"Amberflo customer %s synced", customer.ID)
	}
	return ctrl.Result{}, nil
}

// reconcileDelete soft-disables the Amberflo customer, then releases the
// finalizer. Force-delete annotation semantics: we still attempt a
// DisableCustomer call and log its outcome, but do not block the finalizer
// removal on success.
func (r *BillingAccountReconciler) reconcileDelete(
	ctx context.Context,
	logger logr.Logger,
	account *billingv1alpha1.BillingAccount,
) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(account, CustomerLinkFinalizer) {
		return ctrl.Result{}, nil
	}

	forceDelete := account.Annotations[ForceDeleteAnnotation] == "true"
	customerID := string(account.UID)
	logger = logger.WithValues("customerID", customerID, "forceDelete", forceDelete)

	// Skip the disable call entirely when force-delete is requested — the
	// operator has explicitly opted to bypass the soft-disable guarantee.
	// The brief flags this case with a ForceDeleted event.
	if forceDelete || r.AllowCustomerDelete {
		disableErr := r.AmberfloClient.DisableCustomer(ctx, customerID)
		if disableErr != nil {
			logger.Info("DisableCustomer failed but force-delete path continues",
				"err", disableErr.Error())
			if r.Recorder != nil {
				r.Recorder.Eventf(account, "Warning", EventReasonForceDeleted,
					"DisableCustomer failed, continuing via force-delete: %v", disableErr)
			}
		} else {
			logger.Info("Amberflo customer disabled (force-delete path)")
			if r.Recorder != nil {
				r.Recorder.Eventf(account, "Normal", EventReasonDisabled,
					"Amberflo customer %s disabled", customerID)
			}
		}
		if err := r.removeFinalizer(ctx, account); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	disableErr := r.AmberfloClient.DisableCustomer(ctx, customerID)
	if disableErr != nil {
		switch {
		case amberflo.IsTransient(disableErr):
			logger.Info("DisableCustomer transient failure; requeueing",
				"err", disableErr.Error())
			if r.Recorder != nil {
				r.Recorder.Eventf(account, "Warning", EventReasonDisableFailed,
					"transient: %v", disableErr)
			}
			return ctrl.Result{RequeueAfter: transientRequeueAfter}, nil
		default:
			logger.Error(disableErr, "DisableCustomer permanent failure; finalizer blocks deletion")
			if r.Recorder != nil {
				r.Recorder.Eventf(account, "Warning", EventReasonDisableFailed,
					"permanent (annotate %s=true to force): %v",
					ForceDeleteAnnotation, disableErr)
			}
			return ctrl.Result{RequeueAfter: permanentDisableRequeueAfter}, nil
		}
	}

	logger.Info("Amberflo customer disabled")
	if r.Recorder != nil {
		r.Recorder.Eventf(account, "Normal", EventReasonDisabled,
			"Amberflo customer %s disabled", customerID)
	}

	if err := r.removeFinalizer(ctx, account); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// removeFinalizer drops the customer-link finalizer and updates the object.
// Splits out for readability in the two delete-path branches.
func (r *BillingAccountReconciler) removeFinalizer(
	ctx context.Context,
	account *billingv1alpha1.BillingAccount,
) error {
	controllerutil.RemoveFinalizer(account, CustomerLinkFinalizer)
	if err := r.Update(ctx, account); err != nil {
		return fmt.Errorf("remove finalizer: %w", err)
	}
	return nil
}

// handleAmberfloError turns a failed EnsureCustomer into an event and the
// appropriate ctrl.Result. Transient errors requeue with a fixed backoff;
// permanent errors emit a warning and stop.
func (r *BillingAccountReconciler) handleAmberfloError(
	logger logr.Logger,
	account *billingv1alpha1.BillingAccount,
	err error,
) (ctrl.Result, error) {
	switch {
	case amberflo.IsPermanent(err):
		logger.Error(err, "Amberflo EnsureCustomer permanent failure")
		if r.Recorder != nil {
			r.Recorder.Eventf(account, "Warning", EventReasonSyncFailed,
				"%s: %v", syncReasonPermanent, err)
		}
		return ctrl.Result{}, nil
	case amberflo.IsTransient(err):
		logger.Info("Amberflo EnsureCustomer transient failure; requeueing",
			"err", err.Error(),
			"requeueAfter", transientRequeueAfter.String())
		if r.Recorder != nil {
			r.Recorder.Eventf(account, "Warning", EventReasonSyncFailed,
				"%s: %v", syncReasonTransient, err)
		}
		return ctrl.Result{RequeueAfter: transientRequeueAfter}, nil
	default:
		// Unclassified errors (e.g. from context or aggregation): treat as
		// transient so we do not wedge the reconcile loop.
		logger.Error(err, "Amberflo EnsureCustomer unclassified failure; treating as transient")
		if r.Recorder != nil {
			r.Recorder.Eventf(account, "Warning", EventReasonSyncFailed,
				"%s: %v", syncReasonInvalid, err)
		}
		return ctrl.Result{RequeueAfter: transientRequeueAfter}, nil
	}
}

// aggregateActiveBindingProjects lists every BillingAccountBinding
// referencing the given account via the BindingBillingAccountRefField
// indexer, filters to status.phase == Active, collects the project names,
// and returns the deterministic sorted+deduped result.
func (r *BillingAccountReconciler) aggregateActiveBindingProjects(
	ctx context.Context,
	account *billingv1alpha1.BillingAccount,
) ([]string, error) {
	var bindings billingv1alpha1.BillingAccountBindingList
	if err := r.List(ctx, &bindings,
		client.InNamespace(account.Namespace),
		client.MatchingFields{BindingBillingAccountRefField: account.Name},
	); err != nil {
		return nil, err
	}
	return projectsFromActiveBindings(bindings.Items), nil
}

// projectsFromActiveBindings extracts the project names from bindings whose
// status.phase is Active, drops empty entries, de-duplicates, and sorts.
func projectsFromActiveBindings(items []billingv1alpha1.BillingAccountBinding) []string {
	projects := make([]string, 0, len(items))
	for i := range items {
		b := &items[i]
		if b.Status.Phase != billingv1alpha1.BillingAccountBindingPhaseActive {
			continue
		}
		if b.Spec.ProjectRef.Name == "" {
			continue
		}
		projects = append(projects, b.Spec.ProjectRef.Name)
	}
	return sortedCopy(projects)
}

// desiredCustomerFromAccount translates the Milo-side BillingAccount into
// the amberflo-client DesiredCustomer shape.
//
// ID is string(UID) — stable across renames and guaranteed unique per the
// pivot brief. Display Name is the BillingAccount name (not ContactInfo.Name)
// so operators searching Amberflo by customer name get the familiar
// BillingAccount identifier.
func desiredCustomerFromAccount(
	account *billingv1alpha1.BillingAccount,
	projects []string,
) amberflo.DesiredCustomer {
	desired := amberflo.DesiredCustomer{
		ID:           string(account.UID),
		Name:         account.Name,
		CurrencyCode: account.Spec.CurrencyCode,
		Projects:     sortedCopy(projects),
	}
	if account.Spec.ContactInfo != nil {
		desired.Email = account.Spec.ContactInfo.Email
	}
	if pt := account.Spec.PaymentTerms; pt != nil {
		desired.PaymentTerms = &amberflo.PaymentTerms{
			NetDays:           int(pt.NetDays),
			InvoiceFrequency:  pt.InvoiceFrequency,
			InvoiceDayOfMonth: int(pt.InvoiceDayOfMonth),
		}
	}
	return desired
}

// sortedCopy returns a de-duplicated, sorted copy of in. Nil in => nil out.
func sortedCopy(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// SetupWithManager registers the reconciler with mgr, wiring a watch on
// BillingAccount and a map-func watch on BillingAccountBinding that
// enqueues the parent BillingAccount.
func (r *BillingAccountReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Client == nil {
		r.Client = mgr.GetClient()
	}
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor("amberflo-provider")
	}
	if r.Log.GetSink() == nil {
		r.Log = mgr.GetLogger().WithName("billingaccount-controller")
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named("billingaccount").
		For(&billingv1alpha1.BillingAccount{}).
		Watches(&billingv1alpha1.BillingAccountBinding{},
			handler.EnqueueRequestsFromMapFunc(
				func(ctx context.Context, obj client.Object) []reconcile.Request {
					binding, ok := obj.(*billingv1alpha1.BillingAccountBinding)
					if !ok {
						return nil
					}
					return []reconcile.Request{{
						NamespacedName: types.NamespacedName{
							Name:      binding.Spec.BillingAccountRef.Name,
							Namespace: binding.Namespace,
						},
					}}
				},
			),
		).
		Complete(r)
}

