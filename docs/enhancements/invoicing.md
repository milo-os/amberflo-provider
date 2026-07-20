---
status: provisional
stage: alpha
latest-milestone: "v0"
---

<!-- omit from toc -->
# Invoicing

- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Stripe Customer ID Sync](#stripe-customer-id-sync)
  - [Invoice Webhook Receiver](#invoice-webhook-receiver)
  - [Invoice Reconciliation Fallback](#invoice-reconciliation-fallback)
  - [RBAC](#rbac)
- [Risks and Mitigations](#risks-and-mitigations)
- [Future Work](#future-work)
- [Alternatives](#alternatives)
  - [Poll Amberflo's Invoice API Only](#poll-amberflos-invoice-api-only)
  - [A Kubernetes CRD for Provider Configuration](#a-kubernetes-crd-for-provider-configuration)
- [References](#references)

## Summary

[milo-os/billing][billing-invoicing] defines the vendor-agnostic `Invoice`
contract every invoicing provider follows. This enhancement is Amberflo's
implementation of that contract: `amberflo-provider` links a
`BillingAccount` to Stripe through Amberflo, learns when Amberflo has
computed an invoice, and keeps `Invoice` in sync as payment status changes —
so account owners, support, and finance can tell whether an account is
current without ever touching Amberflo directly.

## Motivation

The platform decided Amberflo will own invoice generation and charging
directly through its native Stripe integration, rather than driving charges
through `stripe-provider` itself. Implementing that requires two new links:
a `BillingAccount`'s Stripe customer id has to reach Amberflo before it can
charge anything, and Amberflo's own billing-cycle outcome has to reach Milo
once it computes an invoice.

### Goals

- Sync the Stripe customer id already collected via `stripe-provider` into
  Amberflo's `Customer.stripeId` trait, so Amberflo can charge through its
  native Stripe integration.
- Receive Amberflo's invoice-ready signal and create/update `Invoice` per
  the contract in [milo-os/billing][billing-invoicing].
- Keep `Invoice` in sync as payment status changes.
- Implement the RBAC boundaries the generic contract assumes of an
  invoicing provider.

### Non-Goals

- Redefining the `Invoice` contract — that's decided in
  [milo-os/billing][billing-invoicing], not here.
- Charging directly through Stripe — Amberflo's native integration drives
  the actual charge once the Stripe customer id is linked.
- Tax, pricing, and rate-card logic — already Amberflo's responsibility.
- A general-purpose inbound webhook framework — this is specifically for
  Amberflo's invoice events.

## Proposal

### Stripe Customer ID Sync

The controller resolves a `BillingAccount`'s Stripe customer id —
`BillingAccount.spec.defaultPaymentMethodRef` → `PaymentMethod` (must be
`Active`) → `StripePaymentMethod.status.stripeCustomerId` — and syncs it
onto the matching Amberflo customer record, read-only RBAC scoped to that
one field, matching the narrow exception documented in
[milo-os/billing][billing-invoicing]'s Cross-Provider Identity Resolution.

The sync only runs once `BillingAccount.status.DefaultPaymentMethodReady` is
`True`. Until then, Amberflo has nothing to charge against, and any
`Invoice` created in the meantime surfaces `PastDue`.

### Invoice Webhook Receiver

A new webhook receiver handles Amberflo's `ready-product-invoices` event
(and its payment-status-changed equivalent). On receipt:

1. Verify the request came from Amberflo; reject anything that doesn't
   verify.
2. Extract the Amberflo customer id (== `BillingAccount` name) and invoice
   id from the payload.
3. Fetch full invoice detail from Amberflo's invoice API.
4. Create or update `Invoice`, using the deterministic
   `<billing-account>-<year>-<month>` name from the generic contract,
   normalizing Amberflo's invoice status into `phase`
   (`Open`/`Paid`/`PastDue`/`Void`), and recording Amberflo's own invoice id
   under the `amberflo.billing.miloapis.com/invoiceKey` annotation.

A payment-status-changed event reuses the same create/update path against
the existing `Invoice`.

### Invoice Reconciliation Fallback

The generic contract doesn't require the webhook to be the only detection
mechanism. The controller also checks Amberflo's invoice-list API for the
customer on its normal reconcile loop, using the same create/update path as
the webhook handler — a missed webhook delivery self-heals on the next
reconcile instead of leaving `Invoice` stale.

### RBAC

Cluster-wide: read `BillingAccount`, create/update `Invoice`, read
`PaymentMethod`. Narrow: read
`StripePaymentMethod.status.stripeCustomerId` only — not general read
access to `stripe-provider`'s CRDs.

## Risks and Mitigations

A missed or delayed webhook delivery is covered by the reconcile-loop
fallback above, so it doesn't leave `Invoice` permanently stale.

An unverified request to the webhook endpoint could forge an invoice
update; the signing-secret check rejects anything that doesn't verify
before any `Invoice` write happens.

Syncing `stripeId` before a payment method is actually linked would let
Amberflo attempt to charge against nothing. The
`DefaultPaymentMethodReady` gate prevents that ordering.

## Future Work

- Tuning the reconcile-loop polling cadence once real invoice volume is
  known.
- Metrics and alerting on webhook delivery and invoice-fetch failures.
- Handling Amberflo invoice voids and credits, once that becomes a real
  scenario.

## Alternatives

### Poll Amberflo's Invoice API Only

Considered dropping the webhook receiver entirely and relying solely on the
reconcile-loop poll. Rejected: polling only on the reconcile interval means
invoice and payment status updates lag by up to that interval. The webhook
gives near-real-time updates without extra Amberflo API calls; polling
stays as a fallback, not the primary path.

### A Kubernetes CRD for Provider Configuration

Considered introducing a Kubernetes CRD for the provider's own
configuration, matching the generic contract's assumption that provider
config lives in a CRD. Rejected: the provider's configuration today is
file/Secret-based, not a Kubernetes object. Introducing a CRD for just this
one integration would split configuration across two mechanisms for no
real benefit.

## References

[billing-invoicing]: https://github.com/milo-os/billing/blob/main/docs/enhancements/invoicing.md
[payment-methods]: https://github.com/milo-os/billing/blob/main/docs/enhancements/payment-methods.md
[amberflo-docs]: https://docs.amberflo.io/
