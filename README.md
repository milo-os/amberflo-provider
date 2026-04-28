# amberflo-provider

A Kubernetes controller that keeps Milo's billing records in sync with Amberflo's usage-based billing platform.

## What it does

When someone on the Milo platform creates a billing account or links a project to one, that intent needs to land somewhere that can actually meter usage and produce invoices. The `amberflo-provider` is the piece that makes that happen. It watches Milo's billing resources and translates them into the matching entities — customers, product plans, and subscriptions — inside Amberflo.

The goal is that platform operators never have to think about Amberflo directly. They work with Milo's native billing APIs; this provider handles the reconciliation behind the scenes so invoicing stays consistent with what's declared in the platform.

If a billing relationship changes, is renamed, or is removed, the provider reflects that change downstream so the two systems don't drift apart.

## How it fits into Milo

- **Milo Billing Service** (`milo-os/billing`) is the source of truth. It owns the `BillingAccount` and `BillingAccountBinding` custom resources in the `billing.miloapis.com/v1alpha1` API group.
- **Amberflo** is the downstream system of record for metered usage and invoicing.
- **amberflo-provider** (this repo) is the reconciler in between. It sits alongside Milo's other providers and runs as a standard Kubernetes controller.

## Status

Alpha. This project is under active development and APIs, behaviors, and deployment shape are all expected to change. It is not yet recommended for production use outside of Milo-operated environments.

## Getting started

Installation and usage documentation will be published as the project stabilizes. In the meantime:

- Kubernetes manifests and kustomize overlays live under `config/`.
- A published OCI bundle for deployment will be announced here once available.

If you are a Milo operator looking to try this out today, reach out to the team rather than attempting to self-install — the surface area is still changing.

## Development

This controller is built with [kubebuilder](https://book.kubebuilder.io/) and written in Go. Build, test, and code-generation tasks are orchestrated with [Task](https://taskfile.dev/). See the `Taskfile` and `Makefile` in the repo root for the current set of supported targets.

Contributions from inside the Milo organization are welcome; external contributors should open an issue first to discuss scope.

## License

Licensed under the [Apache License, Version 2.0](./LICENSE).

## Related projects

- [milo-os/billing](https://github.com/milo-os/billing) — the Milo Billing Service that owns the upstream CRDs this provider reconciles.
- [datum-cloud/test-infra](https://github.com/datum-cloud/test-infra) — shared tooling used to stand up end-to-end test environments (milo-os/test-infra pending migration).
- [Amberflo documentation](https://docs.amberflo.io/) — reference for the downstream billing platform.
