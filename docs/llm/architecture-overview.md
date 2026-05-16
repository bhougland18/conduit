# Architecture Overview

## System Shape

Pureflow is an experimental Flow-Based Programming engine written in Rust.
It validates workflow documents, executes node graphs through bounded channels,
and emits machine-facing metadata and run summaries.

The architecture is intentionally layered:

- `conduit-cli` is the human and automation entrypoint.
- `conduit-engine` validates execution preconditions and orchestrates runs.
- `conduit-runtime` bridges engine scheduling into the async substrate.
- `conduit-core` owns runtime-facing types, ports, metadata, errors, and capabilities.
- `conduit-workflow` owns structural workflow validation.
- `conduit-contract` owns node contracts and capability/schema alignment.
- `conduit-wasm` owns Wasmtime and WIT/component ABI details.
- `conduit-introspection` provides read-only projections for inspect/explain flows.
- `conduit-test-kit` provides builders and fixtures for tests and examples.

## Core Responsibilities

Stable responsibilities:

- Workflow documents are validated before execution.
- Graph topology and port references are checked separately from runtime policy.
- Contracts and capabilities must agree before nodes run.
- Runtime execution uses bounded channels, explicit cancellation, and metadata emission.
- Run summaries and metadata JSONL are separate machine-facing artifacts.
- Native and WASM execution are both supported, but the public model stays Pureflow-owned.

Important architectural idea:

- Pureflow owns the workflow model, contracts, ports, metadata, and capabilities.
- `asupersync` is the runtime substrate, not the public model.
- WASM is an adapter boundary, not the center of the design.

## External Boundaries

Primary ingress and egress:

- Ingress: workflow documents from CLI or automation.
- Egress: terminal run summary JSON and metadata JSONL.
- Observability: lifecycle, message, queue-pressure, error, and external-effect records.
- Execution adapters: native executors and WASM batch executors.

Boundary facts to remember:

- `conduit-runtime` may use `asupersync` internally.
- `conduit-core` should not expose raw `asupersync` or Wasmtime types in its public API.
- WASM guests use a Pureflow-defined WIT world and host-owned channels.
- Capability enforcement is explicit and becomes a real boundary for WASM and future process-backed nodes.
