# Workflow Run Guide

This guide covers the normal CLI path for checking and running workflow JSON
documents:

1. `validate`
2. `inspect`
3. `explain`
4. `run`
5. metadata JSONL and run summary interpretation

The examples use `examples/native-linear-etl.workflow.json`.

## Validate

Use `validate` first when editing workflow JSON:

```bash
cargo run -p conduit-cli -- validate examples/native-linear-etl.workflow.json
```

Expected output:

```text
valid workflow `native-linear-etl`
nodes: 3
edges: 2
```

`validate` checks the external JSON format and the static graph structure. It
rejects malformed JSON, missing or unsupported `conduit_version`, invalid
identifiers, duplicate nodes or ports, unknown edge endpoints, invalid edge
capacity, and cycles unless a later runtime path explicitly opts into cycle
execution.

## Inspect

Use `inspect` when tooling or review needs a machine-readable view of the
workflow boundary:

```bash
cargo run -p conduit-cli -- inspect examples/native-linear-etl.workflow.json
```

The command prints JSON containing the workflow id, nodes, ports, edge
capacities, execution mode, enforcement level, determinism, retry declaration,
and declared effects. The current CLI projects workflow nodes as passive native
contracts for inspection unless a richer contract source is introduced by a
later product surface.

Use `inspect` output for automation that needs stable topology data. Use
`explain` for human review.

## Explain

Use `explain` before a run when you want a compact text summary:

```bash
cargo run -p conduit-cli -- explain examples/native-linear-etl.workflow.json
```

The output includes:

- workflow id, node count, and edge count
- execution mode summary
- metadata policy summary
- node order with input and output counts
- edge list with resolved capacity labels

`explain` does not execute nodes or write metadata. It validates and summarizes
the declared graph.

## Run

Run a workflow and write metadata JSONL:

```bash
cargo run -p conduit-cli -- run examples/native-linear-etl.workflow.json /tmp/conduit-native-linear-etl.metadata.jsonl
```

Expected text summary:

```text
ran workflow `native-linear-etl`
nodes: 3
edges: 2
metadata: /tmp/conduit-native-linear-etl.metadata.jsonl
records: 24
```

The CLI constructs a real executor registry for the workflow, runs nodes through
bounded Conduit ports, validates output ports before graph delivery, and records
runtime facts into the requested JSONL file.

The built-in native CLI executor is intentionally generic. It drains declared
input ports and emits deterministic packets on declared output ports, proving
the registry, port, metadata, and summary paths without pretending to run
domain-specific ETL code.

## Run JSON Summary

Use `--json` when automation needs a stable run summary:

```bash
cargo run -p conduit-cli -- run --json examples/native-linear-etl.workflow.json /tmp/conduit-native-linear-etl.metadata.jsonl
```

The metadata file is still written to the requested path. Standard output is a
single JSON document with:

- `status`: `completed`, `failed`, or `cancelled`
- `error`: top-level error object or `null`
- `workflow`: workflow id, node count, and edge count
- `metadata`: output path and record count
- `summary`: terminal state and node/error/deadlock counters

For successful runs, `error`, `summary.first_error`, and
`summary.deadlock_diagnostic` are `null`.

For failed runs, `error` and `summary.first_error` use the stable Conduit error
object with `code`, `message`, `visibility`, and `retry_disposition`.

`summary.observed_message_count` is reserved and currently remains `0` until
runner-level message accounting is attached.

## WASM Components

To run one or more workflow nodes as WASM components, pass a component manifest
to `run`:

```bash
cargo run -p conduit-cli -- run \
  --wasm-components wasm-components.json \
  workflow.json \
  /tmp/conduit.metadata.jsonl
```

Manifest shape:

```json
{
  "components": [
    {
      "node": "wasm-upper",
      "component": "components/uppercase.wasm",
      "fuel": 100000000
    }
  ]
}
```

Component paths are resolved relative to the manifest file. Nodes listed in the
manifest use `WasmtimeBatchComponent` wrapped by `BatchNodeExecutor`; nodes not
listed in the manifest use the native CLI executor. WASM outputs still pass
through the host-owned `PortsOut` validation boundary before they enter
downstream graph edges.

## Metadata JSONL

The metadata path passed to `run` receives one JSON object per runtime
observation. Current record families are:

- `lifecycle`: node scheduling, start, completion, failure, and cancellation
- `message`: enqueue, dequeue, and drop observations at port boundaries
- `queue_pressure`: bounded edge capacity, reserve, send, receive, and closure
  observations
- `error`: node and workflow errors with stable Conduit error codes

Metadata intentionally omits timestamps, process ids, hostnames, thread ids,
random addresses, and raw payload bytes so repeated runs remain reproducible.

For the complete record schema and run summary JSON shape, see
[metadata-json.md](metadata-json.md).

## Choosing The Command

- Use `validate` while authoring workflow JSON.
- Use `inspect` when another tool needs topology and contract JSON.
- Use `explain` when a human needs to review topology and metadata behavior.
- Use `run` for text output plus metadata JSONL.
- Use `run --json` for automation that needs a machine-facing run result.
- Add `--wasm-components` when workflow nodes should load Wasmtime components
  instead of the generic native CLI executor.
