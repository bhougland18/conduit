# Examples Catalog

This catalog lists runnable examples, the command to run each one, the expected
observable output, and the product surface the example exercises.

Run commands from the repository root.

## Authoring Examples Pack

Files:

- `examples/authoring/README.md`
- `examples/authoring/native-fanout.workflow.json`
- `examples/authoring/native-join.workflow.yaml`
- `examples/authoring/wasm-uppercase.workflow.json`
- `examples/authoring/wasm-uppercase.components.json`

The authoring pack provides compact workflow shapes for generated or
hand-written workflow documents:

- native fanout: one source branches to a primary sink and audit sink
- native join: two sources feed a join-style node before a sink
- WASM uppercase: native source and sink with one manifest-loaded WASM node

See [../examples/authoring/README.md](../examples/authoring/README.md) for
validate, inspect, explain, native run, and WASM run snippets with expected
output notes.

## Native Linear ETL Workflow

Files:

- `examples/native-linear-etl.workflow.json`
- `examples/native-linear-etl.md`

Commands:

```bash
cargo run -p conduit-cli -- validate examples/native-linear-etl.workflow.json
cargo run -p conduit-cli -- inspect examples/native-linear-etl.workflow.json
cargo run -p conduit-cli -- explain examples/native-linear-etl.workflow.json
cargo run -p conduit-cli -- run examples/native-linear-etl.workflow.json /tmp/conduit-native-linear-etl.metadata.jsonl
cargo run -p conduit-cli -- run --json examples/native-linear-etl.workflow.json /tmp/conduit-native-linear-etl.metadata.jsonl
```

Expected `validate` output:

```text
valid workflow `native-linear-etl`
nodes: 3
edges: 2
```

Expected `explain` highlights:

```text
workflow `native-linear-etl`
status: valid
nodes: 3
edges: 2
execution: native-registry
metadata: jsonl lifecycle, message, and queue-pressure records with tiered control-only policy
  - source.rows -> transform.rows capacity=2
  - transform.cleaned -> sink.cleaned capacity=2
```

Expected text `run` output:

```text
ran workflow `native-linear-etl`
nodes: 3
edges: 2
metadata: /tmp/conduit-native-linear-etl.metadata.jsonl
records: 24
```

Expected `run --json` summary fields:

```json
{
  "status": "completed",
  "error": null,
  "metadata": {
    "record_count": 24
  },
  "summary": {
    "terminal_state": "completed",
    "scheduled_node_count": 3,
    "completed_node_count": 3,
    "error_count": 0
  }
}
```

Metadata output:

- writes 24 JSONL records to the requested metadata path
- includes lifecycle, message-boundary, and queue-pressure records
- uses stable execution id `cli-run-1`

Surfaces exercised:

- canonical workflow JSON parsing
- workflow validation
- CLI `validate`, `inspect`, `explain`, `run`, and `run --json`
- native executor registry
- bounded graph ports and output validation
- metadata JSONL writer
- run summary JSON

## Engine Feedback Loop Example

File:

- `crates/conduit-engine/examples/feedback_loop.rs`

Command:

```bash
cargo run -p conduit-engine --example feedback_loop
```

Expected output:

```text
counter received seed
driver received ack
workflow feedback-loop completed with 2 scheduled nodes and 0 errors
```

Surfaces exercised:

- `WorkflowGraph::with_cycles_allowed`
- explicit `WorkflowRunPolicy::feedback_loops`
- `StaticNodeExecutorRegistry`
- bounded cyclic graph wiring
- async `PortsIn`/`PortsOut` send and receive
- `WorkflowRunSummary` success reporting

## WASM Mixed Pipeline Example

Files:

- `crates/conduit-wasm/examples/mixed_pipeline.rs`
- `crates/conduit-wasm/examples/README.md`
- `crates/conduit-wasm/fixtures/uppercase-guest/`

Command:

```bash
env -u RUSTFLAGS nix develop . --command cargo run -p conduit-wasm --example mixed_pipeline
```

Expected output:

```text
# no stdout on success
```

The process exits successfully after asserting that the native sink received
`HELLO FROM WASM`.

Important environment note:

- The example builds the uppercase guest fixture for `wasm32-wasip2` during the
  run.
- The ambient shell may fail with `can't find crate for core` if the
  `wasm32-wasip2` target is not installed.
- Use the Nix devshell command above so the Rust target and WASM tools are
  available.

Surfaces exercised:

- real `wasm32-wasip2` guest fixture build
- `WasmtimeBatchComponent`
- `BatchNodeExecutor<WasmtimeBatchComponent>`
- mixed native and WASM executors in one `StaticNodeExecutorRegistry`
- bounded native source -> WASM transform -> native sink graph
- host-owned output validation before WASM packets enter downstream edges

## CLI WASM Component Manifest Smoke Path

The CLI can load WASM component nodes from a manifest. This path is not yet a
checked-in standalone workflow example, but it is the product surface used by
`conduit run --wasm-components`.

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

Command shape:

```bash
cargo run -p conduit-cli -- run \
  --wasm-components wasm-components.json \
  workflow.json \
  /tmp/conduit.metadata.jsonl
```

Expected output shape:

```text
ran workflow `<workflow-id>`
nodes: <node-count>
edges: <edge-count>
metadata: /tmp/conduit.metadata.jsonl
records: <record-count>
```

Surfaces exercised:

- CLI WASM component manifest parsing
- component path resolution relative to manifest location
- per-component Wasmtime fuel limit selection
- mixed native/WASM executor registry construction
- CLI metadata JSONL and run summary surfaces

## Related Docs

- [workflow-run-guide.md](workflow-run-guide.md)
- [metadata-json.md](metadata-json.md)
- [../examples/authoring/README.md](../examples/authoring/README.md)
- [../examples/native-linear-etl.md](../examples/native-linear-etl.md)
- [../crates/conduit-wasm/examples/README.md](../crates/conduit-wasm/examples/README.md)
