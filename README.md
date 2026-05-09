# Conduit

Conduit is an experimental Flow-Based Programming workflow engine written in Rust.
The current repository state has a working vertical slice for validated workflow
documents, bounded graph execution, metadata JSONL, native executor registries,
and Wasmtime Component Model batch nodes.

## Current Capabilities

- Parse and validate canonical JSON workflow documents with typed diagnostics.
- Inspect workflow topology, contracts, capabilities, enforcement levels, and
  edge capacities as JSON.
- Explain runnable topology and metadata behavior from the CLI.
- Run workflows through a real executor registry backed by bounded async ports.
- Capture lifecycle, message-boundary, queue-pressure, structured error, and
  deadlock metadata as JSONL.
- Emit machine-facing `conduit run --json` summaries with stable status and
  error fields.
- Execute native nodes and manifest-loaded WASM component nodes in the same
  graph.
- Validate WASM outputs at the host graph boundary before packets enter
  downstream edges.
- Apply Wasmtime fuel limits and cancellation-aware interruption to guest
  invocation.

The remaining open work is primarily product documentation and release hygiene,
plus deferred data-tier experiments that are intentionally parked until concrete
workloads justify them.

## Repo Layout

- `crates/conduit-types` - validated identifier primitives
- `crates/conduit-workflow` - static workflow graph model and validation
- `crates/conduit-workflow-format` - versioned external workflow format parsing
- `crates/conduit-core` - runtime-facing traits, ports, metadata, capability, and error types
- `crates/conduit-contract` - node contract data and validation
- `crates/conduit-introspection` - pure workflow/contract/capability projections
- `crates/conduit-runtime` - `asupersync` runtime adapter and node observer boundary
- `crates/conduit-engine` - workflow orchestration, registry execution, backpressure, policies, and summaries
- `crates/conduit-wasm` - Wasmtime-backed Component Model batch adapter and WIT boundary
- `crates/conduit-cli` - validation, inspection, explanation, and run commands
- `crates/conduit-test-kit` - reusable builders, doubles, and test helpers
- `examples/` - runnable workflow examples
- `docs/` - proposal, epic planning, audit notes, and handoff material

## Build

The project is developed through the Nix devshell so the expected nightly Rust
toolchain and project wrappers are available.

```bash
nix develop . --command cargo check --workspace --all-targets
```

## Test

```bash
nix develop . --command cargo test --workspace
nix develop . --command cargo clippy --workspace --all-targets -- -W clippy::pedantic -W clippy::nursery -W clippy::perf -W clippy::redundant_clone
nix develop . --command cargo fmt --check
nix develop . --command cargo-dylint-nightly --all
```

Use `cargo-dylint-nightly` for the Dylint pass. The devshell now owns the
nightly toolchain and driver wiring for that command directly.

## Examples

Validate, inspect, and explain a workflow:

```bash
cargo run -p conduit-cli -- validate examples/native-linear-etl.workflow.json
cargo run -p conduit-cli -- inspect examples/native-linear-etl.workflow.json
cargo run -p conduit-cli -- explain examples/native-linear-etl.workflow.json
```

Run the native linear ETL topology and write metadata JSONL:

```bash
cargo run -p conduit-cli -- run examples/native-linear-etl.workflow.json /tmp/conduit-native-linear-etl.metadata.jsonl
```

Validate a WASM component manifest before running (catches unknown fields,
invalid node IDs, duplicate entries, and unreadable component paths):

```bash
cargo run -p conduit-cli -- validate-manifest wasm-components.json
```

Pass `--workflow` to also verify that every manifest node exists in the workflow:

```bash
cargo run -p conduit-cli -- validate-manifest --workflow workflow.json wasm-components.json
```

For the complete WASM smoke path (build → validate-manifest → run → inspect →
clean), see `examples/wasm-uppercase.md`.

To load WASM component nodes through the CLI, pass a component manifest to
`run`. Component paths are resolved relative to the manifest file:

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

```bash
cargo run -p conduit-cli -- run --wasm-components wasm-components.json workflow.json /tmp/conduit.metadata.jsonl
```

See `docs/workflow-run-guide.md` for command-by-command workflow execution
guidance, `docs/examples-catalog.md` for runnable examples and expected output,
`examples/native-linear-etl.md` for the native workflow walkthrough, and
`docs/metadata-json.md` for the stable metadata JSONL and `conduit run --json`
summary shapes.

## Help

Every command has built-in help:

```bash
cargo run -p conduit-cli -- --help
cargo run -p conduit-cli -- run --help
```

## Shell Completions

Generate completions for your shell and source them in your shell profile:

```bash
# Bash
cargo run -p conduit-cli -- completions bash >> ~/.bash_completion

# Zsh
cargo run -p conduit-cli -- completions zsh > ~/.zfunc/_conduit

# Fish
cargo run -p conduit-cli -- completions fish > ~/.config/fish/completions/conduit.fish
```

Supported shells: `bash`, `zsh`, `fish`, `powershell`, `elvish`.

## Key Docs

- `docs/archetecture/proposal_final.md` - current architecture proposal and roadmap
- `docs/epics/epic-1-foundation.md` - completed foundation bead plan
- `docs/audits/Audit_2026_05_07.md` - latest audit findings and follow-on ideas
- `docs/workflow-run-guide.md` - validate, inspect, explain, run, and summary guide
- `docs/examples-catalog.md` - runnable examples, expected outputs, and exercised surfaces
- `docs/benchmark-operations.md` - Criterion benchmark commands and comparison guide
- `docs/validation-matrix.md` - format, check, test, Clippy, Dylint, and bench gates
- `docs/release-readiness.md` - release candidate checklist and deferred work notes
- `docs/metadata-json.md` - metadata JSONL and CLI run summary JSON reference
- `examples/wasm-uppercase.md` - WASM node smoke path: build fixture, validate manifest, run, inspect metadata, clean
- `docs/contract-capability-authoring.md` - contract and capability mapping guidance for native and WASM nodes
- `docs/node-authoring-error-patterns.md` - native and WASM node error, cancellation, retry, and metadata patterns
- `crates/conduit-wasm/fixtures/uppercase-guest/README.md` - WASM guest authoring template: WIT contract, JSON shape, build, and step-by-step guide
- `docs/handoff_2026-05-07.md` - latest handoff snapshot
- `docs/AGENTS.md` - repo-local working conventions for coding agents
