# Conduit

Conduit is an experimental Flow-Based Programming workflow engine written in Rust.
The current repository state focuses on the foundational execution model:
validated workflow structure, runtime boundary types, capability descriptors,
error taxonomy, and shared test helpers.

## Repo Layout

- `crates/conduit-types` - validated identifier primitives
- `crates/conduit-workflow` - static workflow graph model and validation
- `crates/conduit-core` - runtime-facing contracts, context, lifecycle, capability, and error types
- `crates/conduit-runtime` - runtime boundary entry points
- `crates/conduit-engine` - scaffolded workflow orchestration
- `crates/conduit-cli` - temporary CLI entrypoint
- `crates/conduit-test-kit` - reusable builders, doubles, and test helpers
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
nix develop . --command cargo clippy --workspace --all-targets -- -W clippy::pedantic -W clippy::nursery
nix develop . --command cargo fmt --check
nix develop . --command cargo-dylint-nightly --all
```

Use `cargo-dylint-nightly` for the Dylint pass. The devshell now owns the
nightly toolchain and driver wiring for that command directly.

## Key Docs

- `docs/conduit_proposal.md` - architecture and requirements proposal
- `docs/epics/epic-1-foundation.md` - completed foundation bead plan
- `docs/audits/Audit_4_23.md` - latest audit findings and follow-on ideas
- `docs/handoff_2026-04-26.md` - latest handoff snapshot
- `docs/AGENTS.md` - repo-local working conventions for coding agents
