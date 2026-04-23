# Conduit Handoff

## Agent Instructions

Read the local ACFS/Beads policy copy before continuing:

- [acfs-agents/beads_rust/AGENTS.md](../acfs-agents/beads_rust/AGENTS.md)

Important local conventions from that file:

- Use one JJ change per bead when practical.
- Put the Beads ID in the JJ change description.
- Do not delete files without explicit human permission.
- Use Verso selectively for foundational design rationale.
- A bead is complete only after tests, relevant property tests, strict Clippy, and Dylint pass.

## Current State

`/home/ben/code/conduit` is now its own colocated JJ/Git repository. `jj root` should report:

```text
/home/ben/code/conduit
```

The previous parent JJ workspace at `/home/ben/code/.jj` should no longer be used for Conduit work.

Beads has been initialized with prefix `cdt`.

Epic:

- `cdt-dmh` - Epic 1: Conduit Foundation

Closed beads:

- `cdt-dmh.1` - Workspace skeleton
- `cdt-dmh.2` - Identity primitives

Next ready bead:

- `cdt-dmh.3` - Workflow model

## Restart Point

Start by making the current bootstrap state into the initial Conduit JJ change, then create a new JJ change for `cdt-dmh.3`.

Suggested next commands to inspect state:

```bash
jj status
nix develop . --command br ready --json
```

Do not begin implementation until the JJ change description includes the active bead ID.
