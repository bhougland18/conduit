# conduit-wasm examples

## `mixed_pipeline`

`mixed_pipeline` proves the host-owned MVP shape:

- native source node sends a byte packet through normal `PortsOut`
- batch-backed middle node drains one input batch, invokes a `BatchExecutor`,
  and sends returned packets through normal `PortsOut`
- native sink node receives the transformed packet through normal `PortsIn`

The example intentionally uses an in-process `UppercaseBatchExecutor` rather
than a checked-in `.wasm` artifact. It exercises the same batch boundary that
`WasmtimeBatchComponent` implements while keeping the repo self-contained.
Once the project has a guest build fixture, the middle node should construct
`WasmtimeBatchComponent::from_component_bytes_with_capabilities` and use that
as the batch executor.

Run it with:

```sh
cargo run -p conduit-wasm --example mixed_pipeline
```
