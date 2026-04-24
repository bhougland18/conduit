//! Runtime mechanics such as supervision and backpressure primitives.

use conduit_core::{NodeExecutor, PortsIn, PortsOut, Result, context::NodeContext};

/// Execute a single node through the runtime boundary.
///
/// # Errors
///
/// Returns an error if the node executor reports one.
pub async fn run_node<E: NodeExecutor + ?Sized>(
    node: &E,
    ctx: NodeContext,
    inputs: PortsIn,
    outputs: PortsOut,
) -> Result<()> {
    node.run(ctx, inputs, outputs).await
}
