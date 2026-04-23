//! Runtime mechanics such as supervision and backpressure primitives.

use conduit_core::{NodeExecutor, Result, context::NodeContext};

/// Execute a single node through the runtime boundary.
///
/// # Errors
///
/// Returns an error if the node executor reports one.
pub fn run_node(node: &dyn NodeExecutor, ctx: &NodeContext) -> Result<()> {
    node.run(ctx)
}
