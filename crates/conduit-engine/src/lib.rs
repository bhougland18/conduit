//! High-level workflow orchestration for Conduit.

use conduit_core::{NodeContext, NodeExecutor, Result};
use conduit_runtime::run_node;
use conduit_workflow::WorkflowDefinition;

/// Execute the scaffolded workflow by invoking the provided executor for each node.
///
/// # Errors
///
/// Returns an error if any node execution fails.
pub fn run_workflow(workflow: &WorkflowDefinition, executor: &dyn NodeExecutor) -> Result<()> {
    for node_id in &workflow.nodes {
        let ctx: NodeContext = NodeContext {
            workflow_id: workflow.id.clone(),
            node_id: node_id.clone(),
        };
        run_node(executor, &ctx)?;
    }

    Ok(())
}
