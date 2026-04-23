//! High-level workflow orchestration for Conduit.

use conduit_core::{
    NodeExecutor, Result,
    context::{ExecutionMetadata, NodeContext},
};
use conduit_runtime::run_node;
use conduit_workflow::WorkflowDefinition;

/// Execute the scaffolded workflow by invoking the provided executor for each node.
///
/// # Errors
///
/// Returns an error if any node execution fails.
pub fn run_workflow(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    executor: &dyn NodeExecutor,
) -> Result<()> {
    for node in workflow.nodes() {
        let ctx: NodeContext =
            NodeContext::new(workflow.id().clone(), node.id().clone(), execution.clone());
        run_node(executor, &ctx)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use conduit_types::{ExecutionId, NodeId, WorkflowId};
    use conduit_workflow::{NodeDefinition, WorkflowDefinition};

    #[derive(Default)]
    struct RecordingExecutor {
        nodes: std::cell::RefCell<Vec<NodeId>>,
    }

    impl NodeExecutor for RecordingExecutor {
        fn run(&self, ctx: &NodeContext) -> Result<()> {
            assert_eq!(ctx.workflow_id().as_str(), "flow");
            assert_eq!(ctx.execution().execution_id().as_str(), "run-1");
            self.nodes.borrow_mut().push(ctx.node_id().clone());
            Ok(())
        }
    }

    #[test]
    fn run_workflow_passes_execution_metadata_to_each_node() {
        let workflow_id: WorkflowId = WorkflowId::new("flow").expect("valid workflow id");
        let first: NodeDefinition =
            NodeDefinition::new(NodeId::new("first").expect("valid node id"), [], [])
                .expect("valid node");
        let second: NodeDefinition =
            NodeDefinition::new(NodeId::new("second").expect("valid node id"), [], [])
                .expect("valid node");
        let workflow: WorkflowDefinition =
            WorkflowDefinition::from_parts(workflow_id, [first, second], [])
                .expect("valid workflow");
        let execution: ExecutionMetadata =
            ExecutionMetadata::first_attempt(ExecutionId::new("run-1").expect("valid run id"));
        let executor: RecordingExecutor = RecordingExecutor::default();

        run_workflow(&workflow, &execution, &executor).expect("workflow should run");

        let nodes: Vec<NodeId> = executor.nodes.into_inner();
        assert_eq!(nodes[0].as_str(), "first");
        assert_eq!(nodes[1].as_str(), "second");
    }
}
