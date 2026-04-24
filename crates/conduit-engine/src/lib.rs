//! High-level workflow orchestration for Conduit.

use conduit_core::{
    NodeExecutor, PortsIn, PortsOut, Result,
    context::{ExecutionMetadata, NodeContext},
};
use conduit_runtime::run_node;
use conduit_workflow::WorkflowDefinition;

/// Execute the scaffolded workflow by invoking the provided executor for each node.
///
/// # Errors
///
/// Returns an error if any node execution fails.
pub async fn run_workflow<E: NodeExecutor + ?Sized>(
    workflow: &WorkflowDefinition,
    execution: &ExecutionMetadata,
    executor: &E,
) -> Result<()> {
    for node in workflow.nodes() {
        let ctx: NodeContext =
            NodeContext::new(workflow.id().clone(), node.id().clone(), execution.clone());
        let inputs: PortsIn = PortsIn::new(node.input_ports().to_vec());
        let outputs: PortsOut = PortsOut::new(node.output_ports().to_vec());
        run_node(executor, ctx, inputs, outputs).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use conduit_core::ErrorCode;
    use conduit_test_kit::{
        FailingExecutor, NodeBuilder, RecordingExecutor, WorkflowBuilder, execution_metadata,
    };
    use futures::executor::block_on;

    #[test]
    fn run_workflow_passes_execution_metadata_to_each_node() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("first").build())
            .node(NodeBuilder::new("second").build())
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: RecordingExecutor = RecordingExecutor::default();

        block_on(run_workflow(&workflow, &execution, &executor)).expect("workflow should run");

        let contexts: Vec<NodeContext> = executor.visited_contexts();
        assert_eq!(contexts[0].workflow_id().as_str(), "flow");
        assert_eq!(contexts[0].execution().execution_id().as_str(), "run-1");
        assert_eq!(executor.visited_node_names(), vec!["first", "second"]);
    }

    #[test]
    fn run_workflow_propagates_executor_failures() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("first").build())
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: FailingExecutor = FailingExecutor::execution("boom");

        let err = block_on(run_workflow(&workflow, &execution, &executor))
            .expect_err("workflow should surface executor failures");

        assert_eq!(err.code(), ErrorCode::NodeExecutionFailed);
    }

    #[test]
    fn run_workflow_passes_declared_node_ports_to_executor() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .node(NodeBuilder::new("sink").input("in").build())
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: RecordingExecutor = RecordingExecutor::default();

        block_on(run_workflow(&workflow, &execution, &executor)).expect("workflow should run");

        assert_eq!(
            executor.visited_input_port_names(),
            vec![Vec::<String>::new(), vec![String::from("in")]]
        );
        assert_eq!(
            executor.visited_output_port_names(),
            vec![vec![String::from("out")], Vec::<String>::new()]
        );
    }
}
