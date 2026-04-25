//! High-level workflow orchestration for Conduit.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use conduit_core::{
    InputPortHandle, NodeExecutor, OutputPortHandle, PortsIn, PortsOut, Result,
    bounded_edge_channel,
    context::{ExecutionMetadata, NodeContext},
};
use conduit_runtime::run_node;
use conduit_types::NodeId;
use conduit_workflow::WorkflowDefinition;

const DEFAULT_EDGE_CAPACITY: NonZeroUsize = NonZeroUsize::MIN;

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
    let (mut inputs_by_node, mut outputs_by_node): PortWiring = build_port_wiring(workflow);

    for node in workflow.nodes() {
        let ctx: NodeContext =
            NodeContext::new(workflow.id().clone(), node.id().clone(), execution.clone());
        let inputs: PortsIn = PortsIn::from_handles(
            node.input_ports().to_vec(),
            inputs_by_node.remove(node.id()).unwrap_or_default(),
        );
        let outputs: PortsOut = PortsOut::from_handles(
            node.output_ports().to_vec(),
            outputs_by_node.remove(node.id()).unwrap_or_default(),
        );
        run_node(executor, ctx, inputs, outputs).await?;
    }

    Ok(())
}

type PortWiring = (
    BTreeMap<NodeId, Vec<InputPortHandle>>,
    BTreeMap<NodeId, Vec<OutputPortHandle>>,
);

fn build_port_wiring(workflow: &WorkflowDefinition) -> PortWiring {
    let mut inputs_by_node: BTreeMap<NodeId, Vec<InputPortHandle>> = BTreeMap::new();
    let mut outputs_by_node: BTreeMap<NodeId, Vec<OutputPortHandle>> = BTreeMap::new();

    for edge in workflow.edges() {
        let (output, input): (OutputPortHandle, InputPortHandle) = bounded_edge_channel(
            edge.source().port_id().clone(),
            edge.target().port_id().clone(),
            DEFAULT_EDGE_CAPACITY,
        );
        outputs_by_node
            .entry(edge.source().node_id().clone())
            .or_default()
            .push(output);
        inputs_by_node
            .entry(edge.target().node_id().clone())
            .or_default()
            .push(input);
    }

    (inputs_by_node, outputs_by_node)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::{Ready, ready},
        sync::Mutex,
    };

    use conduit_core::{
        ErrorCode, PortPacket,
        message::{MessageEndpoint, MessageMetadata, MessageRoute},
    };
    use conduit_test_kit::{
        FailingExecutor, NodeBuilder, RecordingExecutor, WorkflowBuilder, execution_metadata,
        node_id, port_id, workflow_id,
    };
    use conduit_types::{ExecutionId, MessageId};
    use futures::executor::block_on;

    #[derive(Debug, Default)]
    struct ChannelExecutor {
        received: Mutex<Vec<Vec<u8>>>,
    }

    impl ChannelExecutor {
        fn received_payloads(&self) -> Vec<Vec<u8>> {
            self.received
                .lock()
                .expect("channel executor lock should not be poisoned")
                .clone()
        }
    }

    impl NodeExecutor for ChannelExecutor {
        type RunFuture<'a> = Ready<Result<()>>;

        fn run(
            &self,
            ctx: NodeContext,
            mut inputs: PortsIn,
            outputs: PortsOut,
        ) -> Self::RunFuture<'_> {
            if ctx.node_id().as_str() == "source" {
                outputs
                    .try_send(&port_id("out"), packet(b"hello"))
                    .expect("source output should accept packet");
            } else if ctx.node_id().as_str() == "sink" {
                let packet: PortPacket = inputs
                    .try_recv(&port_id("in"))
                    .expect("sink input should receive")
                    .expect("source should have queued a packet");
                self.received
                    .lock()
                    .expect("channel executor lock should not be poisoned")
                    .push(packet.into_payload());
            }

            ready(Ok(()))
        }
    }

    fn execution_id(value: &str) -> ExecutionId {
        ExecutionId::new(value).expect("valid execution id")
    }

    fn message_id(value: &str) -> MessageId {
        MessageId::new(value).expect("valid message id")
    }

    fn packet(value: &[u8]) -> PortPacket {
        let source: MessageEndpoint = MessageEndpoint::new(node_id("source"), port_id("out"));
        let target: MessageEndpoint = MessageEndpoint::new(node_id("sink"), port_id("in"));
        let route: MessageRoute = MessageRoute::new(Some(source), target);
        let execution: ExecutionMetadata = ExecutionMetadata::first_attempt(execution_id("run-1"));
        let metadata: MessageMetadata =
            MessageMetadata::new(message_id("msg-1"), workflow_id("flow"), execution, route);

        PortPacket::new(metadata, value.to_vec())
    }

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

    #[test]
    fn run_workflow_wires_edges_as_bounded_port_channels() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("source").output("out").build())
            .node(NodeBuilder::new("sink").input("in").build())
            .edge("source", "out", "sink", "in")
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: ChannelExecutor = ChannelExecutor::default();

        block_on(run_workflow(&workflow, &execution, &executor)).expect("workflow should run");

        assert_eq!(executor.received_payloads(), vec![b"hello".to_vec()]);
    }
}
