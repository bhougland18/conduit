//! High-level workflow orchestration for Conduit.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use conduit_core::{
    CancellationHandle, InputPortHandle, NodeExecutor, OutputPortHandle, PortsIn, PortsOut, Result,
    bounded_edge_channel,
    context::{CancellationRequest, ExecutionMetadata, NodeContext},
};
use conduit_runtime::run_node;
use conduit_types::NodeId;
use conduit_workflow::WorkflowDefinition;
use futures::stream::{FuturesUnordered, StreamExt};

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
    let cancellation: CancellationHandle = CancellationHandle::new();

    let mut node_runs: FuturesUnordered<_> = FuturesUnordered::new();
    for node in workflow.nodes() {
        let ctx: NodeContext =
            NodeContext::new(workflow.id().clone(), node.id().clone(), execution.clone())
                .with_cancellation_token(cancellation.token());
        let inputs: PortsIn = PortsIn::from_handles(
            node.input_ports().to_vec(),
            inputs_by_node.remove(node.id()).unwrap_or_default(),
        );
        let outputs: PortsOut = PortsOut::from_handles(
            node.output_ports().to_vec(),
            outputs_by_node.remove(node.id()).unwrap_or_default(),
        );
        node_runs.push(run_node(executor, ctx, inputs, outputs));
    }

    let mut first_error: Option<conduit_core::ConduitError> = None;
    while let Some(result) = node_runs.next().await {
        if let Err(err) = result
            && first_error.is_none()
        {
            let _first_request: bool = cancellation.cancel(CancellationRequest::new(format!(
                "node execution failed: {err}"
            )));
            first_error = Some(err);
        }
    }

    first_error.map_or(Ok(()), Err)
}

type PortWiring = (
    BTreeMap<NodeId, Vec<InputPortHandle>>,
    BTreeMap<NodeId, Vec<OutputPortHandle>>,
);

fn build_port_wiring(workflow: &WorkflowDefinition) -> PortWiring {
    let mut inputs_by_node: BTreeMap<NodeId, Vec<InputPortHandle>> = BTreeMap::new();
    let mut outputs_by_node: BTreeMap<NodeId, Vec<OutputPortHandle>> = BTreeMap::new();

    for edge in workflow.edges() {
        let capacity: NonZeroUsize = edge.capacity().resolve(DEFAULT_EDGE_CAPACITY);
        let (output, input): (OutputPortHandle, InputPortHandle) = bounded_edge_channel(
            edge.source().port_id().clone(),
            edge.target().port_id().clone(),
            capacity,
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
        ConduitError, ErrorCode, PacketPayload, PortPacket, PortRecvError,
        message::{MessageEndpoint, MessageMetadata, MessageRoute},
    };
    use conduit_test_kit::{
        FailingExecutor, NodeBuilder, RecordingExecutor, WorkflowBuilder, execution_metadata,
        node_id, port_id, workflow_id,
    };
    use conduit_types::{ExecutionId, MessageId};
    use conduit_workflow::EdgeDefinition;
    use futures::executor::block_on;
    use futures::future::BoxFuture;

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
        type RunFuture<'a> = BoxFuture<'a, Result<()>>;

        fn run(
            &self,
            ctx: NodeContext,
            mut inputs: PortsIn,
            outputs: PortsOut,
        ) -> Self::RunFuture<'_> {
            Box::pin(async move {
                if ctx.node_id().as_str() == "source" {
                    let cancellation = ctx.cancellation_token();
                    outputs
                        .send(&port_id("out"), packet(b"hello"), &cancellation)
                        .await?;
                    outputs
                        .send(&port_id("out"), packet(b"world"), &cancellation)
                        .await?;
                } else if ctx.node_id().as_str() == "sink" {
                    let cancellation = ctx.cancellation_token();
                    for _packet_index in 0..2 {
                        let packet: PortPacket = inputs
                            .recv(&port_id("in"), &cancellation)
                            .await?
                            .expect("source should have queued a packet");
                        self.received
                            .lock()
                            .expect("channel executor lock should not be poisoned")
                            .push(
                                packet
                                    .into_payload()
                                    .as_bytes()
                                    .expect("channel test sends bytes")
                                    .to_vec(),
                            );
                    }
                }

                Ok(())
            })
        }
    }

    #[derive(Debug, Default)]
    struct AggregateFailureExecutor {
        visited: Mutex<Vec<String>>,
    }

    impl AggregateFailureExecutor {
        fn visited_node_names(&self) -> Vec<String> {
            self.visited
                .lock()
                .expect("aggregate failure executor lock should not be poisoned")
                .clone()
        }
    }

    impl NodeExecutor for AggregateFailureExecutor {
        type RunFuture<'a> = Ready<Result<()>>;

        fn run(
            &self,
            ctx: NodeContext,
            _inputs: PortsIn,
            _outputs: PortsOut,
        ) -> Self::RunFuture<'_> {
            self.visited
                .lock()
                .expect("aggregate failure executor lock should not be poisoned")
                .push(ctx.node_id().to_string());

            if ctx.node_id().as_str() == "first" {
                return ready(Err(ConduitError::execution("first failed")));
            }

            ready(Ok(()))
        }
    }

    #[derive(Debug, Default)]
    struct SiblingCancellationExecutor {
        cancellation_observed: Mutex<bool>,
    }

    impl SiblingCancellationExecutor {
        fn cancellation_observed(&self) -> bool {
            *self
                .cancellation_observed
                .lock()
                .expect("sibling cancellation executor lock should not be poisoned")
        }
    }

    #[derive(Debug, Default)]
    struct CapacityProbeExecutor {
        observed: Mutex<Vec<Option<usize>>>,
    }

    impl CapacityProbeExecutor {
        fn observed_capacities(&self) -> Vec<Option<usize>> {
            self.observed
                .lock()
                .expect("capacity probe executor lock should not be poisoned")
                .clone()
        }
    }

    impl NodeExecutor for CapacityProbeExecutor {
        type RunFuture<'a> = Ready<Result<()>>;

        fn run(
            &self,
            ctx: NodeContext,
            inputs: PortsIn,
            _outputs: PortsOut,
        ) -> Self::RunFuture<'_> {
            if ctx.node_id().as_str() == "probe" {
                let capacity = inputs.capacity(&port_id("in"));
                self.observed
                    .lock()
                    .expect("capacity probe executor lock should not be poisoned")
                    .push(capacity);
            }

            ready(Ok(()))
        }
    }

    impl NodeExecutor for SiblingCancellationExecutor {
        type RunFuture<'a> = BoxFuture<'a, Result<()>>;

        fn run(
            &self,
            ctx: NodeContext,
            mut inputs: PortsIn,
            _outputs: PortsOut,
        ) -> Self::RunFuture<'_> {
            Box::pin(async move {
                if ctx.node_id().as_str() == "fail" {
                    return Err(ConduitError::execution("fail requested"));
                }

                if ctx.node_id().as_str() == "worker" {
                    let cancellation = ctx.cancellation_token();
                    let result: std::result::Result<Option<PortPacket>, PortRecvError> =
                        inputs.recv(&port_id("in"), &cancellation).await;
                    if matches!(result, Err(PortRecvError::Cancelled { .. })) {
                        *self
                            .cancellation_observed
                            .lock()
                            .expect("sibling cancellation executor lock should not be poisoned") =
                            true;
                        return Ok(());
                    }

                    return Err(ConduitError::execution(
                        "worker input should be cancelled after sibling failure",
                    ));
                }

                Ok(())
            })
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

        PortPacket::new(metadata, PacketPayload::from(value.to_vec()))
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

        assert_eq!(
            executor.received_payloads(),
            vec![b"hello".to_vec(), b"world".to_vec()]
        );
    }

    #[test]
    fn run_workflow_uses_explicit_edge_capacity() {
        let workflow: WorkflowDefinition = WorkflowDefinition::from_parts(
            workflow_id("flow"),
            [
                NodeBuilder::new("source").output("out").build(),
                NodeBuilder::new("probe").input("in").build(),
            ],
            [EdgeDefinition::with_capacity(
                conduit_workflow::EdgeEndpoint::new(node_id("source"), port_id("out")),
                conduit_workflow::EdgeEndpoint::new(node_id("probe"), port_id("in")),
                NonZeroUsize::new(3).expect("nonzero"),
            )],
        )
        .expect("workflow should be valid");
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: CapacityProbeExecutor = CapacityProbeExecutor::default();

        block_on(run_workflow(&workflow, &execution, &executor)).expect("workflow should run");

        assert_eq!(
            executor.observed_capacities(),
            vec![Some(NonZeroUsize::new(3).expect("nonzero").get())]
        );
    }

    #[test]
    fn run_workflow_aggregates_terminal_results_after_polling_all_nodes() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("first").build())
            .node(NodeBuilder::new("second").build())
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: AggregateFailureExecutor = AggregateFailureExecutor::default();

        let err = block_on(run_workflow(&workflow, &execution, &executor))
            .expect_err("workflow should surface executor failures");

        assert_eq!(err.code(), ErrorCode::NodeExecutionFailed);
        assert_eq!(
            executor.visited_node_names(),
            vec![String::from("first"), String::from("second")]
        );
    }

    #[test]
    fn run_workflow_cancels_siblings_after_first_node_failure() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("worker").input("in").build())
            .node(NodeBuilder::new("fail").output("out").build())
            .edge("fail", "out", "worker", "in")
            .build();
        let execution: ExecutionMetadata = execution_metadata("run-1");
        let executor: SiblingCancellationExecutor = SiblingCancellationExecutor::default();

        let err = block_on(run_workflow(&workflow, &execution, &executor))
            .expect_err("workflow should surface the first node failure");

        assert_eq!(err.code(), ErrorCode::NodeExecutionFailed);
        assert!(executor.cancellation_observed());
    }
}
