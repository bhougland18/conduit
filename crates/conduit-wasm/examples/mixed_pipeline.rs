//! Native source -> WASM batch boundary -> native sink example.
//!
//! The middle node uses the same `BatchExecutor` shape implemented by
//! `WasmtimeBatchComponent`. This example keeps the batch executor in-process so
//! it is always buildable; swapping in a compiled component is limited to
//! constructing `WasmtimeBatchComponent` from guest bytes.

use std::sync::Mutex;

use conduit_core::{
    BatchExecutor, BatchInputs, BatchOutputs, NodeExecutor, PacketPayload, PortPacket, PortsIn,
    PortsOut, Result,
    context::{ExecutionMetadata, NodeContext},
    message::{MessageEndpoint, MessageMetadata, MessageRoute},
};
use conduit_engine::run_workflow;
use conduit_types::{ExecutionId, MessageId, NodeId, PortId, WorkflowId};
use conduit_workflow::{EdgeDefinition, EdgeEndpoint, NodeDefinition, WorkflowDefinition};
use futures::{executor::block_on, future::BoxFuture};

fn main() -> Result<()> {
    let workflow: WorkflowDefinition = workflow();
    let execution: ExecutionMetadata =
        ExecutionMetadata::first_attempt(execution_id("mixed-run-1"));
    let executor: MixedPipelineExecutor = MixedPipelineExecutor::default();

    block_on(run_workflow(&workflow, &execution, &executor))?;

    assert_eq!(
        executor.received_payloads(),
        vec![b"HELLO FROM WASM".to_vec()]
    );
    Ok(())
}

#[derive(Debug, Default)]
struct MixedPipelineExecutor {
    received_payloads: Mutex<Vec<Vec<u8>>>,
    wasm: UppercaseBatchExecutor,
}

impl MixedPipelineExecutor {
    fn received_payloads(&self) -> Vec<Vec<u8>> {
        self.received_payloads
            .lock()
            .expect("received payload lock should not be poisoned")
            .clone()
    }
}

impl NodeExecutor for MixedPipelineExecutor {
    type RunFuture<'a> = BoxFuture<'a, Result<()>>;

    fn run(&self, ctx: NodeContext, mut inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
        Box::pin(async move {
            let cancellation = ctx.cancellation_token();
            match ctx.node_id().as_str() {
                "native-source" => {
                    outputs
                        .send(
                            &port_id("out"),
                            packet(
                                b"hello from wasm".to_vec(),
                                "native-source",
                                "out",
                                "wasm-upper",
                                "in",
                            ),
                            &cancellation,
                        )
                        .await?;
                }
                "wasm-upper" => {
                    let packet: PortPacket = inputs
                        .recv(&port_id("in"), &cancellation)
                        .await?
                        .expect("source should send one packet");
                    let mut batch_inputs: BatchInputs = BatchInputs::new();
                    batch_inputs.push(port_id("in"), packet);

                    let batch_outputs: BatchOutputs = self.wasm.invoke(batch_inputs)?;
                    for packet in batch_outputs.packets(&port_id("out")) {
                        outputs
                            .send(&port_id("out"), packet.clone(), &cancellation)
                            .await?;
                    }
                }
                "native-sink" => {
                    let packet: PortPacket = inputs
                        .recv(&port_id("in"), &cancellation)
                        .await?
                        .expect("WASM node should send one packet");
                    self.received_payloads
                        .lock()
                        .expect("received payload lock should not be poisoned")
                        .push(
                            packet
                                .payload()
                                .as_bytes()
                                .expect("example sends byte payloads")
                                .to_vec(),
                        );
                }
                _ => {}
            }

            Ok(())
        })
    }
}

#[derive(Debug, Default)]
struct UppercaseBatchExecutor;

impl BatchExecutor for UppercaseBatchExecutor {
    fn invoke(&self, inputs: BatchInputs) -> Result<BatchOutputs> {
        let mut outputs: BatchOutputs = BatchOutputs::new();
        for packet in inputs.packets(&port_id("in")) {
            let bytes = packet
                .payload()
                .as_bytes()
                .expect("example sends byte payloads")
                .iter()
                .map(u8::to_ascii_uppercase)
                .collect::<Vec<u8>>();
            outputs.push(
                port_id("out"),
                PortPacket::new(packet.metadata().clone(), PacketPayload::from(bytes)),
            );
        }

        Ok(outputs)
    }
}

fn workflow() -> WorkflowDefinition {
    WorkflowDefinition::from_parts(
        workflow_id("mixed-flow"),
        [
            NodeDefinition::new(
                node_id("native-source"),
                Vec::<PortId>::new(),
                [port_id("out")],
            )
            .expect("valid source"),
            NodeDefinition::new(node_id("wasm-upper"), [port_id("in")], [port_id("out")])
                .expect("valid wasm node"),
            NodeDefinition::new(
                node_id("native-sink"),
                [port_id("in")],
                Vec::<PortId>::new(),
            )
            .expect("valid sink"),
        ],
        [
            EdgeDefinition::new(
                EdgeEndpoint::new(node_id("native-source"), port_id("out")),
                EdgeEndpoint::new(node_id("wasm-upper"), port_id("in")),
            ),
            EdgeDefinition::new(
                EdgeEndpoint::new(node_id("wasm-upper"), port_id("out")),
                EdgeEndpoint::new(node_id("native-sink"), port_id("in")),
            ),
        ],
    )
    .expect("valid mixed workflow")
}

fn packet(
    payload: Vec<u8>,
    source_node: &str,
    source_port: &str,
    target_node: &str,
    target_port: &str,
) -> PortPacket {
    let route: MessageRoute = MessageRoute::new(
        Some(MessageEndpoint::new(
            node_id(source_node),
            port_id(source_port),
        )),
        MessageEndpoint::new(node_id(target_node), port_id(target_port)),
    );
    let metadata: MessageMetadata = MessageMetadata::new(
        message_id("msg-1"),
        workflow_id("mixed-flow"),
        ExecutionMetadata::first_attempt(execution_id("mixed-run-1")),
        route,
    );

    PortPacket::new(metadata, PacketPayload::from(payload))
}

fn execution_id(value: &str) -> ExecutionId {
    ExecutionId::new(value).expect("valid execution id")
}

fn message_id(value: &str) -> MessageId {
    MessageId::new(value).expect("valid message id")
}

fn node_id(value: &str) -> NodeId {
    NodeId::new(value).expect("valid node id")
}

fn port_id(value: &str) -> PortId {
    PortId::new(value).expect("valid port id")
}

fn workflow_id(value: &str) -> WorkflowId {
    WorkflowId::new(value).expect("valid workflow id")
}
