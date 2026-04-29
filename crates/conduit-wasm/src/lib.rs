//! Wasmtime-backed batch adapter boundary for Conduit.
//!
//! The crate owns the Component Model/WIT ABI and keeps Wasmtime types out of
//! `conduit-core`. Guest components implement `conduit:batch/conduit-node`
//! from `wit/conduit-batch.wit`; the host remains responsible for output port
//! validation before packets are sent through `PortsOut`.

use std::num::NonZeroU32;

use conduit_core::{
    BatchExecutor, BatchInputs, BatchOutputs, ConduitError, PacketPayload, PortPacket, Result,
    capability::{CapabilityValidationError, NodeCapabilities},
    context::{ExecutionAttempt, ExecutionMetadata},
    message::{MessageEndpoint, MessageMetadata, MessageRoute},
};
use conduit_types::{ExecutionId, MessageId, NodeId, PortId, WorkflowId};
use serde_json::Value;
use wasmtime::{
    Config, Engine, Store,
    component::{Component, ComponentExportIndex, Func, Instance, Linker, Val},
};

/// WIT package identifier implemented by Conduit WASM batch guests.
pub const WIT_PACKAGE: &str = "conduit:batch@0.1.0";

/// WIT world exported by Conduit WASM batch guests.
pub const WIT_WORLD: &str = "conduit-node";

/// Wasmtime component prepared for Conduit batch execution.
pub struct WasmtimeBatchComponent {
    engine: Engine,
    component: Component,
}

impl WasmtimeBatchComponent {
    /// Compile a guest component from bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if Wasmtime cannot configure the engine or compile the
    /// supplied component bytes.
    pub fn from_component_bytes(bytes: impl AsRef<[u8]>) -> Result<Self> {
        let engine: Engine = component_engine()?;
        let component: Component =
            Component::from_binary(&engine, bytes.as_ref()).map_err(|err: wasmtime::Error| {
                ConduitError::execution(format!("failed to compile component: {err}"))
            })?;

        Ok(Self { engine, component })
    }

    /// Compile a guest component after validating the WASM capability boundary.
    ///
    /// # Errors
    ///
    /// Returns an error if the capability descriptor declares effects that the
    /// current import-free WASM world cannot enforce, or if component
    /// compilation fails.
    pub fn from_component_bytes_with_capabilities(
        bytes: impl AsRef<[u8]>,
        capabilities: &NodeCapabilities,
    ) -> Result<Self> {
        validate_wasm_capabilities(capabilities)?;
        Self::from_component_bytes(bytes)
    }

    /// Instantiate and invoke the guest component with one batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the component cannot instantiate, the guest traps,
    /// or the guest returns malformed Conduit data.
    pub fn invoke(&self, inputs: &BatchInputs) -> Result<BatchOutputs> {
        let linker: Linker<()> = Linker::new(&self.engine);
        let mut store: Store<()> = Store::new(&self.engine, ());
        let instance: Instance =
            linker
                .instantiate(&mut store, &self.component)
                .map_err(|err: wasmtime::Error| {
                    ConduitError::execution(format!("failed to instantiate component: {err}"))
                })?;
        let batch_index: ComponentExportIndex = instance
            .get_export_index(&mut store, None, "conduit:batch/batch@0.1.0")
            .ok_or_else(|| {
                ConduitError::execution("component does not export conduit:batch/batch@0.1.0")
            })?;
        let invoke_index: ComponentExportIndex = instance
            .get_export_index(&mut store, Some(&batch_index), "invoke")
            .ok_or_else(|| ConduitError::execution("component does not export batch.invoke"))?;
        let invoke: Func = instance
            .get_func(&mut store, invoke_index)
            .ok_or_else(|| ConduitError::execution("batch.invoke export is not a function"))?;

        let params: [Val; 1] = [batch_inputs_to_val(inputs)?];
        let mut results: [Val; 1] = [Val::Bool(false)];
        invoke
            .call(&mut store, &params, &mut results)
            .map_err(|err: wasmtime::Error| {
                ConduitError::execution(format!("guest invoke failed: {err}"))
            })?;

        let [result]: [Val; 1] = results;
        batch_outputs_from_result_val(result)
    }
}

impl BatchExecutor for WasmtimeBatchComponent {
    fn invoke(&self, inputs: BatchInputs) -> Result<BatchOutputs> {
        Self::invoke(self, &inputs)
    }
}

/// Validate a capability descriptor for the current import-free WASM world.
///
/// # Errors
///
/// Returns an error if the descriptor declares any external effect capability.
pub fn validate_wasm_capabilities(capabilities: &NodeCapabilities) -> Result<()> {
    if let Some(effect) = capabilities.effects().first() {
        return Err(CapabilityValidationError::UnenforceableEffectCapability {
            node_id: capabilities.node_id().clone(),
            effect: *effect,
        }
        .into());
    }

    Ok(())
}

/// Convert Conduit batch inputs to the WIT-facing ordered port batch shape.
///
/// # Errors
///
/// Returns an error if a payload cannot be represented by WIT ABI `0.1.0`.
pub fn to_wit_port_batches(inputs: &BatchInputs) -> Result<Vec<WitPortBatch>> {
    inputs
        .packets_by_port()
        .iter()
        .map(|(port_id, packets): (&PortId, &Vec<PortPacket>)| {
            Ok(WitPortBatch {
                port_id: port_id.to_string(),
                packets: packets
                    .iter()
                    .map(to_wit_packet)
                    .collect::<Result<Vec<_>>>()?,
            })
        })
        .collect()
}

/// Convert WIT-facing ordered port batches back to Conduit batch outputs.
///
/// # Errors
///
/// Returns an error if a port identifier or packet metadata identifier fails
/// Conduit validation, or if a control payload is not valid JSON.
pub fn from_wit_port_batches(port_batches: Vec<WitPortBatch>) -> Result<BatchOutputs> {
    let mut outputs: BatchOutputs = BatchOutputs::new();
    for port_batch in port_batches {
        let port_id: PortId = PortId::new(port_batch.port_id)?;
        for packet in port_batch.packets {
            outputs.push(port_id.clone(), from_wit_packet(packet)?);
        }
    }

    Ok(outputs)
}

/// WIT-facing port batch representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WitPortBatch {
    /// Port identifier.
    pub port_id: String,
    /// Packets for the port, preserving batch order.
    pub packets: Vec<WitPacket>,
}

/// WIT-facing packet representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WitPacket {
    /// Message metadata.
    pub metadata: conduit_core::message::MessageMetadata,
    /// Packet payload.
    pub payload: WitPayload,
}

/// WIT-facing packet payload representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WitPayload {
    /// Byte payload.
    Bytes(Vec<u8>),
    /// JSON-encoded control payload.
    Control(String),
}

fn component_engine() -> Result<Engine> {
    let mut config: Config = Config::new();
    config.wasm_component_model(true);
    config.epoch_interruption(true);
    Engine::new(&config).map_err(|err: wasmtime::Error| {
        ConduitError::execution(format!("failed to create Wasmtime engine: {err}"))
    })
}

fn batch_inputs_to_val(inputs: &BatchInputs) -> Result<Val> {
    Ok(Val::List(
        to_wit_port_batches(inputs)?
            .into_iter()
            .map(port_batch_to_val)
            .collect(),
    ))
}

fn port_batch_to_val(port_batch: WitPortBatch) -> Val {
    Val::Record(vec![
        ("port-id".to_owned(), Val::String(port_batch.port_id)),
        (
            "packets".to_owned(),
            Val::List(port_batch.packets.into_iter().map(packet_to_val).collect()),
        ),
    ])
}

fn packet_to_val(packet: WitPacket) -> Val {
    Val::Record(vec![
        ("metadata".to_owned(), metadata_to_val(&packet.metadata)),
        ("payload".to_owned(), payload_to_val(packet.payload)),
    ])
}

fn metadata_to_val(metadata: &MessageMetadata) -> Val {
    Val::Record(vec![
        (
            "message-id".to_owned(),
            Val::String(metadata.message_id().to_string()),
        ),
        (
            "workflow-id".to_owned(),
            Val::String(metadata.workflow_id().to_string()),
        ),
        (
            "execution".to_owned(),
            Val::Record(vec![
                (
                    "execution-id".to_owned(),
                    Val::String(metadata.execution().execution_id().to_string()),
                ),
                (
                    "attempt".to_owned(),
                    Val::U32(metadata.execution().attempt().get()),
                ),
            ]),
        ),
        ("route".to_owned(), route_to_val(metadata.route())),
    ])
}

fn route_to_val(route: &MessageRoute) -> Val {
    Val::Record(vec![
        (
            "source".to_owned(),
            Val::Option(
                route
                    .source()
                    .map(|source: &MessageEndpoint| Box::new(endpoint_to_val(source))),
            ),
        ),
        ("target".to_owned(), endpoint_to_val(route.target())),
    ])
}

fn endpoint_to_val(endpoint: &MessageEndpoint) -> Val {
    Val::Record(vec![
        (
            "node-id".to_owned(),
            Val::String(endpoint.node_id().to_string()),
        ),
        (
            "port-id".to_owned(),
            Val::String(endpoint.port_id().to_string()),
        ),
    ])
}

fn payload_to_val(payload: WitPayload) -> Val {
    match payload {
        WitPayload::Bytes(bytes) => {
            Val::Variant("bytes".to_owned(), Some(Box::new(bytes_to_list_val(bytes))))
        }
        WitPayload::Control(value) => {
            Val::Variant("control".to_owned(), Some(Box::new(Val::String(value))))
        }
    }
}

fn bytes_to_list_val(bytes: Vec<u8>) -> Val {
    Val::List(bytes.into_iter().map(Val::U8).collect())
}

fn batch_outputs_from_result_val(value: Val) -> Result<BatchOutputs> {
    let result: std::result::Result<Option<Box<Val>>, Option<Box<Val>>> = match value {
        Val::Result(result) => result,
        _ => {
            return Err(ConduitError::execution(
                "guest returned non-result from batch.invoke",
            ));
        }
    };

    match result {
        Ok(Some(value)) => port_batches_from_val(*value).and_then(from_wit_port_batches),
        Ok(None) => Err(ConduitError::execution(
            "guest returned empty ok result from batch.invoke",
        )),
        Err(Some(value)) => Err(batch_error_from_val(*value)),
        Err(None) => Err(ConduitError::execution(
            "guest returned empty error from batch.invoke",
        )),
    }
}

fn batch_error_from_val(value: Val) -> ConduitError {
    match value {
        Val::Variant(name, Some(detail)) => match *detail {
            Val::String(message) => {
                ConduitError::execution(format!("guest returned {name}: {message}"))
            }
            _ => ConduitError::execution(format!("guest returned malformed {name} error")),
        },
        Val::Variant(name, None) => {
            ConduitError::execution(format!("guest returned {name} without detail"))
        }
        _ => ConduitError::execution("guest returned malformed batch error"),
    }
}

fn port_batches_from_val(value: Val) -> Result<Vec<WitPortBatch>> {
    let values: Vec<Val> = match value {
        Val::List(values) => values,
        _ => {
            return Err(ConduitError::execution(
                "guest returned non-list batch output",
            ));
        }
    };

    values.into_iter().map(port_batch_from_val).collect()
}

fn port_batch_from_val(value: Val) -> Result<WitPortBatch> {
    let fields: Vec<(String, Val)> = record_fields(value, "port batch")?;
    let port_id: String = required_string_field(&fields, "port-id", "port batch")?;
    let packets: Vec<WitPacket> = required_list_field(&fields, "packets", "port batch")?
        .into_iter()
        .map(packet_from_val)
        .collect::<Result<Vec<_>>>()?;

    Ok(WitPortBatch { port_id, packets })
}

fn packet_from_val(value: Val) -> Result<WitPacket> {
    let fields: Vec<(String, Val)> = record_fields(value, "packet")?;
    let metadata: MessageMetadata =
        metadata_from_val(required_field(&fields, "metadata", "packet")?.clone())?;
    let payload: WitPayload =
        payload_from_val(required_field(&fields, "payload", "packet")?.clone())?;

    Ok(WitPacket { metadata, payload })
}

fn metadata_from_val(value: Val) -> Result<MessageMetadata> {
    let fields: Vec<(String, Val)> = record_fields(value, "message metadata")?;
    let message_id: MessageId = MessageId::new(required_string_field(
        &fields,
        "message-id",
        "message metadata",
    )?)?;
    let workflow_id: WorkflowId = WorkflowId::new(required_string_field(
        &fields,
        "workflow-id",
        "message metadata",
    )?)?;
    let execution: ExecutionMetadata =
        execution_from_val(required_field(&fields, "execution", "message metadata")?.clone())?;
    let route: MessageRoute =
        route_from_val(required_field(&fields, "route", "message metadata")?.clone())?;

    Ok(MessageMetadata::new(
        message_id,
        workflow_id,
        execution,
        route,
    ))
}

fn execution_from_val(value: Val) -> Result<ExecutionMetadata> {
    let fields: Vec<(String, Val)> = record_fields(value, "execution metadata")?;
    let execution_id: ExecutionId = ExecutionId::new(required_string_field(
        &fields,
        "execution-id",
        "execution metadata",
    )?)?;
    let attempt: u32 = required_u32_field(&fields, "attempt", "execution metadata")?;
    let attempt: ExecutionAttempt = NonZeroU32::new(attempt)
        .map(ExecutionAttempt::new)
        .ok_or_else(|| ConduitError::execution("guest returned zero execution attempt"))?;

    Ok(ExecutionMetadata::new(execution_id, attempt))
}

fn route_from_val(value: Val) -> Result<MessageRoute> {
    let fields: Vec<(String, Val)> = record_fields(value, "message route")?;
    let source: Option<MessageEndpoint> = match required_field(&fields, "source", "message route")?
    {
        Val::Option(Some(source)) => Some(endpoint_from_val(source.as_ref().clone())?),
        Val::Option(None) => None,
        _ => {
            return Err(ConduitError::execution(
                "guest returned non-option route source",
            ));
        }
    };
    let target: MessageEndpoint =
        endpoint_from_val(required_field(&fields, "target", "message route")?.clone())?;

    Ok(MessageRoute::new(source, target))
}

fn endpoint_from_val(value: Val) -> Result<MessageEndpoint> {
    let fields: Vec<(String, Val)> = record_fields(value, "message endpoint")?;
    let node_id: NodeId = NodeId::new(required_string_field(
        &fields,
        "node-id",
        "message endpoint",
    )?)?;
    let port_id: PortId = PortId::new(required_string_field(
        &fields,
        "port-id",
        "message endpoint",
    )?)?;

    Ok(MessageEndpoint::new(node_id, port_id))
}

fn payload_from_val(value: Val) -> Result<WitPayload> {
    let (name, payload): (String, Option<Box<Val>>) = match value {
        Val::Variant(name, payload) => (name, payload),
        _ => {
            return Err(ConduitError::execution(
                "guest returned non-variant payload",
            ));
        }
    };
    match (name.as_str(), payload) {
        ("bytes", Some(value)) => Ok(WitPayload::Bytes(bytes_from_val(*value)?)),
        ("control", Some(value)) => {
            let value: String = match *value {
                Val::String(value) => value,
                _ => {
                    return Err(ConduitError::execution(
                        "guest returned non-string control payload",
                    ));
                }
            };
            Ok(WitPayload::Control(value))
        }
        (kind, _) => Err(ConduitError::execution(format!(
            "guest returned unsupported payload variant: {kind}"
        ))),
    }
}

fn bytes_from_val(value: Val) -> Result<Vec<u8>> {
    let values: Vec<Val> = match value {
        Val::List(values) => values,
        _ => {
            return Err(ConduitError::execution(
                "guest returned non-list bytes payload",
            ));
        }
    };
    values
        .into_iter()
        .map(|value: Val| match value {
            Val::U8(byte) => Ok(byte),
            _ => Err(ConduitError::execution(
                "guest returned non-u8 byte payload element",
            )),
        })
        .collect()
}

fn record_fields(value: Val, context: &str) -> Result<Vec<(String, Val)>> {
    let fields: Vec<(String, Val)> = match value {
        Val::Record(fields) => fields,
        _ => {
            return Err(ConduitError::execution(format!(
                "guest returned non-record {context}"
            )));
        }
    };
    Ok(fields)
}

fn required_field<'a>(fields: &'a [(String, Val)], name: &str, context: &str) -> Result<&'a Val> {
    fields
        .iter()
        .find_map(|(field_name, value): &(String, Val)| (field_name == name).then_some(value))
        .ok_or_else(|| ConduitError::execution(format!("guest omitted {context} field {name}")))
}

fn required_string_field(fields: &[(String, Val)], name: &str, context: &str) -> Result<String> {
    match required_field(fields, name, context)? {
        Val::String(value) => Ok(value.clone()),
        _ => Err(ConduitError::execution(format!(
            "guest returned non-string {context} field {name}"
        ))),
    }
}

fn required_u32_field(fields: &[(String, Val)], name: &str, context: &str) -> Result<u32> {
    match required_field(fields, name, context)? {
        Val::U32(value) => Ok(*value),
        _ => Err(ConduitError::execution(format!(
            "guest returned non-u32 {context} field {name}"
        ))),
    }
}

fn required_list_field(fields: &[(String, Val)], name: &str, context: &str) -> Result<Vec<Val>> {
    match required_field(fields, name, context)? {
        Val::List(values) => Ok(values.clone()),
        _ => Err(ConduitError::execution(format!(
            "guest returned non-list {context} field {name}"
        ))),
    }
}

#[allow(clippy::match_wildcard_for_single_variants)]
fn to_wit_packet(packet: &PortPacket) -> Result<WitPacket> {
    let payload: WitPayload = match packet.payload() {
        PacketPayload::Bytes(bytes) => WitPayload::Bytes(bytes.to_vec()),
        PacketPayload::Control(value) => WitPayload::Control(value.to_string()),
        #[allow(unreachable_patterns)]
        _ => {
            return Err(ConduitError::execution(
                "payload is not supported by WIT ABI 0.1.0",
            ));
        }
    };

    Ok(WitPacket {
        metadata: packet.metadata().clone(),
        payload,
    })
}

fn from_wit_packet(packet: WitPacket) -> Result<PortPacket> {
    let payload: PacketPayload = match packet.payload {
        WitPayload::Bytes(bytes) => PacketPayload::from(bytes),
        WitPayload::Control(value) => {
            let value: Value = serde_json::from_str(&value).map_err(|err: serde_json::Error| {
                ConduitError::execution(format!("guest returned invalid control payload: {err}"))
            })?;
            PacketPayload::from(value)
        }
    };

    Ok(PortPacket::new(packet.metadata, payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use conduit_core::{
        capability::{EffectCapability, PortCapability, PortCapabilityDirection},
        context::ExecutionMetadata,
        message::{MessageEndpoint, MessageMetadata, MessageRoute},
    };
    use conduit_types::{ExecutionId, MessageId, NodeId, WorkflowId};
    use serde_json::json;

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

    fn metadata() -> MessageMetadata {
        let source: MessageEndpoint = MessageEndpoint::new(node_id("source"), port_id("out"));
        let target: MessageEndpoint = MessageEndpoint::new(node_id("wasm"), port_id("in"));
        let route: MessageRoute = MessageRoute::new(Some(source), target);
        let execution: ExecutionMetadata = ExecutionMetadata::first_attempt(execution_id("run-1"));
        MessageMetadata::new(message_id("msg-1"), workflow_id("flow"), execution, route)
    }

    #[test]
    fn constants_name_the_wit_abi() {
        assert_eq!(WIT_PACKAGE, "conduit:batch@0.1.0");
        assert_eq!(WIT_WORLD, "conduit-node");
    }

    #[test]
    fn wasm_capabilities_accept_import_free_descriptor() {
        let capabilities: NodeCapabilities = NodeCapabilities::native_passive(
            node_id("wasm"),
            [
                PortCapability::new(port_id("in"), PortCapabilityDirection::Receive),
                PortCapability::new(port_id("out"), PortCapabilityDirection::Emit),
            ],
        )
        .expect("valid capabilities");

        validate_wasm_capabilities(&capabilities).expect("no host imports required");
    }

    #[test]
    fn wasm_capabilities_reject_effects_without_imports() {
        let capabilities: NodeCapabilities = NodeCapabilities::new(
            node_id("wasm"),
            [PortCapability::new(
                port_id("in"),
                PortCapabilityDirection::Receive,
            )],
            [EffectCapability::Clock],
        )
        .expect("valid descriptor shape");

        let err: ConduitError =
            validate_wasm_capabilities(&capabilities).expect_err("effect must be denied");

        assert_eq!(err.code(), conduit_core::ErrorCode::InvalidCapabilities);
        assert!(err.to_string().contains("not enforceable"));
    }

    #[test]
    fn port_batches_round_trip_bytes_and_control_payloads() {
        let mut inputs: BatchInputs = BatchInputs::new();
        inputs.push(
            port_id("in"),
            PortPacket::new(
                metadata(),
                PacketPayload::from(b"bytes".as_slice().to_vec()),
            ),
        );
        inputs.push(
            port_id("control"),
            PortPacket::new(metadata(), PacketPayload::from(json!({"op": "flush"}))),
        );

        let wit_batches: Vec<WitPortBatch> =
            to_wit_port_batches(&inputs).expect("inputs should encode as WIT batches");
        let outputs: BatchOutputs =
            from_wit_port_batches(wit_batches).expect("WIT batches should decode");

        assert_eq!(outputs.packets(&port_id("in")).len(), 1);
        assert_eq!(outputs.packets(&port_id("control")).len(), 1);
    }

    #[test]
    fn invalid_control_payload_is_rejected() {
        let packet: WitPacket = WitPacket {
            metadata: metadata(),
            payload: WitPayload::Control("not-json".to_owned()),
        };

        let err: ConduitError = from_wit_packet(packet).expect_err("invalid JSON should fail");

        assert_eq!(err.code(), conduit_core::ErrorCode::NodeExecutionFailed);
    }

    #[test]
    fn dynamic_result_value_decodes_outputs() {
        let output = WitPortBatch {
            port_id: "out".to_owned(),
            packets: vec![WitPacket {
                metadata: metadata(),
                payload: WitPayload::Bytes(b"payload".to_vec()),
            }],
        };
        let result = Val::Result(Ok(Some(Box::new(Val::List(vec![port_batch_to_val(
            output,
        )])))));

        let outputs = batch_outputs_from_result_val(result).expect("result should decode");

        assert_eq!(outputs.packets(&port_id("out")).len(), 1);
    }

    #[test]
    fn dynamic_guest_error_maps_to_execution_error() {
        let result = Val::Result(Err(Some(Box::new(Val::Variant(
            "guest-failure".to_owned(),
            Some(Box::new(Val::String("boom".to_owned()))),
        )))));

        let err = batch_outputs_from_result_val(result).expect_err("guest error should fail");

        assert_eq!(err.code(), conduit_core::ErrorCode::NodeExecutionFailed);
    }
}
