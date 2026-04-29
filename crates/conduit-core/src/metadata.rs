//! Runtime metadata collection boundary.
//!
//! ## Fragment: metadata-collection-boundary
//!
//! Metadata remains split by source type: context metadata identifies an
//! execution attempt, message metadata travels with payloads, and lifecycle
//! metadata records runtime transitions. Message boundary records describe
//! send, receive, and drop observations at the port seam. The sink below is
//! only the collection seam. It lets runtime code report those existing facts
//! without collapsing them into a premature storage, tracing, or graph model.

use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    sync::{Mutex, MutexGuard, PoisonError},
};

use serde_json::{Value, json};

use crate::{
    Result,
    context::{CancellationState, ExecutionMetadata, NodeContext},
    lifecycle::{LifecycleEvent, LifecycleEventKind},
    message::{MessageEndpoint, MessageMetadata, MessageRoute},
};

/// Where a message was observed at the port boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageBoundaryKind {
    /// The message entered an output boundary.
    Enqueued,
    /// The message left an input boundary.
    Dequeued,
    /// The message was dropped at an output boundary.
    Dropped,
}

/// One message observation at a runtime boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageBoundaryRecord {
    kind: MessageBoundaryKind,
    metadata: MessageMetadata,
}

impl MessageBoundaryRecord {
    /// Create a message boundary observation.
    #[must_use]
    pub const fn new(kind: MessageBoundaryKind, metadata: MessageMetadata) -> Self {
        Self { kind, metadata }
    }

    /// Kind of port-boundary observation.
    #[must_use]
    pub const fn kind(&self) -> MessageBoundaryKind {
        self.kind
    }

    /// Message metadata observed at the boundary.
    #[must_use]
    pub const fn metadata(&self) -> &MessageMetadata {
        &self.metadata
    }
}

/// One metadata fact observed at a runtime boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataRecord {
    /// Execution context metadata for one node boundary.
    ExecutionContext(NodeContext),
    /// Lifecycle transition emitted by the runtime.
    Lifecycle(LifecycleEvent),
    /// Message metadata observed at a port boundary.
    Message(MessageBoundaryRecord),
}

/// Convert one metadata record into the stable JSON shape used by JSONL sinks.
///
/// The projection intentionally omits wall-clock timestamps and process-local
/// facts so repeated runs with the same runtime facts can produce identical
/// log lines.
#[must_use]
pub fn metadata_record_to_json_value(record: &MetadataRecord) -> Value {
    match record {
        MetadataRecord::ExecutionContext(context) => json!({
            "record_type": "execution_context",
            "context": node_context_to_json_value(context),
        }),
        MetadataRecord::Lifecycle(event) => json!({
            "record_type": "lifecycle",
            "kind": lifecycle_event_kind_label(event.kind()),
            "context": node_context_to_json_value(event.context()),
        }),
        MetadataRecord::Message(message) => json!({
            "record_type": "message",
            "kind": message_boundary_kind_label(message.kind()),
            "message": message_metadata_to_json_value(message.metadata()),
        }),
    }
}

/// Collection sink for runtime metadata records.
pub trait MetadataSink: Send + Sync {
    /// Record one metadata fact.
    ///
    /// # Errors
    ///
    /// Returns an error when the sink cannot preserve or forward the record.
    fn record(&self, record: &MetadataRecord) -> Result<()>;
}

/// Metadata sink that intentionally records nothing.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopMetadataSink;

impl MetadataSink for NoopMetadataSink {
    fn record(&self, _record: &MetadataRecord) -> Result<()> {
        Ok(())
    }
}

/// Metadata sink that writes one stable JSON object per line.
#[derive(Debug)]
pub struct JsonlMetadataSink<W> {
    writer: Mutex<W>,
}

impl<W> JsonlMetadataSink<W> {
    /// Create a JSONL metadata sink around an existing writer.
    #[must_use]
    pub const fn new(writer: W) -> Self {
        Self {
            writer: Mutex::new(writer),
        }
    }

    /// Return the wrapped writer.
    ///
    /// # Errors
    ///
    /// Returns an error if a prior panic poisoned the writer lock.
    pub fn into_inner(self) -> Result<W> {
        self.writer
            .into_inner()
            .map_err(|_err: PoisonError<W>| jsonl_lock_error())
    }

    fn lock_writer(&self) -> Result<MutexGuard<'_, W>> {
        self.writer
            .lock()
            .map_err(|_err: PoisonError<MutexGuard<'_, W>>| jsonl_lock_error())
    }
}

impl<W> JsonlMetadataSink<W>
where
    W: Write,
{
    /// Flush the wrapped writer.
    ///
    /// # Errors
    ///
    /// Returns an error if the writer cannot flush buffered metadata.
    pub fn flush(&self) -> Result<()> {
        let mut writer: MutexGuard<'_, W> = self.lock_writer()?;
        writer.flush().map_err(|source: std::io::Error| {
            crate::ConduitError::metadata(format!("failed to flush metadata JSONL: {source}"))
        })
    }
}

impl JsonlMetadataSink<BufWriter<File>> {
    /// Create a file-backed JSONL metadata sink, truncating any existing file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created.
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file: File = File::create(path).map_err(|source: std::io::Error| {
            crate::ConduitError::metadata(format!("failed to create metadata JSONL file: {source}"))
        })?;

        Ok(Self::new(BufWriter::new(file)))
    }
}

impl<W> MetadataSink for JsonlMetadataSink<W>
where
    W: Write + Send,
{
    fn record(&self, record: &MetadataRecord) -> Result<()> {
        let value: Value = metadata_record_to_json_value(record);
        let mut writer: MutexGuard<'_, W> = self.lock_writer()?;

        serde_json::to_writer(&mut *writer, &value).map_err(|source: serde_json::Error| {
            crate::ConduitError::metadata(format!(
                "failed to encode metadata JSONL record: {source}"
            ))
        })?;
        writer.write_all(b"\n").map_err(|source: std::io::Error| {
            crate::ConduitError::metadata(format!(
                "failed to write metadata JSONL newline: {source}"
            ))
        })
    }
}

fn node_context_to_json_value(context: &NodeContext) -> Value {
    json!({
        "workflow_id": context.workflow_id().as_str(),
        "node_id": context.node_id().as_str(),
        "execution": execution_metadata_to_json_value(context.execution()),
        "cancellation": cancellation_state_to_json_value(context.cancellation()),
    })
}

fn execution_metadata_to_json_value(execution: &ExecutionMetadata) -> Value {
    json!({
        "execution_id": execution.execution_id().as_str(),
        "attempt": execution.attempt().get(),
    })
}

fn cancellation_state_to_json_value(cancellation: CancellationState) -> Value {
    match cancellation {
        CancellationState::Active => json!({
            "state": "active",
        }),
        CancellationState::Requested(request) => json!({
            "state": "requested",
            "reason": request.reason(),
        }),
    }
}

const fn lifecycle_event_kind_label(kind: LifecycleEventKind) -> &'static str {
    match kind {
        LifecycleEventKind::NodeScheduled => "node_scheduled",
        LifecycleEventKind::NodeStarted => "node_started",
        LifecycleEventKind::NodeCompleted => "node_completed",
        LifecycleEventKind::NodeFailed => "node_failed",
        LifecycleEventKind::NodeCancelled => "node_cancelled",
    }
}

const fn message_boundary_kind_label(kind: MessageBoundaryKind) -> &'static str {
    match kind {
        MessageBoundaryKind::Enqueued => "enqueued",
        MessageBoundaryKind::Dequeued => "dequeued",
        MessageBoundaryKind::Dropped => "dropped",
    }
}

fn message_metadata_to_json_value(metadata: &MessageMetadata) -> Value {
    json!({
        "message_id": metadata.message_id().as_str(),
        "workflow_id": metadata.workflow_id().as_str(),
        "execution": execution_metadata_to_json_value(metadata.execution()),
        "route": message_route_to_json_value(metadata.route()),
    })
}

fn message_route_to_json_value(route: &MessageRoute) -> Value {
    json!({
        "source": route
            .source()
            .map_or(Value::Null, message_endpoint_to_json_value),
        "target": message_endpoint_to_json_value(route.target()),
    })
}

fn message_endpoint_to_json_value(endpoint: &MessageEndpoint) -> Value {
    json!({
        "node_id": endpoint.node_id().as_str(),
        "port_id": endpoint.port_id().as_str(),
    })
}

fn jsonl_lock_error() -> crate::ConduitError {
    crate::ConduitError::metadata("metadata JSONL writer lock poisoned")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ExecutionMetadata;
    use conduit_types::{ExecutionId, MessageId, NodeId, PortId, WorkflowId};
    use std::io;

    fn execution_id(value: &str) -> ExecutionId {
        ExecutionId::new(value).expect("valid execution id")
    }

    fn node_id(value: &str) -> NodeId {
        NodeId::new(value).expect("valid node id")
    }

    fn workflow_id(value: &str) -> WorkflowId {
        WorkflowId::new(value).expect("valid workflow id")
    }

    fn port_id(value: &str) -> PortId {
        PortId::new(value).expect("valid port id")
    }

    fn context() -> NodeContext {
        NodeContext::new(
            workflow_id("flow"),
            node_id("node"),
            ExecutionMetadata::first_attempt(execution_id("run-1")),
        )
    }

    fn message_metadata() -> MessageMetadata {
        let source: crate::message::MessageEndpoint =
            crate::message::MessageEndpoint::new(node_id("source"), port_id("out"));
        let target: crate::message::MessageEndpoint =
            crate::message::MessageEndpoint::new(node_id("sink"), port_id("in"));
        let route: crate::message::MessageRoute =
            crate::message::MessageRoute::new(Some(source), target);

        MessageMetadata::new(
            MessageId::new("msg-1").expect("valid message id"),
            workflow_id("flow"),
            ExecutionMetadata::first_attempt(execution_id("run-1")),
            route,
        )
    }

    #[test]
    fn metadata_record_keeps_context_shape_intact() {
        let record: MetadataRecord = MetadataRecord::ExecutionContext(context());

        assert!(matches!(
            record,
            MetadataRecord::ExecutionContext(ctx) if ctx.node_id().as_str() == "node"
        ));
    }

    #[test]
    fn noop_metadata_sink_accepts_records() {
        let record: MetadataRecord = MetadataRecord::ExecutionContext(context());

        NoopMetadataSink
            .record(&record)
            .expect("noop metadata sink should accept records");
    }

    #[test]
    fn message_boundary_record_keeps_shape_intact() {
        let target: crate::message::MessageEndpoint =
            crate::message::MessageEndpoint::new(node_id("sink"), port_id("in"));
        let route: crate::message::MessageRoute = crate::message::MessageRoute::new(None, target);
        let metadata: MessageMetadata = MessageMetadata::new(
            MessageId::new("msg-1").expect("valid message id"),
            workflow_id("flow"),
            ExecutionMetadata::first_attempt(execution_id("run-1")),
            route,
        );
        let record: MessageBoundaryRecord =
            MessageBoundaryRecord::new(MessageBoundaryKind::Enqueued, metadata);

        assert!(matches!(
            record,
            MessageBoundaryRecord {
                kind: MessageBoundaryKind::Enqueued,
                ..
            }
        ));
    }

    #[test]
    fn metadata_record_json_uses_stable_message_shape() {
        let record: MetadataRecord = MetadataRecord::Message(MessageBoundaryRecord::new(
            MessageBoundaryKind::Dequeued,
            message_metadata(),
        ));

        assert_eq!(
            metadata_record_to_json_value(&record),
            json!({
                "record_type": "message",
                "kind": "dequeued",
                "message": {
                    "message_id": "msg-1",
                    "workflow_id": "flow",
                    "execution": {
                        "execution_id": "run-1",
                        "attempt": 1,
                    },
                    "route": {
                        "source": {
                            "node_id": "source",
                            "port_id": "out",
                        },
                        "target": {
                            "node_id": "sink",
                            "port_id": "in",
                        },
                    },
                },
            })
        );
    }

    #[test]
    fn jsonl_metadata_sink_writes_reproducible_lines() {
        let sink: JsonlMetadataSink<Vec<u8>> = JsonlMetadataSink::new(Vec::new());
        let record: MetadataRecord = MetadataRecord::Lifecycle(LifecycleEvent::new(
            LifecycleEventKind::NodeStarted,
            context(),
        ));

        sink.record(&record)
            .expect("first metadata record should write");
        sink.record(&record)
            .expect("second metadata record should write");
        let output: String = String::from_utf8(sink.into_inner().expect("writer should return"))
            .expect("JSONL should be UTF-8");
        let mut lines = output.lines();
        let first = lines.next().expect("first JSONL line should exist");
        let second = lines.next().expect("second JSONL line should exist");

        assert_eq!(first, second);
        assert!(lines.next().is_none());
    }

    struct FailingWriter;

    impl Write for FailingWriter {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("write failed"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn jsonl_metadata_sink_maps_writer_failures() {
        let sink: JsonlMetadataSink<FailingWriter> = JsonlMetadataSink::new(FailingWriter);
        let record: MetadataRecord = MetadataRecord::ExecutionContext(context());
        let err: crate::ConduitError = sink
            .record(&record)
            .expect_err("writer failure should surface");

        assert_eq!(err.code(), crate::ErrorCode::MetadataCollectionFailed);
        assert!(err.to_string().contains("failed to encode"));
    }
}
