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

use crate::{Result, context::NodeContext, lifecycle::LifecycleEvent, message::MessageMetadata};

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ExecutionMetadata;
    use conduit_types::{ExecutionId, MessageId, NodeId, PortId, WorkflowId};

    fn execution_id(value: &str) -> ExecutionId {
        ExecutionId::new(value).expect("valid execution id")
    }

    fn node_id(value: &str) -> NodeId {
        NodeId::new(value).expect("valid node id")
    }

    fn workflow_id(value: &str) -> WorkflowId {
        WorkflowId::new(value).expect("valid workflow id")
    }

    fn context() -> NodeContext {
        NodeContext::new(
            workflow_id("flow"),
            node_id("node"),
            ExecutionMetadata::first_attempt(execution_id("run-1")),
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
        let target: crate::message::MessageEndpoint = crate::message::MessageEndpoint::new(
            node_id("sink"),
            PortId::new("in").expect("valid port id"),
        );
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
}
