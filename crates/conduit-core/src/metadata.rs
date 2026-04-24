//! Runtime metadata collection boundary.
//!
//! ## Fragment: metadata-collection-boundary
//!
//! Metadata remains split by source type: context metadata identifies an
//! execution attempt, message metadata travels with payloads, and lifecycle
//! metadata records runtime transitions. The sink below is only the collection
//! seam. It lets runtime code report those existing facts without collapsing
//! them into a premature storage, tracing, or graph model.

use crate::{Result, context::NodeContext, lifecycle::LifecycleEvent, message::MessageMetadata};

/// One metadata fact observed at a runtime boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataRecord {
    /// Execution context metadata for one node boundary.
    ExecutionContext(NodeContext),
    /// Lifecycle transition emitted by the runtime.
    Lifecycle(LifecycleEvent),
    /// Message metadata attached to a payload.
    Message(MessageMetadata),
}

/// Collection sink for runtime metadata records.
pub trait MetadataSink: Sync {
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
    use conduit_types::{ExecutionId, NodeId, WorkflowId};

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
}
