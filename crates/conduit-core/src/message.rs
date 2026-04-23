//! Message envelope and routing metadata types.

use conduit_types::{MessageId, NodeId, PortId, WorkflowId};

use crate::context::ExecutionMetadata;

/// Node/port endpoint for a message envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageEndpoint {
    node_id: NodeId,
    port_id: PortId,
}

impl MessageEndpoint {
    /// Create a message endpoint.
    #[must_use]
    pub const fn new(node_id: NodeId, port_id: PortId) -> Self {
        Self { node_id, port_id }
    }

    /// Node referenced by this endpoint.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Port referenced by this endpoint.
    #[must_use]
    pub const fn port_id(&self) -> &PortId {
        &self.port_id
    }
}

/// Static routing metadata carried alongside a message payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageRoute {
    source: Option<MessageEndpoint>,
    target: MessageEndpoint,
}

impl MessageRoute {
    /// Create routing metadata from an optional source to a required target.
    #[must_use]
    pub const fn new(source: Option<MessageEndpoint>, target: MessageEndpoint) -> Self {
        Self { source, target }
    }

    /// Upstream source endpoint, absent for externally injected messages.
    #[must_use]
    pub const fn source(&self) -> Option<&MessageEndpoint> {
        self.source.as_ref()
    }

    /// Downstream target endpoint.
    #[must_use]
    pub const fn target(&self) -> &MessageEndpoint {
        &self.target
    }
}

/// Metadata attached to every message envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageMetadata {
    message_id: MessageId,
    workflow_id: WorkflowId,
    execution: ExecutionMetadata,
    route: MessageRoute,
}

impl MessageMetadata {
    /// Create metadata for one message envelope.
    #[must_use]
    pub const fn new(
        message_id: MessageId,
        workflow_id: WorkflowId,
        execution: ExecutionMetadata,
        route: MessageRoute,
    ) -> Self {
        Self {
            message_id,
            workflow_id,
            execution,
            route,
        }
    }

    /// Identifier for this message.
    #[must_use]
    pub const fn message_id(&self) -> &MessageId {
        &self.message_id
    }

    /// Workflow associated with this message.
    #[must_use]
    pub const fn workflow_id(&self) -> &WorkflowId {
        &self.workflow_id
    }

    /// Execution metadata associated with this message.
    #[must_use]
    pub const fn execution(&self) -> &ExecutionMetadata {
        &self.execution
    }

    /// Static route for this message.
    #[must_use]
    pub const fn route(&self) -> &MessageRoute {
        &self.route
    }
}

/// Runtime message envelope that keeps payloads separate from routing metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageEnvelope<P> {
    metadata: MessageMetadata,
    payload: P,
}

impl<P> MessageEnvelope<P> {
    /// Create a message envelope.
    #[must_use]
    pub const fn new(metadata: MessageMetadata, payload: P) -> Self {
        Self { metadata, payload }
    }

    /// Metadata that travels with the payload.
    #[must_use]
    pub const fn metadata(&self) -> &MessageMetadata {
        &self.metadata
    }

    /// Borrow the payload.
    #[must_use]
    pub const fn payload(&self) -> &P {
        &self.payload
    }

    /// Consume the envelope and return the payload.
    #[must_use]
    pub fn into_payload(self) -> P {
        self.payload
    }

    /// Transform the payload while preserving metadata.
    #[must_use]
    pub fn map_payload<Q>(self, f: impl FnOnce(P) -> Q) -> MessageEnvelope<Q> {
        MessageEnvelope {
            metadata: self.metadata,
            payload: f(self.payload),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use conduit_types::ExecutionId;

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

    fn execution() -> ExecutionMetadata {
        ExecutionMetadata::first_attempt(execution_id("run-1"))
    }

    #[test]
    fn message_envelope_keeps_payload_separate_from_metadata() {
        let target: MessageEndpoint = MessageEndpoint::new(node_id("consumer"), port_id("in"));
        let route: MessageRoute = MessageRoute::new(None, target);
        let metadata: MessageMetadata =
            MessageMetadata::new(message_id("msg-1"), workflow_id("flow"), execution(), route);
        let envelope: MessageEnvelope<&str> = MessageEnvelope::new(metadata, "payload");
        let mapped: MessageEnvelope<usize> = envelope.map_payload(str::len);

        assert_eq!(mapped.payload(), &7);
        assert_eq!(mapped.metadata().message_id().as_str(), "msg-1");
        assert_eq!(
            mapped.metadata().route().target().node_id().as_str(),
            "consumer"
        );
    }
}
