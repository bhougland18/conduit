//! Conduit-owned port handles for the executor boundary.
//!
//! ## Fragment: executor-port-staging
//!
//! The executor contract is explicit about engine-owned inputs and outputs.
//! These handles now preserve declared port identity and can carry bounded
//! edge channels, but the node-facing surface remains Conduit-owned. Runtime
//! code may use `asupersync` internally without making node implementations
//! depend on raw runtime channels or task context.
//!
//! ## Fragment: port-adapter-boundary
//!
//! Future implementations should extend these handles with cancel-safe bounded
//! channel adapters, but callers should still depend on Conduit port semantics
//! rather than on the concrete async runtime. In practice that means
//! `asupersync` concepts such as task context, send permits, and channel errors
//! belong behind `PortsIn` and `PortsOut`, with explicit Conduit error and
//! cancellation mapping at the boundary.
//!
//! ## Fragment: output-reserve-commit
//!
//! Output sends use a two-phase reserve/commit shape even before the fully
//! async `Cx`-based API lands. Reserving capacity produces a Conduit-owned
//! permit; committing enqueues the packet; dropping or aborting the permit
//! releases capacity without creating a ghost message. This mirrors the
//! `asupersync` channel contract while keeping runtime details hidden.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;

use asupersync::channel::mpsc;
use conduit_types::PortId;

use crate::message::MessageEnvelope;

/// Default packet payload for the first channel-backed port surface.
pub type PortPacket = MessageEnvelope<Vec<u8>>;

/// Error returned when an output port cannot accept a packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PortSendError {
    /// The node does not declare the requested output port.
    UnknownPort {
        /// Port requested by the caller.
        port_id: PortId,
    },
    /// A downstream input has disconnected.
    Disconnected {
        /// Output port being sent through.
        port_id: PortId,
    },
    /// At least one bounded downstream edge is full.
    Full {
        /// Output port being sent through.
        port_id: PortId,
    },
}

impl fmt::Display for PortSendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownPort { port_id } => {
                write!(f, "output port `{port_id}` is not declared")
            }
            Self::Disconnected { port_id } => {
                write!(f, "output port `{port_id}` is disconnected")
            }
            Self::Full { port_id } => write!(f, "output port `{port_id}` is full"),
        }
    }
}

impl Error for PortSendError {}

/// Error returned when an input port cannot provide a packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PortRecvError {
    /// The node does not declare the requested input port.
    UnknownPort {
        /// Port requested by the caller.
        port_id: PortId,
    },
    /// All upstream senders for this input have disconnected.
    Disconnected {
        /// Input port being received from.
        port_id: PortId,
    },
}

impl fmt::Display for PortRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownPort { port_id } => {
                write!(f, "input port `{port_id}` is not declared")
            }
            Self::Disconnected { port_id } => {
                write!(f, "input port `{port_id}` is disconnected")
            }
        }
    }
}

impl Error for PortRecvError {}

/// Channel-backed input endpoint for one declared input port.
#[derive(Debug)]
pub struct InputPortHandle {
    port_id: PortId,
    receivers: Vec<mpsc::Receiver<PortPacket>>,
}

impl InputPortHandle {
    /// Create an input handle with no connected upstream edge.
    #[must_use]
    pub const fn disconnected(port_id: PortId) -> Self {
        Self {
            port_id,
            receivers: Vec::new(),
        }
    }

    fn connected(port_id: PortId, receiver: mpsc::Receiver<PortPacket>) -> Self {
        Self {
            port_id,
            receivers: vec![receiver],
        }
    }

    fn append(&mut self, mut other: Self) {
        self.receivers.append(&mut other.receivers);
    }

    /// Declared input port identifier.
    #[must_use]
    pub const fn port_id(&self) -> &PortId {
        &self.port_id
    }

    /// Number of upstream bounded edges connected to this input port.
    #[must_use]
    pub const fn connected_edge_count(&self) -> usize {
        self.receivers.len()
    }

    /// Capacity of the first connected upstream edge, if one exists.
    #[must_use]
    pub fn capacity(&self) -> Option<usize> {
        self.receivers.first().map(mpsc::Receiver::capacity)
    }

    fn try_recv(&mut self) -> Result<Option<PortPacket>, PortRecvError> {
        let mut disconnected_count: usize = 0;

        for receiver in &mut self.receivers {
            match receiver.try_recv() {
                Ok(packet) => return Ok(Some(packet)),
                Err(mpsc::RecvError::Empty) => {}
                Err(mpsc::RecvError::Disconnected | mpsc::RecvError::Cancelled) => {
                    disconnected_count += 1;
                }
            }
        }

        if !self.receivers.is_empty() && disconnected_count == self.receivers.len() {
            return Err(PortRecvError::Disconnected {
                port_id: self.port_id.clone(),
            });
        }

        Ok(None)
    }
}

/// Channel-backed output endpoint for one declared output port.
#[derive(Debug, Clone)]
pub struct OutputPortHandle {
    port_id: PortId,
    senders: Vec<mpsc::Sender<PortPacket>>,
}

impl OutputPortHandle {
    /// Create an output handle with no connected downstream edge.
    #[must_use]
    pub const fn disconnected(port_id: PortId) -> Self {
        Self {
            port_id,
            senders: Vec::new(),
        }
    }

    fn connected(port_id: PortId, sender: mpsc::Sender<PortPacket>) -> Self {
        Self {
            port_id,
            senders: vec![sender],
        }
    }

    fn append(&mut self, mut other: Self) {
        self.senders.append(&mut other.senders);
    }

    /// Declared output port identifier.
    #[must_use]
    pub const fn port_id(&self) -> &PortId {
        &self.port_id
    }

    /// Number of downstream bounded edges connected to this output port.
    #[must_use]
    pub const fn connected_edge_count(&self) -> usize {
        self.senders.len()
    }

    /// Capacity of the first connected downstream edge, if one exists.
    #[must_use]
    pub fn capacity(&self) -> Option<usize> {
        self.senders.first().map(mpsc::Sender::capacity)
    }

    fn try_reserve(&self) -> Result<OutputPortSendPermit<'_>, PortSendError> {
        let split_senders: Option<(&mpsc::Sender<PortPacket>, &[mpsc::Sender<PortPacket>])> =
            self.senders.split_last();
        let (last_sender, leading_senders): (
            &mpsc::Sender<PortPacket>,
            &[mpsc::Sender<PortPacket>],
        ) = match split_senders {
            Some(value) => value,
            None => {
                return Ok(OutputPortSendPermit {
                    permits: Vec::new(),
                });
            }
        };

        let mut permits: Vec<mpsc::SendPermit<'_, PortPacket>> =
            Vec::with_capacity(self.senders.len());

        for sender in leading_senders {
            match sender.try_reserve() {
                Ok(permit) => permits.push(permit),
                Err(err) => {
                    return Err(self.map_send_error(err));
                }
            }
        }

        match last_sender.try_reserve() {
            Ok(permit) => {
                permits.push(permit);
                Ok(OutputPortSendPermit { permits })
            }
            Err(err) => Err(self.map_send_error(err)),
        }
    }

    fn map_send_error(&self, err: mpsc::SendError<()>) -> PortSendError {
        match err {
            mpsc::SendError::Disconnected(()) | mpsc::SendError::Cancelled(()) => {
                PortSendError::Disconnected {
                    port_id: self.port_id.clone(),
                }
            }
            mpsc::SendError::Full(()) => PortSendError::Full {
                port_id: self.port_id.clone(),
            },
        }
    }
}

/// Reserved output capacity for one declared output port.
#[derive(Debug)]
#[must_use = "PortSendPermit must be committed with send() or explicitly aborted"]
pub struct PortSendPermit<'a> {
    inner: OutputPortSendPermit<'a>,
}

impl PortSendPermit<'_> {
    /// Commit the reserved capacity and enqueue the packet.
    pub fn send(self, packet: PortPacket) {
        self.inner.send(packet);
    }

    /// Release the reserved capacity without enqueueing a packet.
    pub fn abort(self) {
        self.inner.abort();
    }
}

#[derive(Debug)]
struct OutputPortSendPermit<'a> {
    permits: Vec<mpsc::SendPermit<'a, PortPacket>>,
}

impl OutputPortSendPermit<'_> {
    fn send(mut self, packet: PortPacket) {
        let last_permit: Option<mpsc::SendPermit<'_, PortPacket>> = self.permits.pop();
        let last_permit: mpsc::SendPermit<'_, PortPacket> = match last_permit {
            Some(permit) => permit,
            None => return,
        };
        let leading_permits: Vec<mpsc::SendPermit<'_, PortPacket>> = self.permits;

        for permit in leading_permits {
            permit.send(packet.clone());
        }
        last_permit.send(packet);
    }

    fn abort(self) {
        for permit in self.permits {
            permit.abort();
        }
    }
}

/// Create one bounded edge channel between an output port and an input port.
#[must_use]
pub fn bounded_edge_channel(
    output_port_id: PortId,
    input_port_id: PortId,
    capacity: NonZeroUsize,
) -> (OutputPortHandle, InputPortHandle) {
    let (sender, receiver): (mpsc::Sender<PortPacket>, mpsc::Receiver<PortPacket>) =
        mpsc::channel(capacity.get());
    (
        OutputPortHandle::connected(output_port_id, sender),
        InputPortHandle::connected(input_port_id, receiver),
    )
}

/// Declared input ports available to a node execution boundary.
#[derive(Debug, Default)]
pub struct PortsIn {
    port_ids: Vec<PortId>,
    ports: Vec<InputPortHandle>,
}

impl PortsIn {
    /// Create input handles with declared port identifiers and no channels.
    #[must_use]
    pub fn new(port_ids: impl Into<Vec<PortId>>) -> Self {
        let port_ids: Vec<PortId> = port_ids.into();
        Self::from_handles(port_ids, Vec::new())
    }

    /// Create input handles from declared ports and connected channel handles.
    #[must_use]
    pub fn from_handles(
        port_ids: impl Into<Vec<PortId>>,
        handles: impl Into<Vec<InputPortHandle>>,
    ) -> Self {
        let port_ids: Vec<PortId> = port_ids.into();
        let mut by_port: BTreeMap<PortId, InputPortHandle> = BTreeMap::new();

        for handle in handles.into() {
            let port_id: PortId = handle.port_id().clone();
            if let Some(existing) = by_port.get_mut(&port_id) {
                existing.append(handle);
            } else {
                by_port.insert(port_id, handle);
            }
        }

        let mut ports: Vec<InputPortHandle> = Vec::with_capacity(port_ids.len());
        for port_id in &port_ids {
            let handle: InputPortHandle = by_port
                .remove(port_id)
                .unwrap_or_else(|| InputPortHandle::disconnected(port_id.clone()));
            ports.push(handle);
        }

        Self { port_ids, ports }
    }

    /// Declared input port identifiers for this node.
    #[must_use]
    pub fn port_ids(&self) -> &[PortId] {
        &self.port_ids
    }

    /// Return whether this node currently has no declared inputs.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.port_ids.is_empty()
    }

    /// Number of connected upstream edges for a declared input port.
    #[must_use]
    pub fn connected_edge_count(&self, port_id: &PortId) -> Option<usize> {
        self.ports
            .iter()
            .find(|port: &&InputPortHandle| port.port_id() == port_id)
            .map(InputPortHandle::connected_edge_count)
    }

    /// Capacity of the first connected upstream edge for a declared input port.
    #[must_use]
    pub fn capacity(&self, port_id: &PortId) -> Option<usize> {
        self.ports
            .iter()
            .find(|port: &&InputPortHandle| port.port_id() == port_id)
            .and_then(InputPortHandle::capacity)
    }

    /// Try to receive one packet from a declared input port without blocking.
    ///
    /// Returns `Ok(None)` when the port is declared but no packet is currently
    /// queued. The async waiting surface is intentionally deferred to the
    /// reserve/commit bead.
    ///
    /// # Errors
    ///
    /// Returns an error if the port is undeclared or all upstream senders have
    /// disconnected.
    pub fn try_recv(&mut self, port_id: &PortId) -> Result<Option<PortPacket>, PortRecvError> {
        let port: &mut InputPortHandle = self
            .ports
            .iter_mut()
            .find(|port: &&mut InputPortHandle| port.port_id() == port_id)
            .ok_or_else(|| PortRecvError::UnknownPort {
                port_id: port_id.clone(),
            })?;
        port.try_recv()
    }
}

/// Declared output ports available to a node execution boundary.
#[derive(Debug, Clone, Default)]
pub struct PortsOut {
    port_ids: Vec<PortId>,
    ports: Vec<OutputPortHandle>,
}

impl PortsOut {
    /// Create output handles with declared port identifiers and no channels.
    #[must_use]
    pub fn new(port_ids: impl Into<Vec<PortId>>) -> Self {
        let port_ids: Vec<PortId> = port_ids.into();
        Self::from_handles(port_ids, Vec::new())
    }

    /// Create output handles from declared ports and connected channel handles.
    #[must_use]
    pub fn from_handles(
        port_ids: impl Into<Vec<PortId>>,
        handles: impl Into<Vec<OutputPortHandle>>,
    ) -> Self {
        let port_ids: Vec<PortId> = port_ids.into();
        let mut by_port: BTreeMap<PortId, OutputPortHandle> = BTreeMap::new();

        for handle in handles.into() {
            let port_id: PortId = handle.port_id().clone();
            if let Some(existing) = by_port.get_mut(&port_id) {
                existing.append(handle);
            } else {
                by_port.insert(port_id, handle);
            }
        }

        let mut ports: Vec<OutputPortHandle> = Vec::with_capacity(port_ids.len());
        for port_id in &port_ids {
            let handle: OutputPortHandle = by_port
                .remove(port_id)
                .unwrap_or_else(|| OutputPortHandle::disconnected(port_id.clone()));
            ports.push(handle);
        }

        Self { port_ids, ports }
    }

    /// Declared output port identifiers for this node.
    #[must_use]
    pub fn port_ids(&self) -> &[PortId] {
        &self.port_ids
    }

    /// Return whether this node currently has no declared outputs.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.port_ids.is_empty()
    }

    /// Number of connected downstream edges for a declared output port.
    #[must_use]
    pub fn connected_edge_count(&self, port_id: &PortId) -> Option<usize> {
        self.ports
            .iter()
            .find(|port: &&OutputPortHandle| port.port_id() == port_id)
            .map(OutputPortHandle::connected_edge_count)
    }

    /// Capacity of the first connected downstream edge for a declared output port.
    #[must_use]
    pub fn capacity(&self, port_id: &PortId) -> Option<usize> {
        self.ports
            .iter()
            .find(|port: &&OutputPortHandle| port.port_id() == port_id)
            .and_then(OutputPortHandle::capacity)
    }

    /// Try to send one packet through a declared output port without blocking.
    ///
    /// Unconnected declared output ports accept and drop packets. That keeps
    /// early scaffold nodes simple while later beads define explicit fan-out
    /// and disconnected-edge policy. Connected sends reserve capacity before
    /// committing the packet, so cancellation or drop between those phases
    /// releases the reserved slots instead of creating partial messages.
    ///
    /// # Errors
    ///
    /// Returns an error if the port is undeclared, a downstream receiver has
    /// disconnected, or a bounded downstream edge is full.
    pub fn try_send(&self, port_id: &PortId, packet: PortPacket) -> Result<(), PortSendError> {
        self.try_reserve(port_id)?.send(packet);
        Ok(())
    }

    /// Try to reserve output capacity without committing a packet.
    ///
    /// Dropping the returned permit releases all reserved downstream slots.
    ///
    /// # Errors
    ///
    /// Returns an error if the port is undeclared, a downstream receiver has
    /// disconnected, or a bounded downstream edge is full.
    pub fn try_reserve(&self, port_id: &PortId) -> Result<PortSendPermit<'_>, PortSendError> {
        let port: &OutputPortHandle = self
            .ports
            .iter()
            .find(|port: &&OutputPortHandle| port.port_id() == port_id)
            .ok_or_else(|| PortSendError::UnknownPort {
                port_id: port_id.clone(),
            })?;
        port.try_reserve()
            .map(|inner: OutputPortSendPermit<'_>| PortSendPermit { inner })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use conduit_types::{ExecutionId, MessageId, NodeId, WorkflowId};

    use crate::{
        context::ExecutionMetadata,
        message::{MessageEndpoint, MessageMetadata, MessageRoute},
    };

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

    fn packet(value: &[u8]) -> PortPacket {
        let source: MessageEndpoint = MessageEndpoint::new(node_id("source"), port_id("out"));
        let target: MessageEndpoint = MessageEndpoint::new(node_id("sink"), port_id("in"));
        let route: MessageRoute = MessageRoute::new(Some(source), target);
        let execution: ExecutionMetadata = ExecutionMetadata::first_attempt(execution_id("run-1"));
        let metadata: MessageMetadata =
            MessageMetadata::new(message_id("msg-1"), workflow_id("flow"), execution, route);

        MessageEnvelope::new(metadata, value.to_vec())
    }

    #[test]
    fn ports_preserve_declared_port_order() {
        let inputs: PortsIn = PortsIn::new(vec![port_id("left"), port_id("right")]);
        let outputs: PortsOut = PortsOut::new(vec![port_id("out")]);

        assert_eq!(
            inputs
                .port_ids()
                .iter()
                .map(PortId::as_str)
                .collect::<Vec<_>>(),
            vec!["left", "right"]
        );
        assert_eq!(
            outputs
                .port_ids()
                .iter()
                .map(PortId::as_str)
                .collect::<Vec<_>>(),
            vec!["out"]
        );
    }

    #[test]
    fn bounded_edge_channel_enforces_capacity() {
        let (output, input): (OutputPortHandle, InputPortHandle) =
            bounded_edge_channel(port_id("out"), port_id("in"), NonZeroUsize::MIN);
        let mut inputs: PortsIn = PortsIn::from_handles([port_id("in")], [input]);
        let outputs: PortsOut = PortsOut::from_handles([port_id("out")], [output]);

        outputs
            .try_send(&port_id("out"), packet(b"first"))
            .expect("first packet should fit");
        let err: PortSendError = outputs
            .try_send(&port_id("out"), packet(b"second"))
            .expect_err("second packet should hit bounded capacity");

        assert_eq!(
            err,
            PortSendError::Full {
                port_id: port_id("out")
            }
        );
        assert_eq!(outputs.capacity(&port_id("out")), Some(1));
        assert_eq!(inputs.capacity(&port_id("in")), Some(1));

        let received: PortPacket = inputs
            .try_recv(&port_id("in"))
            .expect("receive should succeed")
            .expect("packet should be queued");

        assert_eq!(received.payload(), b"first");
        assert!(
            inputs
                .try_recv(&port_id("in"))
                .expect("empty receive should not fail")
                .is_none()
        );
    }

    #[test]
    fn reserved_output_capacity_commits_on_send() {
        let (output, input): (OutputPortHandle, InputPortHandle) =
            bounded_edge_channel(port_id("out"), port_id("in"), NonZeroUsize::MIN);
        let mut inputs: PortsIn = PortsIn::from_handles([port_id("in")], [input]);
        let outputs: PortsOut = PortsOut::from_handles([port_id("out")], [output]);

        let permit: PortSendPermit<'_> = outputs
            .try_reserve(&port_id("out"))
            .expect("reservation should succeed");
        let err: PortSendError = outputs
            .try_send(&port_id("out"), packet(b"blocked"))
            .expect_err("reserved capacity should block another send");

        assert_eq!(
            err,
            PortSendError::Full {
                port_id: port_id("out")
            }
        );

        permit.send(packet(b"committed"));

        let received: PortPacket = inputs
            .try_recv(&port_id("in"))
            .expect("receive should succeed")
            .expect("committed packet should be queued");
        assert_eq!(received.payload(), b"committed");
    }

    #[test]
    fn dropped_output_permit_releases_capacity_without_message() {
        let (output, input): (OutputPortHandle, InputPortHandle) =
            bounded_edge_channel(port_id("out"), port_id("in"), NonZeroUsize::MIN);
        let mut inputs: PortsIn = PortsIn::from_handles([port_id("in")], [input]);
        let outputs: PortsOut = PortsOut::from_handles([port_id("out")], [output]);

        let permit: PortSendPermit<'_> = outputs
            .try_reserve(&port_id("out"))
            .expect("reservation should succeed");
        drop(permit);

        assert!(
            inputs
                .try_recv(&port_id("in"))
                .expect("dropped permit should not disconnect")
                .is_none()
        );

        outputs
            .try_send(&port_id("out"), packet(b"after-drop"))
            .expect("dropped permit should release capacity");
        let received: PortPacket = inputs
            .try_recv(&port_id("in"))
            .expect("receive should succeed")
            .expect("new packet should be queued");

        assert_eq!(received.payload(), b"after-drop");
    }

    #[test]
    fn aborted_output_permit_releases_capacity_without_message() {
        let (output, input): (OutputPortHandle, InputPortHandle) =
            bounded_edge_channel(port_id("out"), port_id("in"), NonZeroUsize::MIN);
        let mut inputs: PortsIn = PortsIn::from_handles([port_id("in")], [input]);
        let outputs: PortsOut = PortsOut::from_handles([port_id("out")], [output]);

        outputs
            .try_reserve(&port_id("out"))
            .expect("reservation should succeed")
            .abort();

        assert!(
            inputs
                .try_recv(&port_id("in"))
                .expect("aborted permit should not disconnect")
                .is_none()
        );
        outputs
            .try_send(&port_id("out"), packet(b"after-abort"))
            .expect("aborted permit should release capacity");
    }

    #[test]
    fn undeclared_ports_are_rejected() {
        let mut inputs: PortsIn = PortsIn::new([port_id("in")]);
        let outputs: PortsOut = PortsOut::new([port_id("out")]);

        assert_eq!(
            outputs
                .try_send(&port_id("missing"), packet(b"value"))
                .expect_err("unknown output must fail"),
            PortSendError::UnknownPort {
                port_id: port_id("missing")
            }
        );
        assert_eq!(
            inputs
                .try_recv(&port_id("missing"))
                .expect_err("unknown input must fail"),
            PortRecvError::UnknownPort {
                port_id: port_id("missing")
            }
        );
    }
}
