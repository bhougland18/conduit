//! Capability descriptors that constrain runtime behavior without owning graph shape.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use conduit_types::{NodeId, PortId};

/// Direction of message flow a node claims for a port.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortCapabilityDirection {
    /// The node may receive messages through the port.
    Receive,
    /// The node may emit messages through the port.
    Emit,
}

impl PortCapabilityDirection {
    const fn label(self) -> &'static str {
        match self {
            Self::Receive => "receive",
            Self::Emit => "emit",
        }
    }
}

/// External effect a node may request from the runtime boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EffectCapability {
    /// Read from host filesystem resources.
    FileSystemRead,
    /// Write to host filesystem resources.
    FileSystemWrite,
    /// Open outbound network connections.
    NetworkOutbound,
    /// Spawn child processes.
    ProcessSpawn,
    /// Read process environment.
    EnvironmentRead,
    /// Mutate process environment.
    EnvironmentWrite,
    /// Use wall-clock time or timers.
    Clock,
}

/// A named claim that a node may use a port in one direction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortCapability {
    port_id: PortId,
    direction: PortCapabilityDirection,
}

impl PortCapability {
    /// Create a port capability claim.
    #[must_use]
    pub const fn new(port_id: PortId, direction: PortCapabilityDirection) -> Self {
        Self { port_id, direction }
    }

    /// Port claimed by this capability.
    #[must_use]
    pub const fn port_id(&self) -> &PortId {
        &self.port_id
    }

    /// Direction claimed by this capability.
    #[must_use]
    pub const fn direction(&self) -> PortCapabilityDirection {
        self.direction
    }
}

/// Validation error for node capability descriptors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapabilityValidationError {
    /// A node declared the same effect capability more than once.
    DuplicateEffect {
        /// Node whose capability descriptor is invalid.
        node_id: NodeId,
        /// Duplicated effect capability.
        effect: EffectCapability,
    },
    /// A node declared the same port and direction more than once.
    DuplicatePortCapability {
        /// Node whose capability descriptor is invalid.
        node_id: NodeId,
        /// Duplicated port.
        port_id: PortId,
        /// Duplicated direction.
        direction: PortCapabilityDirection,
    },
    /// A node declared one port as both receive and emit.
    ConflictingPortDirection {
        /// Node whose capability descriptor is invalid.
        node_id: NodeId,
        /// Port with conflicting direction claims.
        port_id: PortId,
    },
}

impl fmt::Display for CapabilityValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateEffect { node_id, effect } => {
                write!(
                    f,
                    "node `{node_id}` declares duplicate effect capability `{effect:?}`"
                )
            }
            Self::DuplicatePortCapability {
                node_id,
                port_id,
                direction,
            } => write!(
                f,
                "node `{node_id}` declares duplicate {} capability for port `{port_id}`",
                direction.label()
            ),
            Self::ConflictingPortDirection { node_id, port_id } => write!(
                f,
                "node `{node_id}` declares port `{port_id}` for both receive and emit"
            ),
        }
    }
}

impl Error for CapabilityValidationError {}

/// Validated capability descriptor for one node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeCapabilities {
    node_id: NodeId,
    ports: Vec<PortCapability>,
    effects: Vec<EffectCapability>,
}

impl NodeCapabilities {
    /// Create a validated node capability descriptor.
    ///
    /// # Errors
    ///
    /// Returns an error if the descriptor repeats an effect, repeats a
    /// port-direction claim, or declares one port as both receiving and
    /// emitting.
    pub fn new(
        node_id: NodeId,
        ports: impl Into<Vec<PortCapability>>,
        effects: impl Into<Vec<EffectCapability>>,
    ) -> Result<Self, CapabilityValidationError> {
        let ports: Vec<PortCapability> = ports.into();
        let effects: Vec<EffectCapability> = effects.into();
        reject_duplicate_effects(&node_id, &effects)?;
        reject_invalid_port_capabilities(&node_id, &ports)?;

        Ok(Self {
            node_id,
            ports,
            effects,
        })
    }

    /// Node constrained by this capability descriptor.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Port capabilities claimed by the node.
    #[must_use]
    pub fn ports(&self) -> &[PortCapability] {
        &self.ports
    }

    /// Effect capabilities claimed by the node.
    #[must_use]
    pub fn effects(&self) -> &[EffectCapability] {
        &self.effects
    }

    /// Return whether this descriptor grants a specific effect capability.
    #[must_use]
    pub fn allows_effect(&self, effect: EffectCapability) -> bool {
        self.effects.contains(&effect)
    }

    /// Return whether this descriptor grants a specific port-direction capability.
    #[must_use]
    pub fn allows_port(&self, port_id: &PortId, direction: PortCapabilityDirection) -> bool {
        self.ports
            .iter()
            .any(|port: &PortCapability| port.port_id() == port_id && port.direction() == direction)
    }
}

fn reject_duplicate_effects(
    node_id: &NodeId,
    effects: &[EffectCapability],
) -> Result<(), CapabilityValidationError> {
    let mut seen: BTreeSet<EffectCapability> = BTreeSet::new();

    for effect in effects {
        if !seen.insert(*effect) {
            return Err(CapabilityValidationError::DuplicateEffect {
                node_id: node_id.clone(),
                effect: *effect,
            });
        }
    }

    Ok(())
}

fn reject_invalid_port_capabilities(
    node_id: &NodeId,
    ports: &[PortCapability],
) -> Result<(), CapabilityValidationError> {
    let mut receives: BTreeSet<PortId> = BTreeSet::new();
    let mut emits: BTreeSet<PortId> = BTreeSet::new();

    for port in ports {
        let current: &mut BTreeSet<PortId> = match port.direction() {
            PortCapabilityDirection::Receive => &mut receives,
            PortCapabilityDirection::Emit => &mut emits,
        };

        if !current.insert(port.port_id().clone()) {
            return Err(CapabilityValidationError::DuplicatePortCapability {
                node_id: node_id.clone(),
                port_id: port.port_id().clone(),
                direction: port.direction(),
            });
        }
    }

    if let Some(port_id) = receives.intersection(&emits).next() {
        return Err(CapabilityValidationError::ConflictingPortDirection {
            node_id: node_id.clone(),
            port_id: port_id.clone(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node_id(value: &str) -> NodeId {
        NodeId::new(value).expect("valid node id")
    }

    fn port_id(value: &str) -> PortId {
        PortId::new(value).expect("valid port id")
    }

    fn receive(port: &str) -> PortCapability {
        PortCapability::new(port_id(port), PortCapabilityDirection::Receive)
    }

    fn emit(port: &str) -> PortCapability {
        PortCapability::new(port_id(port), PortCapabilityDirection::Emit)
    }

    #[test]
    fn valid_capabilities_keep_ports_and_effects_separate() {
        let capabilities: NodeCapabilities = NodeCapabilities::new(
            node_id("reader"),
            [receive("input"), emit("output")],
            [EffectCapability::FileSystemRead, EffectCapability::Clock],
        )
        .expect("valid capabilities");

        assert_eq!(capabilities.node_id().as_str(), "reader");
        assert!(capabilities.allows_effect(EffectCapability::FileSystemRead));
        assert!(capabilities.allows_port(&port_id("input"), PortCapabilityDirection::Receive));
        assert!(!capabilities.allows_effect(EffectCapability::ProcessSpawn));
    }

    #[test]
    fn duplicate_effects_are_rejected() {
        let err: CapabilityValidationError = NodeCapabilities::new(
            node_id("reader"),
            Vec::<PortCapability>::new(),
            [
                EffectCapability::FileSystemRead,
                EffectCapability::FileSystemRead,
            ],
        )
        .expect_err("duplicate effect must fail");

        assert_eq!(
            err,
            CapabilityValidationError::DuplicateEffect {
                node_id: node_id("reader"),
                effect: EffectCapability::FileSystemRead
            }
        );
    }

    #[test]
    fn duplicate_port_direction_is_rejected() {
        let err: CapabilityValidationError = NodeCapabilities::new(
            node_id("reader"),
            [receive("input"), receive("input")],
            Vec::<EffectCapability>::new(),
        )
        .expect_err("duplicate port direction must fail");

        assert_eq!(
            err,
            CapabilityValidationError::DuplicatePortCapability {
                node_id: node_id("reader"),
                port_id: port_id("input"),
                direction: PortCapabilityDirection::Receive
            }
        );
    }

    #[test]
    fn conflicting_port_directions_are_rejected() {
        let err: CapabilityValidationError = NodeCapabilities::new(
            node_id("router"),
            [receive("data"), emit("data")],
            Vec::<EffectCapability>::new(),
        )
        .expect_err("conflicting port direction must fail");

        assert_eq!(
            err,
            CapabilityValidationError::ConflictingPortDirection {
                node_id: node_id("router"),
                port_id: port_id("data")
            }
        );
    }
}
