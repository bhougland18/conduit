//! Placeholder port handles for the executor boundary.
//!
//! ## Fragment: executor-port-staging
//!
//! The proposal wants the executor contract to be explicit about engine-owned
//! inputs and outputs, but the runtime does not yet have channel wiring or
//! backpressure machinery. These types therefore carry only the declared port
//! identities for a node. That keeps the public signature pointed in the right
//! direction without claiming that streaming transport already exists.

use conduit_types::PortId;

/// Declared input ports available to a node execution boundary.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PortsIn {
    port_ids: Vec<PortId>,
}

impl PortsIn {
    /// Create placeholder input handles from declared port identifiers.
    #[must_use]
    pub fn new(port_ids: impl Into<Vec<PortId>>) -> Self {
        Self {
            port_ids: port_ids.into(),
        }
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
}

/// Declared output ports available to a node execution boundary.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PortsOut {
    port_ids: Vec<PortId>,
}

impl PortsOut {
    /// Create placeholder output handles from declared port identifiers.
    #[must_use]
    pub fn new(port_ids: impl Into<Vec<PortId>>) -> Self {
        Self {
            port_ids: port_ids.into(),
        }
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn port_id(value: &str) -> PortId {
        PortId::new(value).expect("valid port id")
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
}
