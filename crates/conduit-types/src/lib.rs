//! Shared domain types for Conduit.

use std::error::Error;
use std::fmt;
use std::str::FromStr;

/// Kinds of opaque identifiers used by Conduit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentifierKind {
    /// A workflow identifier.
    Workflow,
    /// A node identifier within a workflow graph.
    Node,
    /// A port identifier on a node.
    Port,
}

impl IdentifierKind {
    const fn label(self) -> &'static str {
        match self {
            Self::Workflow => "workflow id",
            Self::Node => "node id",
            Self::Port => "port id",
        }
    }
}

/// Error returned when an identifier is malformed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentifierError {
    /// The identifier was empty or only whitespace.
    Empty {
        /// Kind of identifier that failed validation.
        kind: IdentifierKind,
    },
    /// The identifier contained whitespace.
    Whitespace {
        /// Kind of identifier that failed validation.
        kind: IdentifierKind,
    },
    /// The identifier contained a control character.
    Control {
        /// Kind of identifier that failed validation.
        kind: IdentifierKind,
    },
}

impl fmt::Display for IdentifierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty { kind } => write!(f, "{} must not be empty", kind.label()),
            Self::Whitespace { kind } => write!(f, "{} must not contain whitespace", kind.label()),
            Self::Control { kind } => {
                write!(f, "{} must not contain control characters", kind.label())
            }
        }
    }
}

impl Error for IdentifierError {}

fn validate_identifier(kind: IdentifierKind, value: &str) -> Result<(), IdentifierError> {
    if value.trim().is_empty() {
        return Err(IdentifierError::Empty { kind });
    }

    if value.chars().any(char::is_whitespace) {
        return Err(IdentifierError::Whitespace { kind });
    }

    if value.chars().any(char::is_control) {
        return Err(IdentifierError::Control { kind });
    }

    Ok(())
}

macro_rules! id_type {
    ($name:ident, $kind:expr, $docs:literal) => {
        #[doc = $docs]
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(String);

        impl $name {
            /// Create a validated identifier.
            ///
            /// # Errors
            ///
            /// Returns an error if the value is empty, contains whitespace, or
            /// contains a control character.
            pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
                let value = value.into();
                validate_identifier($kind, &value)?;
                Ok(Self(value))
            }

            /// View the identifier as a string slice.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = IdentifierError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::new(s)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

id_type!(
    WorkflowId,
    IdentifierKind::Workflow,
    "Stable workflow identifier."
);
id_type!(
    NodeId,
    IdentifierKind::Node,
    "Stable node identifier inside a workflow graph."
);
id_type!(
    PortId,
    IdentifierKind::Port,
    "Stable port identifier on a node."
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_id_rejects_empty_values() {
        let err = WorkflowId::new("").expect_err("empty identifiers must fail");
        assert_eq!(
            err,
            IdentifierError::Empty {
                kind: IdentifierKind::Workflow
            }
        );
    }

    #[test]
    fn node_id_rejects_whitespace() {
        let err = NodeId::new("node one").expect_err("whitespace identifiers must fail");
        assert_eq!(
            err,
            IdentifierError::Whitespace {
                kind: IdentifierKind::Node
            }
        );
    }

    #[test]
    fn port_id_round_trips_through_display_and_parse() {
        let id = PortId::new("out-1").expect("valid identifier");
        let parsed = PortId::from_str(id.as_str()).expect("round-trip should succeed");

        assert_eq!(id, parsed);
        assert_eq!(id.to_string(), "out-1");
        assert_eq!(id.as_ref(), "out-1");
    }

    #[test]
    fn identifiers_reject_control_characters() {
        let err = WorkflowId::new("flow\u{0007}one").expect_err("control characters must fail");
        assert!(matches!(
            err,
            IdentifierError::Control {
                kind: IdentifierKind::Workflow
            }
        ));
    }
}
