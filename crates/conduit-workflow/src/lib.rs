//! External workflow definitions and validation entrypoints.

use conduit_types::{IdentifierError, NodeId, WorkflowId};

/// Parsed workflow definition used by the scaffold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowDefinition {
    /// Workflow identifier.
    pub id: WorkflowId,
    /// Declared nodes in execution order.
    pub nodes: Vec<NodeId>,
}

impl WorkflowDefinition {
    /// Create a placeholder workflow with no nodes.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow identifier is invalid.
    pub fn empty(name: impl Into<String>) -> Result<Self, IdentifierError> {
        Ok(Self {
            id: WorkflowId::new(name)?,
            nodes: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_workflow_uses_valid_identifier() {
        let workflow = WorkflowDefinition::empty("conduit-scaffold").expect("valid id");

        assert_eq!(workflow.id.as_str(), "conduit-scaffold");
        assert!(workflow.nodes.is_empty());
    }

    #[test]
    fn empty_workflow_rejects_invalid_identifier() {
        let err = WorkflowDefinition::empty("bad workflow").expect_err("whitespace must fail");
        assert_eq!(
            err,
            IdentifierError::Whitespace {
                kind: conduit_types::IdentifierKind::Workflow
            }
        );
    }
}
