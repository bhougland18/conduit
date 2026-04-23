//! Core traits and contracts for Conduit.

use conduit_types::{NodeId, WorkflowId};

/// Minimal execution context passed to runtime-managed nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeContext {
    /// Workflow currently being executed.
    pub workflow_id: WorkflowId,
    /// Node currently being executed.
    pub node_id: NodeId,
}

/// Shared result type for scaffolded APIs.
pub type Result<T> = std::result::Result<T, String>;

/// Minimal node interface for the first runtime skeleton.
pub trait NodeExecutor {
    /// Execute a unit of work for a node.
    ///
    /// # Errors
    ///
    /// Returns an error if the node cannot complete the requested unit of
    /// work.
    fn run(&self, ctx: &NodeContext) -> Result<()>;
}
