//! Core traits and contracts for Conduit.

pub mod context;
pub mod lifecycle;
pub mod message;

use context::NodeContext;

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
