//! Core traits and contracts for Conduit.

pub mod capability;
pub mod context;
pub mod error;
pub mod lifecycle;
pub mod message;

use context::NodeContext;
pub use error::{
    CancellationError, ConduitError, ErrorCode, ErrorVisibility, ExecutionError, LifecycleError,
    RetryDisposition, ValidationError,
};

/// Shared result type for runtime-facing APIs.
pub type Result<T> = std::result::Result<T, ConduitError>;

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
