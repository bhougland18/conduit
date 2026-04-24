//! Core traits and contracts for Conduit.

pub mod capability;
pub mod context;
pub mod error;
pub mod lifecycle;
pub mod message;
pub mod ports;

use std::future::Future;

use context::NodeContext;
pub use error::{
    CancellationError, ConduitError, ErrorCode, ErrorVisibility, ExecutionError, LifecycleError,
    RetryDisposition, ValidationError,
};
pub use ports::{PortsIn, PortsOut};

/// Shared result type for runtime-facing APIs.
pub type Result<T> = std::result::Result<T, ConduitError>;

/// Async node interface for the first runtime skeleton.
///
/// The trait matches the proposal's intended boundary shape early, but the
/// current `PortsIn` and `PortsOut` values only expose declared port identity.
/// Later runtime beads will replace that placeholder wiring with live channel
/// handles without needing to revisit every executor signature first.
pub trait NodeExecutor: Sync {
    /// Future returned by one node execution attempt.
    type RunFuture<'a>: Future<Output = Result<()>> + Send + 'a
    where
        Self: 'a;

    /// Execute one runtime-managed node boundary.
    ///
    /// # Errors
    ///
    /// Returns an error if the node cannot complete the requested unit of
    /// work.
    fn run(&self, ctx: NodeContext, inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_>;
}
