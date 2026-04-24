//! Execution context and cancellation boundary types.
//!
//! ## Fragment: context-runtime-boundary
//!
//! This module keeps the runtime-facing context deliberately small: workflow
//! identity, node identity, execution identity, and cancellation state. That
//! is enough for the foundation beads to define what a node is executing
//! without prematurely choosing an async runtime, scheduler, or transport.
//!
//! ## Fragment: context-cancellation-shape
//!
//! Cancellation is represented as visible state rather than as an active
//! signaling primitive. That keeps the current boundary honest: the scaffolded
//! runtime can express that cancellation was requested, but it does not yet
//! claim to support cross-task interruption or supervisor-driven propagation.
//! When `asupersync` arrives, the signaling mechanism may change, but the node
//! surface should still read as "this execution may already be cancelled."
//!
//! ## Fragment: context-attempt-numbering
//!
//! Execution attempts are one-based on purpose. Retry counts are usually read
//! by humans in logs and diagnostics, and `attempt = 1` is less error-prone
//! than forcing every downstream consumer to translate from zero-based storage.

use std::num::NonZeroU32;

use conduit_types::{ExecutionId, NodeId, WorkflowId};

/// One-based attempt number for an execution boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExecutionAttempt(NonZeroU32);

impl ExecutionAttempt {
    /// Create an execution attempt from a one-based value.
    #[must_use]
    pub const fn new(value: NonZeroU32) -> Self {
        Self(value)
    }

    /// First attempt for a workflow execution.
    #[must_use]
    pub const fn first() -> Self {
        Self(NonZeroU32::MIN)
    }

    /// Return the one-based attempt number.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// Metadata that identifies one workflow execution attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionMetadata {
    execution_id: ExecutionId,
    attempt: ExecutionAttempt,
}

impl ExecutionMetadata {
    /// Create execution metadata for an explicit attempt.
    #[must_use]
    pub const fn new(execution_id: ExecutionId, attempt: ExecutionAttempt) -> Self {
        Self {
            execution_id,
            attempt,
        }
    }

    /// Create execution metadata for the first attempt.
    #[must_use]
    pub const fn first_attempt(execution_id: ExecutionId) -> Self {
        Self::new(execution_id, ExecutionAttempt::first())
    }

    /// Identifier for this workflow execution.
    #[must_use]
    pub const fn execution_id(&self) -> &ExecutionId {
        &self.execution_id
    }

    /// One-based attempt for this workflow execution.
    #[must_use]
    pub const fn attempt(&self) -> ExecutionAttempt {
        self.attempt
    }
}

/// Cancellation request visible at the runtime boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancellationRequest {
    reason: String,
}

impl CancellationRequest {
    /// Create a cancellation request with a human-readable reason.
    #[must_use]
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }

    /// Human-readable reason for cancellation.
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

/// Cancellation state carried by a node execution context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancellationState {
    /// No cancellation has been requested.
    Active,
    /// Cancellation has been requested at the runtime boundary.
    Requested(CancellationRequest),
}

impl CancellationState {
    /// Return whether cancellation has been requested.
    #[must_use]
    pub const fn is_requested(&self) -> bool {
        matches!(self, Self::Requested(_))
    }
}

/// Minimal execution context passed to runtime-managed nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeContext {
    workflow_id: WorkflowId,
    node_id: NodeId,
    execution: ExecutionMetadata,
    cancellation: CancellationState,
}

impl NodeContext {
    /// Create an active node context for one execution attempt.
    #[must_use]
    pub const fn new(
        workflow_id: WorkflowId,
        node_id: NodeId,
        execution: ExecutionMetadata,
    ) -> Self {
        Self {
            workflow_id,
            node_id,
            execution,
            cancellation: CancellationState::Active,
        }
    }

    /// Create a copy of this context with cancellation requested.
    #[must_use]
    pub fn with_cancellation(mut self, request: CancellationRequest) -> Self {
        self.cancellation = CancellationState::Requested(request);
        self
    }

    /// Workflow currently being executed.
    #[must_use]
    pub const fn workflow_id(&self) -> &WorkflowId {
        &self.workflow_id
    }

    /// Node currently being executed.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Execution metadata shared by nodes in the same run.
    #[must_use]
    pub const fn execution(&self) -> &ExecutionMetadata {
        &self.execution
    }

    /// Cancellation state visible to this node.
    #[must_use]
    pub const fn cancellation(&self) -> &CancellationState {
        &self.cancellation
    }

    /// Return whether cancellation has been requested.
    #[must_use]
    pub const fn is_cancelled(&self) -> bool {
        self.cancellation.is_requested()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn execution_id(value: &str) -> ExecutionId {
        ExecutionId::new(value).expect("valid execution id")
    }

    fn node_id(value: &str) -> NodeId {
        NodeId::new(value).expect("valid node id")
    }

    fn workflow_id(value: &str) -> WorkflowId {
        WorkflowId::new(value).expect("valid workflow id")
    }

    fn execution() -> ExecutionMetadata {
        ExecutionMetadata::first_attempt(execution_id("run-1"))
    }

    #[test]
    fn first_execution_attempt_is_one_based() {
        assert_eq!(ExecutionAttempt::first().get(), 1);
    }

    #[test]
    fn node_context_starts_active_and_can_carry_cancellation() {
        let ctx: NodeContext = NodeContext::new(workflow_id("flow"), node_id("node"), execution());

        assert!(!ctx.is_cancelled());
        assert!(matches!(ctx.cancellation(), CancellationState::Active));

        let cancelled: NodeContext =
            ctx.with_cancellation(CancellationRequest::new("shutdown requested"));

        assert!(cancelled.is_cancelled());
        assert!(matches!(
            cancelled.cancellation(),
            CancellationState::Requested(request) if request.reason() == "shutdown requested"
        ));
    }
}
