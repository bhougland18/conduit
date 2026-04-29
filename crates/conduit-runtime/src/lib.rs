//! Runtime mechanics such as supervision and backpressure primitives.
//!
//! ## Fragment: runtime-asupersync-bootstrap
//!
//! `conduit-runtime` now owns a real runtime substrate, but only at the level
//! the current node interface can honestly support. The wrapper below uses
//! `asupersync::runtime::RuntimeBuilder` to provide a task-tree-backed entry
//! point for one node execution at a time. It deliberately does not claim
//! workflow scheduling, channel wiring, or full FBP semantics yet.
//!
//! ## Fragment: runtime-asupersync-boundary
//!
//! `asupersync` is the runtime substrate, not the public FBP model. Conduit
//! owns graph validation, node contracts, port handles, metadata, capability
//! descriptors, and introspection. Runtime adapters may use `asupersync`
//! contexts, bounded channels, cancellation, and task handles internally, but
//! those types should not leak through `NodeExecutor`, `NodeContext`,
//! `PortsIn`, or `PortsOut` unless a later bead explicitly revisits the
//! boundary.
//!
//! ## Fragment: runtime-test-determinism
//!
//! Production construction intentionally keeps the default `asupersync`
//! builder. Tests that assert ordering, cancellation, or failure propagation
//! should use the deterministic current-thread constructor so those checks are
//! about Conduit behavior rather than host scheduler timing.

use asupersync::runtime::{Runtime, RuntimeBuilder};
use conduit_core::{
    CancellationError, CancellationHandle, ConduitError, NodeExecutor, PortsIn, PortsOut, Result,
    context::{CancellationState, NodeContext},
    lifecycle::{LifecycleEvent, LifecycleEventKind, LifecycleHook, NoopLifecycleHook},
    metadata::{MetadataRecord, MetadataSink, NoopMetadataSink},
};
use std::future::Future;
use std::sync::Arc;

/// Narrow runtime wrapper backed by `asupersync`.
pub struct AsupersyncRuntime {
    runtime: Runtime,
}

impl AsupersyncRuntime {
    /// Build the current `asupersync`-backed runtime wrapper.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying runtime cannot be constructed.
    pub fn new() -> Result<Self> {
        Self::from_builder(RuntimeBuilder::new())
    }

    fn from_builder(builder: RuntimeBuilder) -> Result<Self> {
        let runtime: Runtime = match builder.build() {
            Ok(runtime) => runtime,
            Err(err) => {
                return Err(ConduitError::execution(format!(
                    "failed to build asupersync runtime: {err}"
                )));
            }
        };

        Ok(Self { runtime })
    }

    /// Run a future on the owned runtime.
    pub fn block_on<T>(&self, future: impl Future<Output = T>) -> T {
        self.runtime.block_on(future)
    }

    #[cfg(test)]
    fn deterministic_for_tests() -> Result<Self> {
        Self::from_builder(RuntimeBuilder::current_thread().poll_budget(1))
    }

    /// Create a Conduit-owned cancellation handle for runtime-managed contexts.
    #[must_use]
    pub fn cancellation_handle() -> CancellationHandle {
        CancellationHandle::new()
    }

    /// Execute one node on the owned runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if cancellation was already requested, lifecycle
    /// observation fails, or the node executor reports one.
    pub fn run_node<E: NodeExecutor + ?Sized>(
        &self,
        node: &E,
        ctx: NodeContext,
        inputs: PortsIn,
        outputs: PortsOut,
    ) -> Result<()> {
        if let Some(err) = cancellation_error(&ctx) {
            return Err(err);
        }

        self.runtime.block_on(run_node(node, ctx, inputs, outputs))
    }

    /// Execute one node with an externally drivable cancellation handle.
    ///
    /// # Errors
    ///
    /// Returns an error if cancellation was already requested or the node
    /// executor reports one.
    pub fn run_node_with_cancellation_handle<E: NodeExecutor + ?Sized>(
        &self,
        node: &E,
        ctx: NodeContext,
        inputs: PortsIn,
        outputs: PortsOut,
        cancellation: &CancellationHandle,
    ) -> Result<()> {
        let ctx: NodeContext = ctx.with_cancellation_token(cancellation.token());
        if let Some(err) = cancellation_error(&ctx) {
            return Err(err);
        }

        self.runtime.block_on(run_node(node, ctx, inputs, outputs))
    }

    /// Execute one node on the owned runtime and collect metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if cancellation was already requested, metadata
    /// collection fails, lifecycle observation fails, or the node executor
    /// reports one.
    pub fn run_node_with_metadata_sink<E, M>(
        &self,
        node: &E,
        ctx: NodeContext,
        inputs: PortsIn,
        outputs: PortsOut,
        metadata_sink: Arc<M>,
    ) -> Result<()>
    where
        E: NodeExecutor + ?Sized,
        M: MetadataSink + 'static,
    {
        if let Some(err) = cancellation_error(&ctx) {
            return Err(err);
        }

        self.runtime.block_on(run_node_with_metadata_sink(
            node,
            ctx,
            inputs,
            outputs,
            metadata_sink,
        ))
    }
}

/// Execute a single node through the runtime boundary.
///
/// # Errors
///
/// Returns an error if lifecycle observation fails or the node executor
/// reports one.
pub async fn run_node<E: NodeExecutor + ?Sized>(
    node: &E,
    ctx: NodeContext,
    inputs: PortsIn,
    outputs: PortsOut,
) -> Result<()> {
    run_node_with_observers(
        node,
        ctx,
        inputs,
        outputs,
        &NoopLifecycleHook,
        Arc::new(NoopMetadataSink),
    )
    .await
}

/// Execute a single node through the runtime boundary and report lifecycle events.
///
/// # Errors
///
/// Returns an error if lifecycle observation fails or the node executor
/// reports one.
pub async fn run_node_with_hook<E, H>(
    node: &E,
    ctx: NodeContext,
    inputs: PortsIn,
    outputs: PortsOut,
    hook: &H,
) -> Result<()>
where
    E: NodeExecutor + ?Sized,
    H: LifecycleHook + ?Sized,
{
    run_node_with_observers(node, ctx, inputs, outputs, hook, Arc::new(NoopMetadataSink)).await
}

/// Execute a single node through the runtime boundary and collect metadata.
///
/// # Errors
///
/// Returns an error if metadata collection fails or the node executor reports
/// one.
pub async fn run_node_with_metadata_sink<E, M>(
    node: &E,
    ctx: NodeContext,
    inputs: PortsIn,
    outputs: PortsOut,
    metadata_sink: Arc<M>,
) -> Result<()>
where
    E: NodeExecutor + ?Sized,
    M: MetadataSink + 'static,
{
    run_node_with_observers(
        node,
        ctx,
        inputs,
        outputs,
        &NoopLifecycleHook,
        metadata_sink,
    )
    .await
}

/// Execute a node and report both lifecycle and metadata observations.
///
/// # Errors
///
/// Returns an error if start observation fails, terminal observation fails
/// after successful execution, or the node executor reports one.
pub async fn run_node_with_observers<E, H, M>(
    node: &E,
    ctx: NodeContext,
    inputs: PortsIn,
    outputs: PortsOut,
    hook: &H,
    metadata_sink: Arc<M>,
) -> Result<()>
where
    E: NodeExecutor + ?Sized,
    H: LifecycleHook + ?Sized,
    M: MetadataSink + 'static,
{
    let metadata_sink: Arc<dyn MetadataSink + Send + Sync> = metadata_sink.clone();
    let inputs: PortsIn = inputs.with_metadata_sink(metadata_sink.clone());
    let outputs: PortsOut = outputs.with_metadata_sink(metadata_sink.clone());
    observe_lifecycle(
        hook,
        metadata_sink.as_ref(),
        LifecycleEventKind::NodeStarted,
        ctx.clone(),
    )?;

    let result: Result<()> = node.run(ctx.clone(), inputs, outputs).await;
    let kind: LifecycleEventKind = if result.is_ok() {
        LifecycleEventKind::NodeCompleted
    } else {
        LifecycleEventKind::NodeFailed
    };
    let terminal_observation: Result<()> =
        observe_lifecycle(hook, metadata_sink.as_ref(), kind, ctx);

    match (result, terminal_observation) {
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(err)) | (Err(err), _) => Err(err),
    }
}

fn observe_lifecycle<H, M>(
    hook: &H,
    metadata_sink: &M,
    kind: LifecycleEventKind,
    ctx: NodeContext,
) -> Result<()>
where
    H: LifecycleHook + ?Sized,
    M: MetadataSink + ?Sized,
{
    let event: LifecycleEvent = LifecycleEvent::new(kind, ctx);
    metadata_sink.record(&MetadataRecord::Lifecycle(event.clone()))?;
    hook.observe(&event)
}

fn cancellation_error(ctx: &NodeContext) -> Option<ConduitError> {
    match ctx.cancellation() {
        CancellationState::Active => None,
        CancellationState::Requested(request) => {
            Some(ConduitError::from(CancellationError::new(request.reason())))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;

    use conduit_core::{
        CancellationError, ConduitError, LifecycleError, MetadataError,
        context::CancellationRequest, lifecycle::LifecycleEventKind,
    };
    use conduit_test_kit::{
        FailingExecutor, RecordingExecutor, execution_metadata, node_id, workflow_id,
    };
    use futures::executor::block_on;

    #[derive(Debug, Default)]
    struct RecordingHook {
        events: Mutex<Vec<LifecycleEventKind>>,
    }

    impl RecordingHook {
        fn recorded(&self) -> Vec<LifecycleEventKind> {
            self.events
                .lock()
                .expect("recording hook lock should not be poisoned")
                .clone()
        }
    }

    impl LifecycleHook for RecordingHook {
        fn observe(&self, event: &LifecycleEvent) -> Result<()> {
            self.events
                .lock()
                .expect("recording hook lock should not be poisoned")
                .push(event.kind());
            Ok(())
        }
    }

    #[derive(Debug)]
    struct FailingHook;

    impl LifecycleHook for FailingHook {
        fn observe(&self, _event: &LifecycleEvent) -> Result<()> {
            Err(ConduitError::from(LifecycleError::new("hook failed")))
        }
    }

    #[derive(Debug, Default)]
    struct RecordingMetadataSink {
        events: Mutex<Vec<LifecycleEventKind>>,
    }

    impl RecordingMetadataSink {
        fn recorded(&self) -> Vec<LifecycleEventKind> {
            self.events
                .lock()
                .expect("metadata sink lock should not be poisoned")
                .clone()
        }
    }

    impl MetadataSink for RecordingMetadataSink {
        fn record(&self, record: &MetadataRecord) -> Result<()> {
            if let MetadataRecord::Lifecycle(event) = record {
                self.events
                    .lock()
                    .expect("metadata sink lock should not be poisoned")
                    .push(event.kind());
            }
            Ok(())
        }
    }

    #[derive(Debug)]
    struct FailingMetadataSink;

    impl MetadataSink for FailingMetadataSink {
        fn record(&self, _record: &MetadataRecord) -> Result<()> {
            Err(ConduitError::from(MetadataError::new(
                "metadata sink failed",
            )))
        }
    }

    #[derive(Debug)]
    struct FailingOnNodeFailedMetadataSink;

    impl MetadataSink for FailingOnNodeFailedMetadataSink {
        fn record(&self, record: &MetadataRecord) -> Result<()> {
            if matches!(
                record,
                MetadataRecord::Lifecycle(event)
                    if event.kind() == LifecycleEventKind::NodeFailed
            ) {
                return Err(ConduitError::from(MetadataError::new(
                    "terminal metadata failed",
                )));
            }
            Ok(())
        }
    }

    #[derive(Debug)]
    struct CancellingExecutor {
        handle: CancellationHandle,
        observations: Mutex<Vec<bool>>,
    }

    impl CancellingExecutor {
        fn new(handle: CancellationHandle) -> Self {
            Self {
                handle,
                observations: Mutex::new(Vec::new()),
            }
        }

        fn record(&self, value: bool) {
            self.observations
                .lock()
                .expect("cancelling executor observations lock should not be poisoned")
                .push(value);
        }

        fn observations(&self) -> Vec<bool> {
            self.observations
                .lock()
                .expect("cancelling executor observations lock should not be poisoned")
                .clone()
        }
    }

    impl NodeExecutor for CancellingExecutor {
        type RunFuture<'a> = Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

        fn run(
            &self,
            ctx: NodeContext,
            _inputs: PortsIn,
            _outputs: PortsOut,
        ) -> Self::RunFuture<'_> {
            Box::pin(async move {
                self.record(ctx.is_cancelled());
                let _first_request: bool = self
                    .handle
                    .cancel(CancellationRequest::new("runtime supervisor stopped node"));
                self.record(ctx.is_cancelled());
                Ok(())
            })
        }
    }

    fn context() -> NodeContext {
        NodeContext::new(
            workflow_id("flow"),
            node_id("node"),
            execution_metadata("run-1"),
        )
    }

    fn deterministic_runtime() -> AsupersyncRuntime {
        AsupersyncRuntime::deterministic_for_tests().expect("deterministic runtime should build")
    }

    #[test]
    fn deterministic_runtime_for_tests_uses_current_thread_config() {
        let runtime: AsupersyncRuntime = deterministic_runtime();

        assert_eq!(runtime.runtime.config().worker_threads, 1);
        assert_eq!(runtime.runtime.config().poll_budget, 1);
    }

    #[test]
    fn production_runtime_builder_defaults_remain_separate_from_test_runtime() {
        let production: AsupersyncRuntime =
            AsupersyncRuntime::new().expect("production runtime should build");
        let deterministic: AsupersyncRuntime = deterministic_runtime();

        assert_eq!(production.runtime.config().poll_budget, 128);
        assert_eq!(deterministic.runtime.config().worker_threads, 1);
        assert_eq!(deterministic.runtime.config().poll_budget, 1);
    }

    #[test]
    fn asupersync_runtime_runs_one_node() {
        let runtime: AsupersyncRuntime = deterministic_runtime();
        let executor: RecordingExecutor = RecordingExecutor::default();

        runtime
            .run_node(
                &executor,
                context(),
                PortsIn::default(),
                PortsOut::default(),
            )
            .expect("execution should succeed");

        assert_eq!(executor.visited_node_names(), vec!["node"]);
    }

    #[test]
    fn asupersync_runtime_preserves_executor_failures() {
        let runtime: AsupersyncRuntime = deterministic_runtime();
        let executor: FailingExecutor = FailingExecutor::execution("boom");

        let err: ConduitError = runtime
            .run_node(
                &executor,
                context(),
                PortsIn::default(),
                PortsOut::default(),
            )
            .expect_err("execution should fail");

        assert_eq!(err, ConduitError::execution("boom"));
    }

    #[test]
    fn asupersync_runtime_rejects_pre_cancelled_contexts() {
        let runtime: AsupersyncRuntime = deterministic_runtime();
        let executor: RecordingExecutor = RecordingExecutor::default();
        let ctx: NodeContext =
            context().with_cancellation(CancellationRequest::new("shutdown requested"));

        let err: ConduitError = runtime
            .run_node(&executor, ctx, PortsIn::default(), PortsOut::default())
            .expect_err("cancelled execution should not run");

        assert_eq!(
            err,
            ConduitError::from(CancellationError::new("shutdown requested"))
        );
        assert!(executor.visited_contexts().is_empty());
    }

    #[test]
    fn asupersync_runtime_cancellation_handle_is_visible_inside_running_node() {
        let runtime: AsupersyncRuntime = deterministic_runtime();
        let handle: CancellationHandle = AsupersyncRuntime::cancellation_handle();
        let executor: CancellingExecutor = CancellingExecutor::new(handle.clone());

        runtime
            .run_node_with_cancellation_handle(
                &executor,
                context(),
                PortsIn::default(),
                PortsOut::default(),
                &handle,
            )
            .expect("execution should succeed");

        assert_eq!(executor.observations(), vec![false, true]);
        assert!(handle.is_cancelled());
    }

    #[test]
    fn asupersync_runtime_rejects_child_context_after_shared_cancellation() {
        let runtime: AsupersyncRuntime = deterministic_runtime();
        let handle: CancellationHandle = AsupersyncRuntime::cancellation_handle();
        let canceller: CancellingExecutor = CancellingExecutor::new(handle.clone());
        let child: RecordingExecutor = RecordingExecutor::default();

        runtime
            .run_node_with_cancellation_handle(
                &canceller,
                context(),
                PortsIn::default(),
                PortsOut::default(),
                &handle,
            )
            .expect("first execution should request cancellation");
        let err: ConduitError = runtime
            .run_node_with_cancellation_handle(
                &child,
                context(),
                PortsIn::default(),
                PortsOut::default(),
                &handle,
            )
            .expect_err("shared cancellation should reject child execution");

        assert_eq!(
            err,
            ConduitError::from(CancellationError::new("runtime supervisor stopped node"))
        );
        assert!(child.visited_contexts().is_empty());
    }

    #[test]
    fn run_node_with_hook_emits_started_then_completed() {
        let executor: RecordingExecutor = RecordingExecutor::default();
        let hook: RecordingHook = RecordingHook::default();

        block_on(run_node_with_hook(
            &executor,
            context(),
            PortsIn::default(),
            PortsOut::default(),
            &hook,
        ))
        .expect("execution should succeed");

        assert_eq!(
            hook.recorded(),
            vec![
                LifecycleEventKind::NodeStarted,
                LifecycleEventKind::NodeCompleted,
            ]
        );
    }

    #[test]
    fn run_node_with_hook_emits_started_then_failed_and_preserves_executor_error() {
        let executor: FailingExecutor = FailingExecutor::execution("boom");
        let hook: RecordingHook = RecordingHook::default();

        let err: ConduitError = block_on(run_node_with_hook(
            &executor,
            context(),
            PortsIn::default(),
            PortsOut::default(),
            &hook,
        ))
        .expect_err("execution should fail");

        assert_eq!(
            hook.recorded(),
            vec![
                LifecycleEventKind::NodeStarted,
                LifecycleEventKind::NodeFailed
            ]
        );
        assert_eq!(err, ConduitError::execution("boom"));
    }

    #[test]
    fn run_node_provides_noop_default_hook() {
        let executor: RecordingExecutor = RecordingExecutor::default();

        block_on(run_node(
            &executor,
            context(),
            PortsIn::default(),
            PortsOut::default(),
        ))
        .expect("execution should succeed");
    }

    #[test]
    fn run_node_with_hook_propagates_hook_failures() {
        let executor: RecordingExecutor = RecordingExecutor::default();
        let err: ConduitError = block_on(run_node_with_hook(
            &executor,
            context(),
            PortsIn::default(),
            PortsOut::default(),
            &FailingHook,
        ))
        .expect_err("hook failure should surface");

        assert_eq!(err, ConduitError::from(LifecycleError::new("hook failed")));
    }

    #[test]
    fn run_node_with_metadata_sink_records_lifecycle_events() {
        let executor: RecordingExecutor = RecordingExecutor::default();
        let sink: Arc<RecordingMetadataSink> = Arc::new(RecordingMetadataSink::default());

        block_on(run_node_with_metadata_sink(
            &executor,
            context(),
            PortsIn::default(),
            PortsOut::default(),
            sink.clone(),
        ))
        .expect("execution should succeed");

        assert_eq!(
            sink.recorded(),
            vec![
                LifecycleEventKind::NodeStarted,
                LifecycleEventKind::NodeCompleted,
            ]
        );
    }

    #[test]
    fn asupersync_runtime_can_collect_metadata() {
        let runtime: AsupersyncRuntime = deterministic_runtime();
        let executor: RecordingExecutor = RecordingExecutor::default();
        let sink: Arc<RecordingMetadataSink> = Arc::new(RecordingMetadataSink::default());

        runtime
            .run_node_with_metadata_sink(
                &executor,
                context(),
                PortsIn::default(),
                PortsOut::default(),
                sink.clone(),
            )
            .expect("execution should succeed");

        assert_eq!(
            sink.recorded(),
            vec![
                LifecycleEventKind::NodeStarted,
                LifecycleEventKind::NodeCompleted,
            ]
        );
    }

    #[test]
    fn run_node_with_metadata_sink_propagates_start_collection_failures() {
        let executor: RecordingExecutor = RecordingExecutor::default();

        let err: ConduitError = block_on(run_node_with_metadata_sink(
            &executor,
            context(),
            PortsIn::default(),
            PortsOut::default(),
            Arc::new(FailingMetadataSink),
        ))
        .expect_err("metadata failure should surface");

        assert_eq!(
            err,
            ConduitError::from(MetadataError::new("metadata sink failed"))
        );
        assert!(executor.visited_contexts().is_empty());
    }

    #[test]
    fn executor_failure_takes_precedence_over_terminal_metadata_failure() {
        let executor: FailingExecutor = FailingExecutor::execution("boom");

        let err: ConduitError = block_on(run_node_with_metadata_sink(
            &executor,
            context(),
            PortsIn::default(),
            PortsOut::default(),
            Arc::new(FailingOnNodeFailedMetadataSink),
        ))
        .expect_err("executor failure should surface");

        assert_eq!(err, ConduitError::execution("boom"));
    }
}
