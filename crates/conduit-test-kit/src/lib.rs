//! Reusable builders, test doubles, and property strategies for Conduit tests.

use std::{
    future::{Ready, ready},
    num::NonZeroUsize,
    sync::Mutex,
};

use conduit_core::{
    ConduitError, NodeExecutor, PortsIn, PortsOut, Result,
    context::{ExecutionMetadata, NodeContext},
};
use conduit_types::{ExecutionId, NodeId, PortId, WorkflowId};
use conduit_workflow::{EdgeDefinition, EdgeEndpoint, NodeDefinition, WorkflowDefinition};
use proptest::{prelude::*, sample::select};

const IDENTIFIER_ALPHABET: [char; 66] = [
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's',
    't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L',
    'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3', '4',
    '5', '6', '7', '8', '9', '-', '_', '.', '/',
];

/// Create a workflow identifier for tests.
///
/// # Panics
///
/// Panics if `value` is not a valid Conduit workflow identifier.
#[must_use]
pub fn workflow_id(value: &str) -> WorkflowId {
    WorkflowId::new(value).expect("test workflow id must be valid")
}

/// Create a node identifier for tests.
///
/// # Panics
///
/// Panics if `value` is not a valid Conduit node identifier.
#[must_use]
pub fn node_id(value: &str) -> NodeId {
    NodeId::new(value).expect("test node id must be valid")
}

/// Create a port identifier for tests.
///
/// # Panics
///
/// Panics if `value` is not a valid Conduit port identifier.
#[must_use]
pub fn port_id(value: &str) -> PortId {
    PortId::new(value).expect("test port id must be valid")
}

/// Create execution metadata for the first attempt of a test run.
///
/// # Panics
///
/// Panics if `value` is not a valid Conduit execution identifier.
#[must_use]
pub fn execution_metadata(value: &str) -> ExecutionMetadata {
    ExecutionMetadata::first_attempt(
        ExecutionId::new(value).expect("test execution id must be valid"),
    )
}

/// Builder for validated workflow node definitions.
#[derive(Debug, Clone)]
pub struct NodeBuilder {
    id: NodeId,
    input_ports: Vec<PortId>,
    output_ports: Vec<PortId>,
}

impl NodeBuilder {
    /// Start a node builder for one node identifier.
    #[must_use]
    pub fn new(id: &str) -> Self {
        Self {
            id: node_id(id),
            input_ports: Vec::new(),
            output_ports: Vec::new(),
        }
    }

    /// Add an input port to the node.
    #[must_use]
    pub fn input(mut self, id: &str) -> Self {
        self.input_ports.push(port_id(id));
        self
    }

    /// Add an output port to the node.
    #[must_use]
    pub fn output(mut self, id: &str) -> Self {
        self.output_ports.push(port_id(id));
        self
    }

    /// Build a validated node definition.
    ///
    /// # Panics
    ///
    /// Panics if the configured ports violate node-definition invariants.
    #[must_use]
    pub fn build(self) -> NodeDefinition {
        NodeDefinition::new(self.id, self.input_ports, self.output_ports)
            .expect("test node definition must be valid")
    }
}

/// Builder for validated workflow definitions.
#[derive(Debug, Clone)]
pub struct WorkflowBuilder {
    id: WorkflowId,
    nodes: Vec<NodeDefinition>,
    edges: Vec<EdgeDefinition>,
}

impl WorkflowBuilder {
    /// Start a workflow builder for one workflow identifier.
    #[must_use]
    pub fn new(id: &str) -> Self {
        Self {
            id: workflow_id(id),
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    /// Add a node definition to the workflow.
    #[must_use]
    pub fn node(mut self, node: NodeDefinition) -> Self {
        self.nodes.push(node);
        self
    }

    /// Add a validated edge between two node ports.
    #[must_use]
    pub fn edge(
        mut self,
        source_node: &str,
        source_port: &str,
        target_node: &str,
        target_port: &str,
    ) -> Self {
        self.edges.push(EdgeDefinition::new(
            EdgeEndpoint::new(node_id(source_node), port_id(source_port)),
            EdgeEndpoint::new(node_id(target_node), port_id(target_port)),
        ));
        self
    }

    /// Add a validated edge between two node ports with an explicit capacity.
    #[must_use]
    pub fn edge_with_capacity(
        mut self,
        source_node: &str,
        source_port: &str,
        target_node: &str,
        target_port: &str,
        capacity: NonZeroUsize,
    ) -> Self {
        self.edges.push(EdgeDefinition::with_capacity(
            EdgeEndpoint::new(node_id(source_node), port_id(source_port)),
            EdgeEndpoint::new(node_id(target_node), port_id(target_port)),
            capacity,
        ));
        self
    }

    /// Build a validated workflow definition.
    ///
    /// # Panics
    ///
    /// Panics if the configured workflow graph violates structural invariants.
    #[must_use]
    pub fn build(self) -> WorkflowDefinition {
        WorkflowDefinition::from_parts(self.id, self.nodes, self.edges)
            .expect("test workflow definition must be valid")
    }
}

/// Executor test double that records the visited node order.
#[derive(Default)]
pub struct RecordingExecutor {
    contexts: Mutex<Vec<NodeContext>>,
    inputs: Mutex<Vec<PortsIn>>,
    outputs: Mutex<Vec<PortsOut>>,
}

impl RecordingExecutor {
    /// Return the visited node contexts in call order.
    ///
    /// # Panics
    ///
    /// Panics if the internal recording lock has been poisoned by an earlier
    /// failing test thread.
    #[must_use]
    pub fn visited_contexts(&self) -> Vec<NodeContext> {
        self.contexts
            .lock()
            .expect("recording executor contexts lock should not be poisoned")
            .clone()
    }

    /// Return the visited node identifiers in call order.
    #[must_use]
    pub fn visited_nodes(&self) -> Vec<NodeId> {
        self.visited_contexts()
            .into_iter()
            .map(|ctx: NodeContext| ctx.node_id().clone())
            .collect()
    }

    /// Return the visited node names in call order.
    #[must_use]
    pub fn visited_node_names(&self) -> Vec<String> {
        self.visited_nodes()
            .into_iter()
            .map(|node: NodeId| node.to_string())
            .collect()
    }

    /// Return the visited input-port names in call order.
    ///
    /// # Panics
    ///
    /// Panics if the internal recording lock has been poisoned by an earlier
    /// failing test thread.
    #[must_use]
    pub fn visited_input_port_names(&self) -> Vec<Vec<String>> {
        self.inputs
            .lock()
            .expect("recording executor inputs lock should not be poisoned")
            .iter()
            .map(|ports: &PortsIn| ports.port_ids().iter().map(ToString::to_string).collect())
            .collect()
    }

    /// Return the visited output-port names in call order.
    ///
    /// # Panics
    ///
    /// Panics if the internal recording lock has been poisoned by an earlier
    /// failing test thread.
    #[must_use]
    pub fn visited_output_port_names(&self) -> Vec<Vec<String>> {
        self.outputs
            .lock()
            .expect("recording executor outputs lock should not be poisoned")
            .iter()
            .map(|ports: &PortsOut| ports.port_ids().iter().map(ToString::to_string).collect())
            .collect()
    }
}

impl NodeExecutor for RecordingExecutor {
    type RunFuture<'a> = Ready<Result<()>>;

    fn run(&self, ctx: NodeContext, inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
        self.contexts
            .lock()
            .expect("recording executor contexts lock should not be poisoned")
            .push(ctx);
        self.inputs
            .lock()
            .expect("recording executor inputs lock should not be poisoned")
            .push(inputs);
        self.outputs
            .lock()
            .expect("recording executor outputs lock should not be poisoned")
            .push(outputs);
        ready(Ok(()))
    }
}

/// Executor test double that always returns the configured failure.
#[derive(Debug, Clone)]
pub struct FailingExecutor {
    error: ConduitError,
}

impl FailingExecutor {
    /// Create a failing executor with an explicit Conduit error.
    #[must_use]
    pub const fn new(error: ConduitError) -> Self {
        Self { error }
    }

    /// Create a failing executor that reports an execution failure.
    #[must_use]
    pub fn execution(message: impl Into<String>) -> Self {
        Self::new(ConduitError::execution(message))
    }
}

impl NodeExecutor for FailingExecutor {
    type RunFuture<'a> = Ready<Result<()>>;

    fn run(&self, _ctx: NodeContext, _inputs: PortsIn, _outputs: PortsOut) -> Self::RunFuture<'_> {
        ready(Err(self.error.clone()))
    }
}

/// Strategy for identifiers that satisfy Conduit's current validation rules.
pub fn valid_identifier_strategy() -> impl Strategy<Value = String> {
    prop::collection::vec(select(&IDENTIFIER_ALPHABET), 1..16)
        .prop_map(|chars: Vec<char>| chars.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use conduit_core::ErrorCode;

    #[test]
    fn workflow_builder_builds_valid_linear_workflow() {
        let workflow: WorkflowDefinition = WorkflowBuilder::new("flow")
            .node(NodeBuilder::new("first").output("out").build())
            .node(NodeBuilder::new("second").input("in").build())
            .edge("first", "out", "second", "in")
            .build();

        assert_eq!(workflow.nodes().len(), 2);
        assert_eq!(workflow.edges().len(), 1);
    }

    #[test]
    fn failing_executor_returns_configured_error() {
        let executor: FailingExecutor = FailingExecutor::execution("boom");
        let ctx: NodeContext = NodeContext::new(
            workflow_id("flow"),
            node_id("node"),
            execution_metadata("run-1"),
        );
        let err: ConduitError = executor
            .run(ctx, PortsIn::default(), PortsOut::default())
            .into_inner()
            .expect_err("executor must fail");

        assert_eq!(err.code(), ErrorCode::NodeExecutionFailed);
    }
}
