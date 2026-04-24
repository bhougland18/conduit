//! External workflow definitions and validation entrypoints.
//!
//! This crate owns the static workflow graph shape. It validates structural
//! facts that must be true before any runtime can reason about execution:
//! nodes are uniquely named, ports are uniquely named within a node, and edges
//! connect declared output ports to declared input ports. Runtime concerns such
//! as scheduling policy, cycles, payload compatibility, cancellation, and
//! backpressure are intentionally left to later layers.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

use conduit_types::{IdentifierError, NodeId, PortId, WorkflowId};

/// Direction of a port in a node's static topology.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortDirection {
    /// A port that receives data or control from an upstream node.
    Input,
    /// A port that emits data or control to a downstream node.
    Output,
}

impl PortDirection {
    const fn label(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Output => "output",
        }
    }
}

/// Which side of an edge failed validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeEndpointRole {
    /// The upstream endpoint of an edge.
    Source,
    /// The downstream endpoint of an edge.
    Target,
}

impl EdgeEndpointRole {
    const fn label(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Target => "target",
        }
    }
}

/// Error returned when a workflow graph is structurally invalid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowValidationError {
    /// Two nodes in the graph used the same identifier.
    DuplicateNode {
        /// Duplicated node identifier.
        node_id: NodeId,
    },
    /// A node declared the same port identifier more than once.
    DuplicatePort {
        /// Node that owns the duplicated port.
        node_id: NodeId,
        /// Duplicated port identifier.
        port_id: PortId,
    },
    /// An edge referenced a node that is not declared in the graph.
    UnknownNode {
        /// Zero-based index of the invalid edge.
        edge_index: usize,
        /// Endpoint role that referenced the missing node.
        endpoint: EdgeEndpointRole,
        /// Missing node identifier.
        node_id: NodeId,
    },
    /// An edge referenced a port that is not declared for the required direction.
    UnknownPort {
        /// Zero-based index of the invalid edge.
        edge_index: usize,
        /// Endpoint role that referenced the missing port.
        endpoint: EdgeEndpointRole,
        /// Node that should own the port.
        node_id: NodeId,
        /// Missing port identifier.
        port_id: PortId,
        /// Direction required by this endpoint.
        expected: PortDirection,
    },
}

impl fmt::Display for WorkflowValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateNode { node_id } => {
                write!(f, "workflow graph contains duplicate node `{node_id}`")
            }
            Self::DuplicatePort { node_id, port_id } => {
                write!(f, "node `{node_id}` contains duplicate port `{port_id}`")
            }
            Self::UnknownNode {
                edge_index,
                endpoint,
                node_id,
            } => write!(
                f,
                "edge {edge_index} {} references unknown node `{node_id}`",
                endpoint.label()
            ),
            Self::UnknownPort {
                edge_index,
                endpoint,
                node_id,
                port_id,
                expected,
            } => write!(
                f,
                "edge {edge_index} {} references unknown {} port `{port_id}` on node `{node_id}`",
                endpoint.label(),
                expected.label()
            ),
        }
    }
}

impl Error for WorkflowValidationError {}

/// Static endpoint for one side of a workflow edge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeEndpoint {
    node_id: NodeId,
    port_id: PortId,
}

impl EdgeEndpoint {
    /// Create an edge endpoint from a node and port identifier.
    #[must_use]
    pub const fn new(node_id: NodeId, port_id: PortId) -> Self {
        Self { node_id, port_id }
    }

    /// Node referenced by this endpoint.
    #[must_use]
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Port referenced by this endpoint.
    #[must_use]
    pub const fn port_id(&self) -> &PortId {
        &self.port_id
    }
}

/// Directed connection from one output port to one input port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeDefinition {
    source: EdgeEndpoint,
    target: EdgeEndpoint,
}

impl EdgeDefinition {
    /// Create an edge from an upstream endpoint to a downstream endpoint.
    #[must_use]
    pub const fn new(source: EdgeEndpoint, target: EdgeEndpoint) -> Self {
        Self { source, target }
    }

    /// Upstream output endpoint.
    #[must_use]
    pub const fn source(&self) -> &EdgeEndpoint {
        &self.source
    }

    /// Downstream input endpoint.
    #[must_use]
    pub const fn target(&self) -> &EdgeEndpoint {
        &self.target
    }
}

/// Static node declaration and its input/output port topology.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDefinition {
    id: NodeId,
    input_ports: Vec<PortId>,
    output_ports: Vec<PortId>,
}

impl NodeDefinition {
    /// Create a node with declared input and output ports.
    ///
    /// # Errors
    ///
    /// Returns an error if a port identifier is repeated within this node,
    /// including reuse across input and output directions.
    pub fn new(
        id: NodeId,
        input_ports: impl Into<Vec<PortId>>,
        output_ports: impl Into<Vec<PortId>>,
    ) -> Result<Self, WorkflowValidationError> {
        let input_ports: Vec<PortId> = input_ports.into();
        let output_ports: Vec<PortId> = output_ports.into();
        reject_duplicate_ports(&id, &input_ports, &output_ports)?;

        Ok(Self {
            id,
            input_ports,
            output_ports,
        })
    }

    /// Node identifier.
    #[must_use]
    pub const fn id(&self) -> &NodeId {
        &self.id
    }

    /// Declared input ports.
    #[must_use]
    pub fn input_ports(&self) -> &[PortId] {
        &self.input_ports
    }

    /// Declared output ports.
    #[must_use]
    pub fn output_ports(&self) -> &[PortId] {
        &self.output_ports
    }
}

/// Validated graph-level workflow structure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowGraph {
    nodes: Vec<NodeDefinition>,
    edges: Vec<EdgeDefinition>,
}

impl WorkflowGraph {
    /// Create and validate a workflow graph.
    ///
    /// # Errors
    ///
    /// Returns an error when nodes or ports are duplicated, or when an edge
    /// references an undeclared node or the wrong port direction.
    pub fn new(
        nodes: impl Into<Vec<NodeDefinition>>,
        edges: impl Into<Vec<EdgeDefinition>>,
    ) -> Result<Self, WorkflowValidationError> {
        let graph: Self = Self {
            nodes: nodes.into(),
            edges: edges.into(),
        };
        graph.validate()?;
        Ok(graph)
    }

    /// Create an empty graph with no nodes or edges.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    /// Declared nodes in stable workflow order.
    #[must_use]
    pub fn nodes(&self) -> &[NodeDefinition] {
        &self.nodes
    }

    /// Declared edges in stable workflow order.
    #[must_use]
    pub fn edges(&self) -> &[EdgeDefinition] {
        &self.edges
    }

    fn validate(&self) -> Result<(), WorkflowValidationError> {
        reject_duplicate_nodes(&self.nodes)?;
        let topology: GraphTopology = GraphTopology::from_nodes(&self.nodes);

        for (edge_index, edge) in self.edges.iter().enumerate() {
            topology.validate_endpoint(
                edge_index,
                EdgeEndpointRole::Source,
                edge.source(),
                PortDirection::Output,
            )?;
            topology.validate_endpoint(
                edge_index,
                EdgeEndpointRole::Target,
                edge.target(),
                PortDirection::Input,
            )?;
        }

        Ok(())
    }
}

/// Parsed workflow definition independent of runtime execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowDefinition {
    id: WorkflowId,
    graph: WorkflowGraph,
}

impl WorkflowDefinition {
    /// Create a workflow definition from an already validated graph.
    #[must_use]
    pub const fn new(id: WorkflowId, graph: WorkflowGraph) -> Self {
        Self { id, graph }
    }

    /// Create a workflow definition from raw graph parts.
    ///
    /// # Errors
    ///
    /// Returns an error when the graph is structurally invalid.
    pub fn from_parts(
        id: WorkflowId,
        nodes: impl Into<Vec<NodeDefinition>>,
        edges: impl Into<Vec<EdgeDefinition>>,
    ) -> Result<Self, WorkflowValidationError> {
        let graph: WorkflowGraph = WorkflowGraph::new(nodes, edges)?;
        Ok(Self::new(id, graph))
    }

    /// Create a placeholder workflow with no nodes.
    ///
    /// # Errors
    ///
    /// Returns an error if the workflow identifier is invalid.
    pub fn empty(name: impl Into<String>) -> Result<Self, IdentifierError> {
        Ok(Self::new(WorkflowId::new(name)?, WorkflowGraph::empty()))
    }

    /// Workflow identifier.
    #[must_use]
    pub const fn id(&self) -> &WorkflowId {
        &self.id
    }

    /// Validated workflow graph.
    #[must_use]
    pub const fn graph(&self) -> &WorkflowGraph {
        &self.graph
    }

    /// Declared nodes in stable workflow order.
    #[must_use]
    pub fn nodes(&self) -> &[NodeDefinition] {
        self.graph.nodes()
    }

    /// Declared edges in stable workflow order.
    #[must_use]
    pub fn edges(&self) -> &[EdgeDefinition] {
        self.graph.edges()
    }
}

struct GraphTopology {
    inputs_by_node: BTreeMap<NodeId, BTreeSet<PortId>>,
    outputs_by_node: BTreeMap<NodeId, BTreeSet<PortId>>,
}

impl GraphTopology {
    fn from_nodes(nodes: &[NodeDefinition]) -> Self {
        let mut inputs_by_node: BTreeMap<NodeId, BTreeSet<PortId>> = BTreeMap::new();
        let mut outputs_by_node: BTreeMap<NodeId, BTreeSet<PortId>> = BTreeMap::new();

        for node in nodes {
            inputs_by_node.insert(
                node.id().clone(),
                node.input_ports().iter().cloned().collect(),
            );
            outputs_by_node.insert(
                node.id().clone(),
                node.output_ports().iter().cloned().collect(),
            );
        }

        Self {
            inputs_by_node,
            outputs_by_node,
        }
    }

    fn validate_endpoint(
        &self,
        edge_index: usize,
        endpoint: EdgeEndpointRole,
        edge_endpoint: &EdgeEndpoint,
        expected: PortDirection,
    ) -> Result<(), WorkflowValidationError> {
        let ports_by_node: &BTreeMap<NodeId, BTreeSet<PortId>> = match expected {
            PortDirection::Input => &self.inputs_by_node,
            PortDirection::Output => &self.outputs_by_node,
        };

        let ports: &BTreeSet<PortId> =
            ports_by_node.get(edge_endpoint.node_id()).ok_or_else(|| {
                WorkflowValidationError::UnknownNode {
                    edge_index,
                    endpoint,
                    node_id: edge_endpoint.node_id().clone(),
                }
            })?;

        if !ports.contains(edge_endpoint.port_id()) {
            return Err(WorkflowValidationError::UnknownPort {
                edge_index,
                endpoint,
                node_id: edge_endpoint.node_id().clone(),
                port_id: edge_endpoint.port_id().clone(),
                expected,
            });
        }

        Ok(())
    }
}

fn reject_duplicate_nodes(nodes: &[NodeDefinition]) -> Result<(), WorkflowValidationError> {
    let mut seen: BTreeSet<NodeId> = BTreeSet::new();

    for node in nodes {
        if !seen.insert(node.id().clone()) {
            return Err(WorkflowValidationError::DuplicateNode {
                node_id: node.id().clone(),
            });
        }
    }

    Ok(())
}

fn reject_duplicate_ports(
    node_id: &NodeId,
    input_ports: &[PortId],
    output_ports: &[PortId],
) -> Result<(), WorkflowValidationError> {
    let mut seen: BTreeSet<PortId> = BTreeSet::new();

    for port_id in input_ports.iter().chain(output_ports) {
        if !seen.insert(port_id.clone()) {
            return Err(WorkflowValidationError::DuplicatePort {
                node_id: node_id.clone(),
                port_id: port_id.clone(),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use conduit_types::IdentifierKind;
    use proptest::{collection::hash_set, prelude::*};

    fn valid_identifier_strategy() -> impl Strategy<Value = String> {
        prop::collection::vec(
            any::<char>().prop_filter(
                "identifier characters must not be whitespace or control",
                |ch| !ch.is_whitespace() && !ch.is_control(),
            ),
            1..16,
        )
        .prop_map(|chars: Vec<char>| chars.into_iter().collect())
    }

    fn workflow_id(value: &str) -> WorkflowId {
        WorkflowId::new(value).expect("valid workflow id")
    }

    fn node_id(value: &str) -> NodeId {
        NodeId::new(value).expect("valid node id")
    }

    fn port_id(value: &str) -> PortId {
        PortId::new(value).expect("valid port id")
    }

    fn endpoint(node: &str, port: &str) -> EdgeEndpoint {
        EdgeEndpoint::new(node_id(node), port_id(port))
    }

    #[test]
    fn empty_workflow_uses_valid_identifier() {
        let workflow = WorkflowDefinition::empty("conduit-scaffold").expect("valid id");

        assert_eq!(workflow.id().as_str(), "conduit-scaffold");
        assert!(workflow.nodes().is_empty());
        assert!(workflow.edges().is_empty());
    }

    #[test]
    fn empty_workflow_rejects_invalid_identifier() {
        let err = WorkflowDefinition::empty("bad workflow").expect_err("whitespace must fail");
        assert_eq!(
            err,
            IdentifierError::Whitespace {
                kind: IdentifierKind::Workflow
            }
        );
    }

    #[test]
    fn valid_workflow_represents_nodes_ports_and_edges() {
        let producer = NodeDefinition::new(
            node_id("producer"),
            Vec::<PortId>::new(),
            [port_id("records")],
        )
        .expect("valid producer");
        let consumer = NodeDefinition::new(
            node_id("consumer"),
            [port_id("records")],
            Vec::<PortId>::new(),
        )
        .expect("valid consumer");
        let edge = EdgeDefinition::new(
            endpoint("producer", "records"),
            endpoint("consumer", "records"),
        );

        let workflow =
            WorkflowDefinition::from_parts(workflow_id("ingest"), [producer, consumer], [edge])
                .expect("valid graph");

        assert_eq!(workflow.id().as_str(), "ingest");
        assert_eq!(workflow.nodes().len(), 2);
        assert_eq!(workflow.edges().len(), 1);
    }

    #[test]
    fn duplicate_nodes_are_rejected() {
        let first =
            NodeDefinition::new(node_id("step"), Vec::<PortId>::new(), Vec::<PortId>::new())
                .expect("valid node");
        let second =
            NodeDefinition::new(node_id("step"), Vec::<PortId>::new(), Vec::<PortId>::new())
                .expect("valid node");

        let err = WorkflowGraph::new([first, second], Vec::<EdgeDefinition>::new())
            .expect_err("duplicate nodes must fail");

        assert_eq!(
            err,
            WorkflowValidationError::DuplicateNode {
                node_id: node_id("step")
            }
        );
    }

    #[test]
    fn duplicate_ports_on_one_node_are_rejected() {
        let err = NodeDefinition::new(node_id("step"), [port_id("value")], [port_id("value")])
            .expect_err("duplicate ports must fail");

        assert_eq!(
            err,
            WorkflowValidationError::DuplicatePort {
                node_id: node_id("step"),
                port_id: port_id("value")
            }
        );
    }

    #[test]
    fn edge_source_must_reference_existing_node() {
        let consumer = NodeDefinition::new(
            node_id("consumer"),
            [port_id("records")],
            Vec::<PortId>::new(),
        )
        .expect("valid consumer");
        let edge = EdgeDefinition::new(
            endpoint("missing", "records"),
            endpoint("consumer", "records"),
        );

        let err = WorkflowGraph::new([consumer], [edge]).expect_err("missing source must fail");

        assert_eq!(
            err,
            WorkflowValidationError::UnknownNode {
                edge_index: 0,
                endpoint: EdgeEndpointRole::Source,
                node_id: node_id("missing")
            }
        );
    }

    #[test]
    fn edge_source_must_reference_output_port() {
        let producer = NodeDefinition::new(
            node_id("producer"),
            [port_id("records")],
            Vec::<PortId>::new(),
        )
        .expect("valid producer");
        let consumer = NodeDefinition::new(
            node_id("consumer"),
            [port_id("records")],
            Vec::<PortId>::new(),
        )
        .expect("valid consumer");
        let edge = EdgeDefinition::new(
            endpoint("producer", "records"),
            endpoint("consumer", "records"),
        );

        let err = WorkflowGraph::new([producer, consumer], [edge])
            .expect_err("input source port must fail");

        assert_eq!(
            err,
            WorkflowValidationError::UnknownPort {
                edge_index: 0,
                endpoint: EdgeEndpointRole::Source,
                node_id: node_id("producer"),
                port_id: port_id("records"),
                expected: PortDirection::Output
            }
        );
    }

    #[test]
    fn edge_target_must_reference_input_port() {
        let producer = NodeDefinition::new(
            node_id("producer"),
            Vec::<PortId>::new(),
            [port_id("records")],
        )
        .expect("valid producer");
        let consumer = NodeDefinition::new(
            node_id("consumer"),
            Vec::<PortId>::new(),
            [port_id("records")],
        )
        .expect("valid consumer");
        let edge = EdgeDefinition::new(
            endpoint("producer", "records"),
            endpoint("consumer", "records"),
        );

        let err = WorkflowGraph::new([producer, consumer], [edge])
            .expect_err("output target port must fail");

        assert_eq!(
            err,
            WorkflowValidationError::UnknownPort {
                edge_index: 0,
                endpoint: EdgeEndpointRole::Target,
                node_id: node_id("consumer"),
                port_id: port_id("records"),
                expected: PortDirection::Input
            }
        );
    }

    fn build_linear_workflow(node_names: &[String]) -> WorkflowDefinition {
        let mut nodes: Vec<NodeDefinition> = Vec::new();
        let mut edges: Vec<EdgeDefinition> = Vec::new();

        for (index, node_name) in node_names.iter().enumerate() {
            let mut input_ports: Vec<PortId> = Vec::new();
            let mut output_ports: Vec<PortId> = Vec::new();

            if index > 0 {
                input_ports.push(port_id("in"));
            }

            if index + 1 < node_names.len() {
                output_ports.push(port_id("out"));
            }

            nodes.push(
                NodeDefinition::new(node_id(node_name), input_ports, output_ports)
                    .expect("linear workflow nodes must be valid"),
            );
        }

        for edge in node_names.windows(2) {
            edges.push(EdgeDefinition::new(
                endpoint(&edge[0], "out"),
                endpoint(&edge[1], "in"),
            ));
        }

        WorkflowDefinition::from_parts(workflow_id("flow"), nodes, edges)
            .expect("linear workflow must be valid")
    }

    proptest! {
        #[test]
        fn linear_workflows_with_unique_valid_node_ids_validate(
            node_names in hash_set(valid_identifier_strategy(), 1..6)
        ) {
            let mut node_names: Vec<String> = node_names.into_iter().collect();
            node_names.sort();

            let workflow: WorkflowDefinition = build_linear_workflow(&node_names);

            prop_assert_eq!(workflow.nodes().len(), node_names.len());
            prop_assert_eq!(workflow.edges().len(), node_names.len().saturating_sub(1));
        }
    }
}
