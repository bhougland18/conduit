//! CLI entrypoint for Conduit workflow validation, inspection, and scaffold runs.

use std::{
    env,
    error::Error,
    fmt::{self, Write as FmtWrite},
    fs,
    future::{Ready, ready},
    path::Path,
    sync::Arc,
};

use conduit_contract::{
    ContractValidationError, Determinism, ExecutionMode, NodeContract, PortContract,
};
use conduit_core::{
    ConduitError, JsonlMetadataSink, NodeExecutor, PortsIn, PortsOut, RetryDisposition,
    TieredMetadataSink,
    capability::{
        CapabilityValidationError, NodeCapabilities, PortCapability, PortCapabilityDirection,
    },
    context::{ExecutionMetadata, NodeContext},
};
use conduit_engine::run_workflow_with_metadata_sink;
use conduit_introspection::{
    IntrospectionJsonError, WorkflowIntrospection, introspect_workflow,
    workflow_introspection_to_json_string,
};
use conduit_runtime::AsupersyncRuntime;
use conduit_types::ExecutionId;
use conduit_workflow::{EdgeCapacity, NodeDefinition, PortDirection, WorkflowDefinition};
use conduit_workflow_format::{WorkflowJsonError, workflow_from_json_str};

type CliResult<T> = Result<T, CliError>;

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(error.exit_code());
    }
}

fn run() -> CliResult<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    let Some(output): Option<String> = command_output(&args, read_file, write_file)? else {
        println!("{}", usage());
        return Ok(());
    };

    print!("{output}");
    Ok(())
}

fn command_output(
    args: &[String],
    read: impl Fn(&Path) -> CliResult<String>,
    write: impl Fn(&Path, &str) -> CliResult<()>,
) -> CliResult<Option<String>> {
    match args {
        [] => Ok(None),
        [flag] if flag == "-h" || flag == "--help" => Ok(None),
        [command, path] if command == "validate" => {
            let input: String = read(Path::new(path))?;
            validate_workflow_json(&input).map(Some)
        }
        [command, path] if command == "inspect" => {
            let input: String = read(Path::new(path))?;
            inspect_workflow_json(&input).map(Some)
        }
        [command, path] if command == "explain" => {
            let input: String = read(Path::new(path))?;
            explain_workflow_json(&input).map(Some)
        }
        [command, workflow_path, metadata_path] if command == "run" => {
            let input: String = read(Path::new(workflow_path))?;
            let run: CliRunOutput = run_workflow_json(&input)?;
            write(Path::new(metadata_path), &run.metadata_jsonl)?;
            Ok(Some(format!(
                "ran workflow `{}`\nnodes: {}\nedges: {}\nmetadata: {}\nrecords: {}\n",
                run.workflow_id, run.node_count, run.edge_count, metadata_path, run.record_count
            )))
        }
        _ => Err(CliError::Usage),
    }
}

fn read_file(path: &Path) -> CliResult<String> {
    fs::read_to_string(path).map_err(|source: std::io::Error| CliError::Io {
        action: "read",
        path: path.display().to_string(),
        source,
    })
}

fn write_file(path: &Path, contents: &str) -> CliResult<()> {
    fs::write(path, contents).map_err(|source: std::io::Error| CliError::Io {
        action: "write",
        path: path.display().to_string(),
        source,
    })
}

fn validate_workflow_json(input: &str) -> CliResult<String> {
    let workflow: WorkflowDefinition = workflow_from_json_str(input)?;

    Ok(format!(
        "valid workflow `{}`\nnodes: {}\nedges: {}\n",
        workflow.id(),
        workflow.nodes().len(),
        workflow.edges().len()
    ))
}

fn inspect_workflow_json(input: &str) -> CliResult<String> {
    let workflow: WorkflowDefinition = workflow_from_json_str(input)?;
    let (contracts, capabilities): (Vec<NodeContract>, Vec<NodeCapabilities>) =
        passive_native_contracts_for_workflow(&workflow)?;
    let introspection: WorkflowIntrospection =
        introspect_workflow(&workflow, &contracts, &capabilities)?;
    let mut output: String = workflow_introspection_to_json_string(&introspection)?;
    output.push('\n');
    Ok(output)
}

fn explain_workflow_json(input: &str) -> CliResult<String> {
    let workflow: WorkflowDefinition = workflow_from_json_str(input)?;
    let mut output: String = format!(
        "workflow `{}`\nstatus: valid\nnodes: {}\nedges: {}\nexecution: scaffold-noop\nmetadata: jsonl lifecycle records with tiered control-only policy\n",
        workflow.id(),
        workflow.nodes().len(),
        workflow.edges().len()
    );

    output.push_str("node order:\n");
    for node in workflow.nodes() {
        writeln!(
            &mut output,
            "  - {} inputs={} outputs={}",
            node.id(),
            node.input_ports().len(),
            node.output_ports().len()
        )
        .map_err(|_err: fmt::Error| ConduitError::execution("failed to format explanation"))?;
    }

    output.push_str("edges:\n");
    for edge in workflow.edges() {
        let capacity: String = match edge.capacity() {
            EdgeCapacity::Default => String::from("default"),
            EdgeCapacity::Explicit(capacity) => capacity.get().to_string(),
        };
        writeln!(
            &mut output,
            "  - {}.{} -> {}.{} capacity={}",
            edge.source().node_id(),
            edge.source().port_id(),
            edge.target().node_id(),
            edge.target().port_id(),
            capacity
        )
        .map_err(|_err: fmt::Error| ConduitError::execution("failed to format explanation"))?;
    }

    Ok(output)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CliRunOutput {
    workflow_id: String,
    node_count: usize,
    edge_count: usize,
    metadata_jsonl: String,
    record_count: usize,
}

fn run_workflow_json(input: &str) -> CliResult<CliRunOutput> {
    let workflow: WorkflowDefinition = workflow_from_json_str(input)?;
    let runtime: AsupersyncRuntime = AsupersyncRuntime::new()?;
    let execution: ExecutionMetadata =
        ExecutionMetadata::first_attempt(ExecutionId::new("cli-run-1")?);
    let executor: CliNoopExecutor = CliNoopExecutor;
    let metadata_sink: Arc<TieredMetadataSink<JsonlMetadataSink<Vec<u8>>>> =
        Arc::new(TieredMetadataSink::new(JsonlMetadataSink::new(Vec::new())));

    runtime.block_on(run_workflow_with_metadata_sink(
        &workflow,
        &execution,
        &executor,
        metadata_sink.clone(),
    ))?;

    let metadata_sink: TieredMetadataSink<JsonlMetadataSink<Vec<u8>>> =
        Arc::try_unwrap(metadata_sink).map_err(
            |_sink: Arc<TieredMetadataSink<JsonlMetadataSink<Vec<u8>>>>| {
                ConduitError::metadata("metadata sink still has active references")
            },
        )?;
    let metadata_bytes: Vec<u8> = metadata_sink.into_inner().into_inner()?;
    let metadata_jsonl: String =
        String::from_utf8(metadata_bytes).map_err(|source: std::string::FromUtf8Error| {
            ConduitError::metadata(format!("metadata JSONL was not valid UTF-8: {source}"))
        })?;
    let record_count: usize = metadata_jsonl.lines().count();

    Ok(CliRunOutput {
        workflow_id: workflow.id().to_string(),
        node_count: workflow.nodes().len(),
        edge_count: workflow.edges().len(),
        metadata_jsonl,
        record_count,
    })
}

#[derive(Debug, Clone, Copy, Default)]
struct CliNoopExecutor;

impl NodeExecutor for CliNoopExecutor {
    type RunFuture<'a> = Ready<conduit_core::Result<()>>;

    fn run(&self, _ctx: NodeContext, _inputs: PortsIn, _outputs: PortsOut) -> Self::RunFuture<'_> {
        ready(Ok(()))
    }
}

fn passive_native_contracts_for_workflow(
    workflow: &WorkflowDefinition,
) -> CliResult<(Vec<NodeContract>, Vec<NodeCapabilities>)> {
    let contracts: Vec<NodeContract> = workflow
        .nodes()
        .iter()
        .map(passive_native_contract_for_node)
        .collect::<CliResult<Vec<NodeContract>>>()?;
    let capabilities: Vec<NodeCapabilities> = workflow
        .nodes()
        .iter()
        .map(passive_native_capabilities_for_node)
        .collect::<CliResult<Vec<NodeCapabilities>>>()?;

    Ok((contracts, capabilities))
}

fn passive_native_contract_for_node(node: &NodeDefinition) -> CliResult<NodeContract> {
    let mut ports: Vec<PortContract> =
        Vec::with_capacity(node.input_ports().len() + node.output_ports().len());
    for port_id in node.input_ports() {
        ports.push(PortContract::new(
            port_id.clone(),
            PortDirection::Input,
            None,
        ));
    }
    for port_id in node.output_ports() {
        ports.push(PortContract::new(
            port_id.clone(),
            PortDirection::Output,
            None,
        ));
    }

    NodeContract::new(
        node.id().clone(),
        ports,
        ExecutionMode::Native,
        Determinism::Unknown,
        RetryDisposition::Unknown,
    )
    .map_err(CliError::Contract)
}

fn passive_native_capabilities_for_node(node: &NodeDefinition) -> CliResult<NodeCapabilities> {
    let mut ports: Vec<PortCapability> =
        Vec::with_capacity(node.input_ports().len() + node.output_ports().len());
    for port_id in node.input_ports() {
        ports.push(PortCapability::new(
            port_id.clone(),
            PortCapabilityDirection::Receive,
        ));
    }
    for port_id in node.output_ports() {
        ports.push(PortCapability::new(
            port_id.clone(),
            PortCapabilityDirection::Emit,
        ));
    }

    NodeCapabilities::native_passive(node.id().clone(), ports).map_err(CliError::Capability)
}

const fn usage() -> &'static str {
    "Usage:\n  conduit validate <workflow.json>\n  conduit inspect <workflow.json>\n  conduit explain <workflow.json>\n  conduit run <workflow.json> <metadata.jsonl>"
}

#[derive(Debug)]
enum CliError {
    Usage,
    Io {
        action: &'static str,
        path: String,
        source: std::io::Error,
    },
    WorkflowJson(WorkflowJsonError),
    Contract(ContractValidationError),
    Capability(CapabilityValidationError),
    IntrospectionJson(IntrospectionJsonError),
    Runtime(ConduitError),
}

impl CliError {
    const fn exit_code(&self) -> i32 {
        match self {
            Self::Usage => 2,
            Self::Io { .. }
            | Self::WorkflowJson(_)
            | Self::Contract(_)
            | Self::Capability(_)
            | Self::IntrospectionJson(_)
            | Self::Runtime(_) => 1,
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage => write!(f, "invalid arguments\n{}", usage()),
            Self::Io {
                action,
                path,
                source,
            } => write!(f, "failed to {action} `{path}`: {source}"),
            Self::WorkflowJson(source) => write!(f, "{source}"),
            Self::Contract(source) => write!(f, "workflow contract validation failed: {source}"),
            Self::Capability(source) => {
                write!(f, "workflow capability validation failed: {source}")
            }
            Self::IntrospectionJson(source) => write!(f, "{source}"),
            Self::Runtime(source) => write!(f, "{source}"),
        }
    }
}

impl Error for CliError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Usage => None,
            Self::Io { source, .. } => Some(source),
            Self::WorkflowJson(source) => Some(source),
            Self::Contract(source) => Some(source),
            Self::Capability(source) => Some(source),
            Self::IntrospectionJson(source) => Some(source),
            Self::Runtime(source) => Some(source),
        }
    }
}

impl From<WorkflowJsonError> for CliError {
    fn from(source: WorkflowJsonError) -> Self {
        Self::WorkflowJson(source)
    }
}

impl From<ContractValidationError> for CliError {
    fn from(source: ContractValidationError) -> Self {
        Self::Contract(source)
    }
}

impl From<IntrospectionJsonError> for CliError {
    fn from(source: IntrospectionJsonError) -> Self {
        Self::IntrospectionJson(source)
    }
}

impl From<ConduitError> for CliError {
    fn from(source: ConduitError) -> Self {
        Self::Runtime(source)
    }
}

impl From<conduit_types::IdentifierError> for CliError {
    fn from(source: conduit_types::IdentifierError) -> Self {
        Self::Runtime(ConduitError::from(source))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WORKFLOW_JSON: &str = r#"{
  "conduit_version": "1",
  "id": "flow",
  "nodes": [
    { "id": "source", "inputs": [], "outputs": ["out"] },
    { "id": "sink", "inputs": ["in"], "outputs": [] }
  ],
  "edges": [
    {
      "source": { "node": "source", "port": "out" },
      "target": { "node": "sink", "port": "in" },
      "capacity": 8
    }
  ]
}"#;

    #[test]
    fn validate_reports_valid_workflow_summary() {
        let output = validate_workflow_json(WORKFLOW_JSON).expect("workflow should validate");

        assert_eq!(output, "valid workflow `flow`\nnodes: 2\nedges: 1\n");
    }

    #[test]
    fn inspect_renders_introspection_json() {
        let output = inspect_workflow_json(WORKFLOW_JSON).expect("workflow should inspect");

        assert!(output.contains("\"workflow_id\": \"flow\""));
        assert!(output.contains("\"execution_mode\": \"native\""));
        assert!(output.contains("\"capacity\""));
    }

    #[test]
    fn explain_reports_valid_topology_and_metadata_policy() {
        let output = explain_workflow_json(WORKFLOW_JSON).expect("workflow should explain");

        assert!(output.contains("workflow `flow`"));
        assert!(output.contains("execution: scaffold-noop"));
        assert!(output.contains("metadata: jsonl lifecycle records"));
        assert!(output.contains("source.out -> sink.in capacity=8"));
    }

    #[test]
    fn run_writes_reproducible_metadata_jsonl() {
        let output = run_workflow_json(WORKFLOW_JSON).expect("workflow should run");

        assert_eq!(output.workflow_id, "flow");
        assert_eq!(output.node_count, 2);
        assert_eq!(output.edge_count, 1);
        assert_eq!(output.record_count, 4);
        assert!(
            output
                .metadata_jsonl
                .contains("\"record_type\":\"lifecycle\"")
        );
        assert!(
            output
                .metadata_jsonl
                .contains("\"execution_id\":\"cli-run-1\"")
        );
    }

    #[test]
    fn validate_rejects_invalid_workflow_json() {
        let err = validate_workflow_json("{").expect_err("malformed JSON should fail");

        assert!(matches!(err, CliError::WorkflowJson(_)));
    }
}
