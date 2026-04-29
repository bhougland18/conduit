//! CLI entrypoint for Conduit workflow validation and inspection.

use std::{env, error::Error, fmt, fs, path::Path};

use conduit_contract::{
    ContractValidationError, Determinism, ExecutionMode, NodeContract, PortContract,
};
use conduit_core::{
    RetryDisposition,
    capability::{
        CapabilityValidationError, NodeCapabilities, PortCapability, PortCapabilityDirection,
    },
};
use conduit_introspection::{
    IntrospectionJsonError, WorkflowIntrospection, introspect_workflow,
    workflow_introspection_to_json_string,
};
use conduit_workflow::{NodeDefinition, PortDirection, WorkflowDefinition};
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
    let Some(output): Option<String> = command_output(&args, read_file)? else {
        println!("{}", usage());
        return Ok(());
    };

    print!("{output}");
    Ok(())
}

fn command_output(
    args: &[String],
    read: impl FnOnce(&Path) -> CliResult<String>,
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
        _ => Err(CliError::Usage),
    }
}

fn read_file(path: &Path) -> CliResult<String> {
    fs::read_to_string(path).map_err(|source: std::io::Error| CliError::Io {
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
    "Usage:\n  conduit validate <workflow.json>\n  conduit inspect <workflow.json>"
}

#[derive(Debug)]
enum CliError {
    Usage,
    Io {
        path: String,
        source: std::io::Error,
    },
    WorkflowJson(WorkflowJsonError),
    Contract(ContractValidationError),
    Capability(CapabilityValidationError),
    IntrospectionJson(IntrospectionJsonError),
}

impl CliError {
    const fn exit_code(&self) -> i32 {
        match self {
            Self::Usage => 2,
            Self::Io { .. }
            | Self::WorkflowJson(_)
            | Self::Contract(_)
            | Self::Capability(_)
            | Self::IntrospectionJson(_) => 1,
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage => write!(f, "invalid arguments\n{}", usage()),
            Self::Io { path, source } => write!(f, "failed to read `{path}`: {source}"),
            Self::WorkflowJson(source) => write!(f, "{source}"),
            Self::Contract(source) => write!(f, "workflow contract validation failed: {source}"),
            Self::Capability(source) => {
                write!(f, "workflow capability validation failed: {source}")
            }
            Self::IntrospectionJson(source) => write!(f, "{source}"),
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
    fn validate_rejects_invalid_workflow_json() {
        let err = validate_workflow_json("{").expect_err("malformed JSON should fail");

        assert!(matches!(err, CliError::WorkflowJson(_)));
    }
}
