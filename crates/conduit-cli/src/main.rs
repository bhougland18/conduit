//! CLI entrypoint for Conduit workflow validation, inspection, and scaffold runs.

use std::{
    collections::BTreeMap,
    env,
    error::Error,
    fmt::{self, Write as FmtWrite},
    fs,
    future::Future,
    path::Path,
    pin::Pin,
    str::FromStr,
    sync::Arc,
};

use conduit_contract::{
    ContractValidationError, Determinism, ExecutionMode, NodeContract, PortContract,
};
use conduit_core::{
    ConduitError, ErrorVisibility, JsonlMetadataSink, NodeExecutor, PacketPayload, PortPacket,
    PortRecvError, PortsIn, PortsOut, RetryDisposition, TieredMetadataSink,
    capability::{
        CapabilityValidationError, NodeCapabilities, PortCapability, PortCapabilityDirection,
    },
    context::{CancellationToken, ExecutionMetadata, NodeContext},
    message::{MessageEndpoint, MessageMetadata, MessageRoute},
};
use conduit_engine::{
    CycleRunPolicy, FeedbackLoopStartup, FeedbackLoopTermination, StaticNodeExecutorRegistry,
    WorkflowDeadlockDiagnostic, WorkflowRunSummary, WorkflowTerminalState,
    run_workflow_with_registry_and_metadata_sink_summary,
};
use conduit_introspection::{
    IntrospectionJsonError, WorkflowIntrospection, introspect_workflow,
    workflow_introspection_to_json_string,
};
use conduit_runtime::AsupersyncRuntime;
use conduit_types::{ExecutionId, MessageId, NodeId, PortId};
use conduit_workflow::{EdgeCapacity, NodeDefinition, PortDirection, WorkflowDefinition};
use conduit_workflow_format::{WorkflowJsonError, workflow_from_json_str};
use serde_json::{Value, json};
use tracing_subscriber::{
    filter::{ParseError, Targets},
    layer::SubscriberExt,
    util::{SubscriberInitExt, TryInitError},
};

type CliResult<T> = Result<T, CliError>;

const CONDUIT_TRACE_ENV: &str = "CONDUIT_TRACE";
const RUST_LOG_ENV: &str = "RUST_LOG";

fn main() {
    if let Err(error) = run() {
        eprintln!("error: {error}");
        std::process::exit(error.exit_code());
    }
}

fn run() -> CliResult<()> {
    initialize_tracing_from_env()?;

    let args: Vec<String> = env::args().skip(1).collect();
    let Some(output): Option<String> = command_output(&args, read_file, write_file)? else {
        println!("{}", usage());
        return Ok(());
    };

    print!("{output}");
    Ok(())
}

fn initialize_tracing_from_env() -> CliResult<()> {
    let Some(targets): Option<Targets> =
        tracing_targets_from_env(|name: &str| env::var(name).ok())?
    else {
        return Ok(());
    };

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_ansi(false),
        )
        .with(targets)
        .try_init()
        .map_err(|source: TryInitError| {
            CliError::Tracing(format!("failed to initialize tracing subscriber: {source}"))
        })
}

fn tracing_targets_from_env(
    read_env: impl Fn(&str) -> Option<String>,
) -> CliResult<Option<Targets>> {
    if let Some(value) = read_env(CONDUIT_TRACE_ENV) {
        return tracing_targets_from_value(CONDUIT_TRACE_ENV, &value);
    }
    if let Some(value) = read_env(RUST_LOG_ENV) {
        return tracing_targets_from_value(RUST_LOG_ENV, &value);
    }

    Ok(None)
}

fn tracing_targets_from_value(env_name: &'static str, value: &str) -> CliResult<Option<Targets>> {
    let trimmed: &str = value.trim();
    if tracing_value_disables_output(trimmed) {
        return Ok(None);
    }

    let filter: &str = if tracing_value_uses_default_filter(trimmed) {
        "info"
    } else {
        trimmed
    };

    Targets::from_str(filter)
        .map(Some)
        .map_err(|source: ParseError| {
            CliError::Tracing(format!(
                "{env_name} has invalid tracing filter `{value}`: {source}"
            ))
        })
}

fn tracing_value_disables_output(value: &str) -> bool {
    let lowercase: String = value.to_ascii_lowercase();
    matches!(lowercase.as_str(), "" | "0" | "false" | "off")
}

fn tracing_value_uses_default_filter(value: &str) -> bool {
    let lowercase: String = value.to_ascii_lowercase();
    matches!(lowercase.as_str(), "1" | "true" | "yes")
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
        [command, flag, workflow_path, metadata_path] if command == "run" && flag == "--json" => {
            let input: String = read(Path::new(workflow_path))?;
            let run: CliRunOutput = run_workflow_json(&input)?;
            write(Path::new(metadata_path), &run.metadata_jsonl)?;
            cli_run_output_to_json_string(&run, metadata_path).map(Some)
        }
        [command, workflow_path, metadata_path, flag] if command == "run" && flag == "--json" => {
            let input: String = read(Path::new(workflow_path))?;
            let run: CliRunOutput = run_workflow_json(&input)?;
            write(Path::new(metadata_path), &run.metadata_jsonl)?;
            cli_run_output_to_json_string(&run, metadata_path).map(Some)
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
        "workflow `{}`\nstatus: valid\nnodes: {}\nedges: {}\nexecution: native-registry\nmetadata: jsonl lifecycle, message, and queue-pressure records with tiered control-only policy\n",
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
    summary: WorkflowRunSummary,
}

fn run_workflow_json(input: &str) -> CliResult<CliRunOutput> {
    let workflow: WorkflowDefinition = workflow_from_json_str(input)?;
    let runtime: AsupersyncRuntime = AsupersyncRuntime::new()?;
    let execution: ExecutionMetadata =
        ExecutionMetadata::first_attempt(ExecutionId::new("cli-run-1")?);
    let registry: StaticNodeExecutorRegistry<CliNativeExecutor> =
        native_registry_for_workflow(&workflow);
    let metadata_sink: Arc<TieredMetadataSink<JsonlMetadataSink<Vec<u8>>>> =
        Arc::new(TieredMetadataSink::new(JsonlMetadataSink::new(Vec::new())));

    let summary: WorkflowRunSummary =
        runtime.block_on(run_workflow_with_registry_and_metadata_sink_summary(
            &workflow,
            &execution,
            &registry,
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
        summary,
    })
}

fn cli_run_output_to_json_string(run: &CliRunOutput, metadata_path: &str) -> CliResult<String> {
    let mut output: String = serde_json::to_string_pretty(&json!({
        "status": workflow_terminal_state_label(run.summary.terminal_state()),
        "error": run
            .summary
            .first_error()
            .map_or(Value::Null, conduit_error_to_json_value),
        "workflow": {
            "id": run.workflow_id,
            "node_count": run.node_count,
            "edge_count": run.edge_count,
        },
        "metadata": {
            "path": metadata_path,
            "record_count": run.record_count,
        },
        "summary": workflow_run_summary_to_json_value(&run.summary),
    }))
    .map_err(|source: serde_json::Error| {
        ConduitError::metadata(format!("failed to encode run summary JSON: {source}"))
    })?;
    output.push('\n');
    Ok(output)
}

fn workflow_run_summary_to_json_value(summary: &WorkflowRunSummary) -> Value {
    json!({
        "terminal_state": workflow_terminal_state_label(summary.terminal_state()),
        "scheduled_node_count": summary.scheduled_node_count(),
        "completed_node_count": summary.completed_node_count(),
        "failed_node_count": summary.failed_node_count(),
        "cancelled_node_count": summary.cancelled_node_count(),
        "pending_node_count": summary.pending_node_count(),
        "observed_message_count": summary.observed_message_count(),
        "error_count": summary.error_count(),
        "first_error": summary
            .first_error()
            .map_or(Value::Null, conduit_error_to_json_value),
        "deadlock_diagnostic": summary
            .deadlock_diagnostic()
            .map_or(Value::Null, workflow_deadlock_diagnostic_to_json_value),
    })
}

fn workflow_deadlock_diagnostic_to_json_value(diagnostic: &WorkflowDeadlockDiagnostic) -> Value {
    json!({
        "workflow_id": diagnostic.workflow_id().as_str(),
        "scheduled_node_count": diagnostic.scheduled_node_count(),
        "pending_node_count": diagnostic.pending_node_count(),
        "completed_node_count": diagnostic.completed_node_count(),
        "failed_node_count": diagnostic.failed_node_count(),
        "cancelled_node_count": diagnostic.cancelled_node_count(),
        "bounded_edge_count": diagnostic.bounded_edge_count(),
        "no_progress_timeout_ms": duration_millis_u64(diagnostic.no_progress_timeout()),
        "cycle_policy": cycle_run_policy_label(diagnostic.cycle_policy()),
        "feedback_loop_startup": feedback_loop_startup_value(diagnostic.cycle_policy()),
        "feedback_loop_termination": feedback_loop_termination_value(diagnostic.cycle_policy()),
    })
}

fn duration_millis_u64(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

const fn cycle_run_policy_label(policy: CycleRunPolicy) -> &'static str {
    match policy {
        CycleRunPolicy::Reject => "reject",
        CycleRunPolicy::AllowFeedbackLoops(_feedback_loop) => "allow_feedback_loops",
    }
}

fn feedback_loop_startup_value(policy: CycleRunPolicy) -> Value {
    match policy {
        CycleRunPolicy::Reject => Value::Null,
        CycleRunPolicy::AllowFeedbackLoops(feedback_loop) => {
            json!(feedback_loop_startup_label(feedback_loop.startup()))
        }
    }
}

fn feedback_loop_termination_value(policy: CycleRunPolicy) -> Value {
    match policy {
        CycleRunPolicy::Reject => Value::Null,
        CycleRunPolicy::AllowFeedbackLoops(feedback_loop) => {
            json!(feedback_loop_termination_label(feedback_loop.termination()))
        }
    }
}

const fn feedback_loop_startup_label(startup: FeedbackLoopStartup) -> &'static str {
    match startup {
        FeedbackLoopStartup::StartAllNodes => "start_all_nodes",
    }
}

const fn feedback_loop_termination_label(termination: FeedbackLoopTermination) -> &'static str {
    match termination {
        FeedbackLoopTermination::AllNodesComplete => "all_nodes_complete",
    }
}

const fn workflow_terminal_state_label(state: WorkflowTerminalState) -> &'static str {
    match state {
        WorkflowTerminalState::Completed => "completed",
        WorkflowTerminalState::Failed => "failed",
        WorkflowTerminalState::Cancelled => "cancelled",
    }
}

fn conduit_error_to_json_value(error: &ConduitError) -> Value {
    json!({
        "code": error.code().as_str(),
        "message": error.to_string(),
        "visibility": error_visibility_label(error.visibility()),
        "retry_disposition": retry_disposition_label(error.retry_disposition()),
    })
}

const fn error_visibility_label(visibility: ErrorVisibility) -> &'static str {
    match visibility {
        ErrorVisibility::User => "user",
        ErrorVisibility::Internal => "internal",
    }
}

const fn retry_disposition_label(disposition: RetryDisposition) -> &'static str {
    match disposition {
        RetryDisposition::Never => "never",
        RetryDisposition::Safe => "safe",
        RetryDisposition::Unknown => "unknown",
    }
}

#[derive(Debug, Clone)]
struct CliNativeExecutor {
    input_ports: Vec<PortId>,
    output_routes: BTreeMap<PortId, Vec<MessageEndpoint>>,
}

impl CliNativeExecutor {
    const fn new(
        input_ports: Vec<PortId>,
        output_routes: BTreeMap<PortId, Vec<MessageEndpoint>>,
    ) -> Self {
        Self {
            input_ports,
            output_routes,
        }
    }
}

impl NodeExecutor for CliNativeExecutor {
    type RunFuture<'a> = Pin<Box<dyn Future<Output = conduit_core::Result<()>> + Send + 'a>>;

    fn run(&self, ctx: NodeContext, inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
        Box::pin(run_cli_native_executor(self, ctx, inputs, outputs))
    }
}

async fn run_cli_native_executor(
    executor: &CliNativeExecutor,
    ctx: NodeContext,
    mut inputs: PortsIn,
    outputs: PortsOut,
) -> conduit_core::Result<()> {
    let cancellation: CancellationToken = ctx.cancellation_token();
    let mut received_count: usize = 0;

    for input_port in &executor.input_ports {
        let port_count: usize =
            drain_cli_input_port(&mut inputs, input_port, &cancellation).await?;
        received_count = received_count.saturating_add(port_count);
    }

    for (output_index, (output_port, targets)) in executor.output_routes.iter().enumerate() {
        send_cli_output_packet(
            &ctx,
            &outputs,
            output_port,
            targets,
            output_index,
            received_count,
            &cancellation,
        )
        .await?;
    }

    Ok(())
}

async fn drain_cli_input_port(
    inputs: &mut PortsIn,
    input_port: &PortId,
    cancellation: &CancellationToken,
) -> conduit_core::Result<usize> {
    let mut received_count: usize = 0;
    loop {
        match inputs.recv(input_port, cancellation).await {
            Ok(Some(_packet)) => received_count = received_count.saturating_add(1),
            Ok(None) | Err(PortRecvError::Disconnected { .. }) => return Ok(received_count),
            Err(err) => return Err(err.into()),
        }
    }
}

async fn send_cli_output_packet(
    ctx: &NodeContext,
    outputs: &PortsOut,
    output_port: &PortId,
    targets: &[MessageEndpoint],
    output_index: usize,
    received_count: usize,
    cancellation: &CancellationToken,
) -> conduit_core::Result<()> {
    let target: MessageEndpoint = targets
        .first()
        .cloned()
        .unwrap_or_else(|| MessageEndpoint::new(ctx.node_id().clone(), output_port.clone()));
    let source: MessageEndpoint = MessageEndpoint::new(ctx.node_id().clone(), output_port.clone());
    let route: MessageRoute = MessageRoute::new(Some(source), target);
    let message_id: MessageId = MessageId::new(format!(
        "cli-{}-{}-{output_index}",
        ctx.node_id(),
        output_port
    ))?;
    let metadata: MessageMetadata = MessageMetadata::new(
        message_id,
        ctx.workflow_id().clone(),
        ctx.execution().clone(),
        route,
    );
    let payload: PacketPayload = PacketPayload::bytes(format!(
        "cli-native node={} output={} received={received_count}",
        ctx.node_id(),
        output_port
    ));

    outputs
        .send(
            output_port,
            PortPacket::new(metadata, payload),
            cancellation,
        )
        .await?;
    Ok(())
}

fn native_registry_for_workflow(
    workflow: &WorkflowDefinition,
) -> StaticNodeExecutorRegistry<CliNativeExecutor> {
    let mut routes_by_node: BTreeMap<NodeId, BTreeMap<PortId, Vec<MessageEndpoint>>> =
        BTreeMap::new();
    for edge in workflow.edges() {
        routes_by_node
            .entry(edge.source().node_id().clone())
            .or_default()
            .entry(edge.source().port_id().clone())
            .or_default()
            .push(MessageEndpoint::new(
                edge.target().node_id().clone(),
                edge.target().port_id().clone(),
            ));
    }

    let mut executors: BTreeMap<NodeId, CliNativeExecutor> = BTreeMap::new();
    for node in workflow.nodes() {
        let mut output_routes: BTreeMap<PortId, Vec<MessageEndpoint>> = BTreeMap::new();
        for output_port in node.output_ports() {
            let targets: Vec<MessageEndpoint> = routes_by_node
                .get(node.id())
                .and_then(|routes: &BTreeMap<PortId, Vec<MessageEndpoint>>| routes.get(output_port))
                .cloned()
                .unwrap_or_default();
            output_routes.insert(output_port.clone(), targets);
        }
        let executor: CliNativeExecutor =
            CliNativeExecutor::new(node.input_ports().to_vec(), output_routes);
        executors.insert(node.id().clone(), executor);
    }

    StaticNodeExecutorRegistry::new(executors)
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
    "Usage:\n  conduit validate <workflow.json>\n  conduit inspect <workflow.json>\n  conduit explain <workflow.json>\n  conduit run <workflow.json> <metadata.jsonl>\n  conduit run --json <workflow.json> <metadata.jsonl>"
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
    Tracing(String),
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
            | Self::Runtime(_)
            | Self::Tracing(_) => 1,
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
            Self::Tracing(message) => write!(f, "{message}"),
        }
    }
}

impl Error for CliError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Usage | Self::Tracing(_) => None,
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
    use std::sync::{Arc, Mutex};

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
    const NATIVE_LINEAR_ETL_WORKFLOW_JSON: &str =
        include_str!("../../../examples/native-linear-etl.workflow.json");

    #[test]
    fn tracing_targets_are_opt_in() {
        let disabled: Option<Targets> =
            tracing_targets_from_env(|_name: &str| None).expect("missing env should parse");
        let explicit_off: Option<Targets> =
            tracing_targets_from_value(CONDUIT_TRACE_ENV, "off").expect("off should parse");
        let explicit_true: Option<Targets> =
            tracing_targets_from_value(CONDUIT_TRACE_ENV, "true").expect("true should parse");
        let rust_log_directive: Option<Targets> =
            tracing_targets_from_value(RUST_LOG_ENV, "conduit.runtime=debug")
                .expect("target directive should parse");

        assert!(disabled.is_none());
        assert!(explicit_off.is_none());
        assert!(explicit_true.is_some());
        assert!(rust_log_directive.is_some());
    }

    #[test]
    fn conduit_trace_takes_precedence_over_rust_log() {
        let targets: Option<Targets> = tracing_targets_from_env(|name: &str| match name {
            CONDUIT_TRACE_ENV => Some(String::from("off")),
            RUST_LOG_ENV => Some(String::from("trace")),
            _ => None,
        })
        .expect("env should parse");

        assert!(targets.is_none());
    }

    #[test]
    fn invalid_tracing_filter_reports_env_name() {
        let err: CliError = tracing_targets_from_value(RUST_LOG_ENV, "conduit.runtime=verbose")
            .expect_err("invalid tracing filter should fail");

        assert!(err.to_string().contains(RUST_LOG_ENV));
        assert!(err.to_string().contains("invalid tracing filter"));
    }

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
        assert!(output.contains("execution: native-registry"));
        assert!(output.contains("metadata: jsonl lifecycle, message, and queue-pressure records"));
        assert!(output.contains("source.out -> sink.in capacity=8"));
    }

    #[test]
    fn run_writes_reproducible_metadata_jsonl() {
        let output = run_workflow_json(WORKFLOW_JSON).expect("workflow should run");

        assert_eq!(output.workflow_id, "flow");
        assert_eq!(output.node_count, 2);
        assert_eq!(output.edge_count, 1);
        assert_eq!(output.record_count, 13);
        assert_eq!(
            output.summary.terminal_state(),
            WorkflowTerminalState::Completed
        );
        assert_eq!(output.summary.scheduled_node_count(), 2);
        assert_eq!(output.summary.completed_node_count(), 2);
        assert_eq!(output.summary.error_count(), 0);
        assert!(output.summary.first_error().is_none());
        assert!(
            output
                .metadata_jsonl
                .contains("\"record_type\":\"lifecycle\"")
        );
        assert!(
            output
                .metadata_jsonl
                .contains("\"record_type\":\"message\"")
        );
        assert!(
            output
                .metadata_jsonl
                .contains("\"record_type\":\"queue_pressure\"")
        );
        assert!(
            output
                .metadata_jsonl
                .contains("\"execution_id\":\"cli-run-1\"")
        );
    }

    #[test]
    fn run_json_output_reports_machine_facing_summary_fields() {
        let output = run_workflow_json(WORKFLOW_JSON).expect("workflow should run");
        let json_output = cli_run_output_to_json_string(&output, "metadata.jsonl")
            .expect("run JSON should encode");
        let value: Value = serde_json::from_str(&json_output).expect("run output should be JSON");

        assert_eq!(value["status"], "completed");
        assert_eq!(value["error"], Value::Null);
        assert_eq!(value["workflow"]["id"], "flow");
        assert_eq!(value["workflow"]["node_count"], 2);
        assert_eq!(value["workflow"]["edge_count"], 1);
        assert_eq!(value["metadata"]["path"], "metadata.jsonl");
        assert_eq!(value["metadata"]["record_count"], 13);
        assert_eq!(value["summary"]["terminal_state"], "completed");
        assert_eq!(value["summary"]["scheduled_node_count"], 2);
        assert_eq!(value["summary"]["completed_node_count"], 2);
        assert_eq!(value["summary"]["failed_node_count"], 0);
        assert_eq!(value["summary"]["cancelled_node_count"], 0);
        assert_eq!(value["summary"]["pending_node_count"], 0);
        assert_eq!(value["summary"]["observed_message_count"], 0);
        assert_eq!(value["summary"]["error_count"], 0);
        assert_eq!(value["summary"]["first_error"], Value::Null);
        assert_eq!(value["summary"]["deadlock_diagnostic"], Value::Null);
    }

    #[test]
    fn run_json_command_writes_metadata_and_returns_json() {
        let written_metadata: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let written_metadata_for_closure: Arc<Mutex<Option<String>>> = written_metadata.clone();
        let args: Vec<String> = vec![
            String::from("run"),
            String::from("--json"),
            String::from("workflow.json"),
            String::from("metadata.jsonl"),
        ];

        let output = command_output(
            &args,
            |path: &Path| {
                assert_eq!(path, Path::new("workflow.json"));
                Ok(String::from(WORKFLOW_JSON))
            },
            |path: &Path, contents: &str| {
                assert_eq!(path, Path::new("metadata.jsonl"));
                *written_metadata_for_closure
                    .lock()
                    .expect("metadata write lock should not be poisoned") =
                    Some(contents.to_owned());
                Ok(())
            },
        )
        .expect("run command should succeed")
        .expect("run command should produce output");
        let value: Value = serde_json::from_str(&output).expect("run output should be JSON");
        let metadata = written_metadata
            .lock()
            .expect("metadata write lock should not be poisoned")
            .clone()
            .expect("metadata should be written");

        assert_eq!(value["status"], "completed");
        assert_eq!(value["metadata"]["record_count"], metadata.lines().count());
        assert!(metadata.contains("\"record_type\":\"lifecycle\""));
    }

    #[test]
    fn cli_error_json_uses_stable_machine_fields() {
        let value: Value = conduit_error_to_json_value(&ConduitError::execution("boom"));

        assert_eq!(value["code"], "CDT-EXEC-001");
        assert_eq!(
            value["message"],
            "CDT-EXEC-001: node execution failed: boom"
        );
        assert_eq!(value["visibility"], "internal");
        assert_eq!(value["retry_disposition"], "unknown");
    }

    #[test]
    fn native_linear_etl_example_runs_through_native_registry() {
        let output =
            run_workflow_json(NATIVE_LINEAR_ETL_WORKFLOW_JSON).expect("example should run");

        assert_eq!(output.workflow_id, "native-linear-etl");
        assert_eq!(output.node_count, 3);
        assert_eq!(output.edge_count, 2);
        assert_eq!(output.record_count, 24);
        assert!(output.metadata_jsonl.contains("\"node_id\":\"transform\""));
        assert!(output.metadata_jsonl.contains("\"port_id\":\"cleaned\""));
    }

    #[test]
    fn validate_rejects_invalid_workflow_json() {
        let err = validate_workflow_json("{").expect_err("malformed JSON should fail");

        assert!(matches!(err, CliError::WorkflowJson(_)));
    }
}
