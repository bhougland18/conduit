//! Temporary CLI entrypoint for the Conduit scaffold.

use conduit_core::{
    NodeExecutor, PortsIn, PortsOut, Result,
    context::{ExecutionMetadata, NodeContext},
};
use conduit_engine::run_workflow;
use conduit_runtime::AsupersyncRuntime;
use conduit_types::ExecutionId;
use conduit_workflow::WorkflowDefinition;
use futures::future::{Ready, ready};

struct PrintExecutor;

impl NodeExecutor for PrintExecutor {
    type RunFuture<'a> = Ready<Result<()>>;

    fn run(&self, ctx: NodeContext, inputs: PortsIn, outputs: PortsOut) -> Self::RunFuture<'_> {
        println!(
            "running workflow={} execution={} node={} inputs={} outputs={}",
            ctx.workflow_id(),
            ctx.execution().execution_id(),
            ctx.node_id(),
            inputs.port_ids().len(),
            outputs.port_ids().len()
        );
        ready(Ok(()))
    }
}

fn main() -> Result<()> {
    let runtime: AsupersyncRuntime = AsupersyncRuntime::new()?;
    let workflow: WorkflowDefinition = WorkflowDefinition::empty("conduit-scaffold")?;
    let execution: ExecutionMetadata =
        ExecutionMetadata::first_attempt(ExecutionId::new("scaffold-run")?);
    let executor: PrintExecutor = PrintExecutor;
    runtime.block_on(run_workflow(&workflow, &execution, &executor))?;
    println!("conduit workspace scaffold is ready");
    Ok(())
}
