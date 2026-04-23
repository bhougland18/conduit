//! Temporary CLI entrypoint for the Conduit scaffold.

use conduit_core::{
    NodeExecutor, Result,
    context::{ExecutionMetadata, NodeContext},
};
use conduit_engine::run_workflow;
use conduit_types::ExecutionId;
use conduit_workflow::WorkflowDefinition;

struct PrintExecutor;

impl NodeExecutor for PrintExecutor {
    fn run(&self, ctx: &NodeContext) -> Result<()> {
        println!(
            "running workflow={} execution={} node={}",
            ctx.workflow_id(),
            ctx.execution().execution_id(),
            ctx.node_id()
        );
        Ok(())
    }
}

fn main() -> Result<()> {
    let workflow: WorkflowDefinition = WorkflowDefinition::empty("conduit-scaffold")?;
    let execution: ExecutionMetadata =
        ExecutionMetadata::first_attempt(ExecutionId::new("scaffold-run")?);
    let executor: PrintExecutor = PrintExecutor;
    run_workflow(&workflow, &execution, &executor)?;
    println!("conduit workspace scaffold is ready");
    Ok(())
}
