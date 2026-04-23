//! Temporary CLI entrypoint for the Conduit scaffold.

use conduit_core::{NodeContext, NodeExecutor, Result};
use conduit_engine::run_workflow;
use conduit_types::IdentifierError;
use conduit_workflow::WorkflowDefinition;

struct PrintExecutor;

impl NodeExecutor for PrintExecutor {
    fn run(&self, ctx: &NodeContext) -> Result<()> {
        println!("running workflow={} node={}", ctx.workflow_id, ctx.node_id);
        Ok(())
    }
}

fn main() -> Result<()> {
    let workflow: WorkflowDefinition = WorkflowDefinition::empty("conduit-scaffold")
        .map_err(|err: IdentifierError| err.to_string())?;
    let executor: PrintExecutor = PrintExecutor;
    run_workflow(&workflow, &executor)?;
    println!("conduit workspace scaffold is ready");
    Ok(())
}
