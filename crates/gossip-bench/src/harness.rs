//! Comparative study harness: a deterministic discrete-event simulation of
//! a gabion cluster under a production-shaped workload, with an oracle that
//! scores every solution on identical offered load. See the "Study harness"
//! section of the crate README for the model, the metrics, the solution
//! contract, and the limitations.

use std::rc::Rc;

use anyhow::Result;

mod driver;
#[cfg(test)]
mod fixtures;
mod net;
mod node;
mod oracle;
mod plan;
mod regime;
mod report;
mod rng;
mod workload;
mod world;

pub use regime::Regime;
pub use report::StudyResult;

/// Run `regime` once under `seed`.
pub fn run(regime: &Regime, seed: u64) -> Result<StudyResult> {
    regime.validate()?;
    let plan = plan::Plan::build(regime, seed);
    let keys = Rc::new(node::KeySpace::build(regime, seed));
    let sizing = node::Sizing::new(regime, &plan, &keys);
    let world = Rc::new(driver::world_for(regime, seed, &plan));
    let cells = sizing.cell_store_config();
    let launch = {
        let world = Rc::clone(&world);
        let keys = Rc::clone(&keys);
        move |idx| node::launch_node(&world, &keys, cells, idx, seed)
    };
    driver::Driver::new(regime, seed, plan, sizing, keys, world, launch).run()
}

#[cfg(test)]
mod tests;
