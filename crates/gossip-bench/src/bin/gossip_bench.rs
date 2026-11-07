//! `gossip-bench --regime regimes/low-3.json --seed 1 [--out r.json]`: run
//! one regime under one seed and write the result JSON. `judge/matrix.py`
//! runs it across regimes and seeds.

use std::io::{self, Write};
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use gossip_bench::harness::{self, Regime};

#[derive(Parser)]
#[command(
    name = "gossip-bench",
    about = "Run one study regime and emit its JSON result."
)]
struct Cli {
    /// Regime data file (`crates/gossip-bench/regimes/*.json`).
    #[arg(long)]
    regime: PathBuf,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// Output file; stdout when omitted.
    #[arg(long)]
    out: Option<PathBuf>,
    /// Exit non-zero unless every health invariant held (the CI smoke).
    #[arg(long)]
    require_valid: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let result = harness::run(&Regime::load(&cli.regime)?, cli.seed)?;
    let mut json = serde_json::to_string_pretty(&result).context("serialize study result")?;
    json.push('\n');
    match &cli.out {
        Some(path) => {
            std::fs::write(path, json).with_context(|| format!("write {}", path.display()))?
        }
        None => io::stdout()
            .lock()
            .write_all(json.as_bytes())
            .context("write stdout")?,
    }
    if cli.require_valid {
        result.require_valid()?;
    }
    Ok(())
}
