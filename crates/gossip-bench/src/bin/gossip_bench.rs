//! `gossip-bench` — run a scenario JSON spec on the in-process simulator
//! and emit a JSON result on stdout.
//!
//! Usage:
//!   gossip-bench run --scenario path/to/scenario.json > result.json
//!
//! The harness Python script (`bench/plot.py`) drives this binary across
//! a matrix of scenario specs and produces matplotlib/seaborn plots from
//! the concatenated results.

use std::fs;
use std::io::{self, Read, Write};
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use gossip_bench::{Scenario, scenarios};

#[derive(Parser, Debug)]
#[command(
    name = "gossip-bench",
    about = "Run gossip simulator scenarios and emit JSON metrics."
)]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Run one scenario and emit a single JSON result on stdout.
    Run {
        /// Path to scenario JSON. `-` reads stdin.
        #[arg(short, long, default_value = "-")]
        scenario: PathBuf,
    },
    /// Read a JSONL stream of scenarios (one per line) on stdin and emit
    /// matching JSONL results on stdout. Useful for matrix sweeps where
    /// the Python harness generates the spec stream.
    Batch,
    /// Print a starter scenario JSON to stdout.
    Example {
        /// Which built-in example to print. Options: convergence,
        /// loss, partition, sustained, scale_n, fanout_sweep.
        #[arg(short, long, default_value = "convergence")]
        kind: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .context("build tokio current-thread runtime")?;
    runtime.block_on(async_main(cli))
}

async fn async_main(cli: Cli) -> Result<()> {
    match cli.command {
        Cmd::Run { scenario } => {
            let raw = read_path_or_stdin(&scenario)?;
            let scenario: Scenario = serde_json::from_str(&raw).context("parse scenario JSON")?;
            let result = scenarios::run_scenario(scenario).await?;
            let stdout = io::stdout();
            let mut out = stdout.lock();
            serde_json::to_writer(&mut out, &result).context("serialize result")?;
            writeln!(out).ok();
            Ok(())
        }
        Cmd::Batch => {
            let stdin = io::stdin();
            let mut input = String::new();
            stdin
                .lock()
                .read_to_string(&mut input)
                .context("read stdin")?;
            let stdout = io::stdout();
            let mut out = stdout.lock();
            for (line_no, line) in input.lines().enumerate() {
                let line = line.trim();
                if line.is_empty() || line.starts_with('#') {
                    continue;
                }
                let scenario: Scenario = serde_json::from_str(line)
                    .with_context(|| format!("parse scenario at line {}", line_no + 1))?;
                let result = scenarios::run_scenario(scenario).await?;
                serde_json::to_writer(&mut out, &result)?;
                writeln!(out).ok();
            }
            Ok(())
        }
        Cmd::Example { kind } => {
            let json =
                example_scenario(&kind).with_context(|| format!("unknown example kind: {kind}"))?;
            let stdout = io::stdout();
            let mut out = stdout.lock();
            out.write_all(json.as_bytes())?;
            writeln!(out).ok();
            Ok(())
        }
    }
}

fn read_path_or_stdin(path: &PathBuf) -> Result<String> {
    if path.as_os_str() == "-" {
        let mut s = String::new();
        io::stdin()
            .lock()
            .read_to_string(&mut s)
            .context("read stdin")?;
        Ok(s)
    } else {
        fs::read_to_string(path).with_context(|| format!("read {}", path.display()))
    }
}

fn example_scenario(kind: &str) -> Option<String> {
    use std::time::Duration;

    use gossip_bench::scenario::{
        LinkAction, LinkModel, NetworkModel, Scenario, ScenarioKind, ScheduledNetworkChange,
        Workload,
    };

    let mut base = Scenario {
        name: "example".to_string(),
        nodes: 8,
        fanout: 3,
        tick_interval: Duration::from_millis(100),
        duration: Duration::from_secs(5),
        sample_interval: Duration::from_millis(100),
        network: NetworkModel::default(),
        workload: Workload::SingleWrite {
            node: 0,
            hits: 10,
            at: Duration::from_millis(100),
        },
        kind: ScenarioKind::Convergence,
        seed: 0xDEAD_BEEF,
        cell_capacity: 256,
        max_cells_per_tick: 256,
        target_err_bps: None,
        min_emit_interval: None,
    };

    match kind {
        "convergence" => {}
        "loss" => {
            base.name = "loss_30pct".to_string();
            base.network.uniform_loss = 0.3;
            base.kind = ScenarioKind::LossTolerance;
            base.duration = Duration::from_secs(10);
        }
        "partition" => {
            base.name = "partition_then_heal".to_string();
            base.nodes = 6;
            base.kind = ScenarioKind::Partition;
            base.duration = Duration::from_secs(20);
            // Cut nodes 0..3 from nodes 3..6 from the start.
            let mut links = Vec::new();
            for from in 0..3 {
                for to in 3..6 {
                    links.push(LinkModel {
                        from,
                        to,
                        action: LinkAction::Block,
                    });
                    links.push(LinkModel {
                        from: to,
                        to: from,
                        action: LinkAction::Block,
                    });
                }
            }
            base.network.links = links.clone();
            // Heal at t=10s.
            base.network.schedule = vec![ScheduledNetworkChange {
                at: Duration::from_secs(10),
                apply: {
                    let mut heal = Vec::new();
                    for from in 0..3 {
                        for to in 3..6 {
                            heal.push(LinkModel {
                                from,
                                to,
                                action: LinkAction::Pass,
                            });
                            heal.push(LinkModel {
                                from: to,
                                to: from,
                                action: LinkAction::Pass,
                            });
                        }
                    }
                    heal
                },
            }];
        }
        "sustained" => {
            base.name = "sustained_4_sources".to_string();
            base.kind = ScenarioKind::Staleness;
            base.workload = Workload::Sustained {
                sources: vec![0, 1, 2, 3],
                per_tick: 1,
                rule_limit: None,
            };
            base.duration = Duration::from_secs(10);
        }
        "scale_n" => {
            base.name = "scale_n_32".to_string();
            base.kind = ScenarioKind::ScaleN;
            base.nodes = 32;
            base.duration = Duration::from_secs(10);
        }
        "fanout_sweep" => {
            base.name = "fanout_5".to_string();
            base.fanout = 5;
        }
        "coverage_fanout" => {
            base.name = "coverage_fanout_n32".to_string();
            base.kind = ScenarioKind::CoverageFanout;
            base.nodes = 32;
            // `fanout` is the floor; the runtime scales the actual pick to
            // the coverage threshold `⌈ln(31)+4⌉ = 8` (31 peers at n=32),
            // well above this floor of 1.
            base.fanout = 1;
            base.duration = Duration::from_secs(5);
            // 256 distinct keys at once → 256 dirty cells. The burst rides
            // one fat frame; the headline `peak_effective_fanout` should sit
            // at the coverage threshold regardless of this volume. The plot
            // harness sweeps `nodes` (16/64/256) to show the pick tracking
            // `⌈ln(n)+c⌉`, and sweeps `cells` to show volume-independence.
            base.workload = Workload::DistinctKeyBurst {
                node: 0,
                cells: 256,
                at: Duration::from_millis(100),
            };
        }
        "error_budget" => {
            base.name = "error_budget_100bps".to_string();
            base.kind = ScenarioKind::ErrorBudget;
            base.nodes = 16;
            base.duration = Duration::from_secs(5);
            base.target_err_bps = Some(100);
            base.workload = Workload::Sustained {
                sources: (0..16).collect(),
                per_tick: 10,
                rule_limit: Some(1600),
            };
        }
        "min_emit_clamp" => {
            base.name = "min_emit_clamp_5ms".to_string();
            base.kind = ScenarioKind::MinEmitClamp;
            base.nodes = 8;
            base.duration = Duration::from_secs(1);
            base.min_emit_interval = Some(Duration::from_millis(5));
            base.workload = Workload::BurstCompressed {
                node: 0,
                hits: 10_000,
                at: Duration::from_millis(0),
                burst_span: Duration::from_millis(50),
            };
        }
        "heartbeat_threshold_mix" => {
            base.name = "heartbeat_threshold_mix".to_string();
            base.kind = ScenarioKind::HeartbeatThresholdMix;
            base.nodes = 8;
            base.duration = Duration::from_secs(5);
            base.workload = Workload::TwoRule {
                hot_node: 0,
                hot_per_tick: 200,
                hot_limit: 1_000,
                cold_node: 1,
                cold_per_interval: 1,
                cold_interval: Duration::from_secs(1),
                cold_limit: 1_000_000,
            };
        }
        _ => return None,
    }
    serde_json::to_string_pretty(&base).ok()
}
