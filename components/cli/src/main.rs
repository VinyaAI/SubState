//! SubState CLI (`substate`).
//!
//! Company-shaped entrypoints:
//! - `substate init` — discover sources and write a sync schema YAML
//! - `substate serve` — headless sidecar (source follows + HTTP/WS)
//! - `substate shell` — same boot, then interactive `cds>` debug REPL

mod boot;
mod config;
mod dispatch;
mod hub;
mod init;
mod shell;
mod ws;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "substate", about = "SubState sync engine sidecar")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Discover Postgres/Kafka and write a reviewable sync schema YAML.
    Init {
        /// Output path (default: SCHEMA_PATH or ./schema.yaml).
        #[arg(long)]
        out: Option<PathBuf>,
        /// Accept all proposals without interactive prompts.
        #[arg(long)]
        defaults: bool,
        /// Kafka messages to sample per topic (default: 20).
        #[arg(long, default_value_t = kafka_source::DEFAULT_SAMPLE_SIZE)]
        sample_size: usize,
    },
    /// Run headless: source follows + `/health`, `/v1/cds`, `/v1/sync`, `/v1/ingest`.
    Serve,
    /// Boot the engine, then drop into the interactive debug shell.
    Shell,
}

#[tokio::main]
async fn main() -> Result<()> {
    boot::load_dotenv();
    boot::init_tracing();

    let cli = Cli::parse();
    match cli.command {
        Command::Init {
            out,
            defaults,
            sample_size,
        } => {
            let options = init::InitOptions::from_env_and_flags(out, defaults, sample_size);
            init::run(options).await
        }
        Command::Serve => run_serve().await,
        Command::Shell => run_shell().await,
    }
}

async fn run_serve() -> Result<()> {
    let runtime = boot::start().await?;
    tracing::info!(
        bind_addr = %runtime.bind_addr,
        "substate serve ready (Ctrl+C to stop)"
    );
    // Park forever while background follows + Axum tasks run.
    std::future::pending::<()>().await;
    Ok(())
}

async fn run_shell() -> Result<()> {
    let runtime = boot::start().await?;
    shell::run(runtime.hub).await?;
    Ok(())
}
