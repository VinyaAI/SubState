//! SubState CLI (`substate`).
//!
//! Company-shaped entrypoints:
//! - `substate serve` — headless sidecar (source follows + HTTP/WS)
//! - `substate shell` — same boot, then interactive `cds>` debug REPL

mod boot;
mod config;
mod dispatch;
mod hub;
mod shell;
mod ws;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "substate", about = "SubState sync engine sidecar")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
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
