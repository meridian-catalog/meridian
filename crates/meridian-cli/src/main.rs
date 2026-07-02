//! The `meridian` binary.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use meridian_common::{AppConfig, MeridianError};

#[derive(Debug, Parser)]
#[command(
    name = "meridian",
    version,
    about = "Meridian: an Iceberg-native data catalog",
    propagate_version = true
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run database migrations, then serve the catalog APIs.
    Serve {
        /// Run every component in this one process.
        ///
        /// Accepted for forward compatibility with the documented
        /// single-binary topology. TODO(M3): once background workers exist,
        /// plain `serve` will run only the API server and this flag will
        /// additionally embed the workers. Today there is nothing to split,
        /// so both forms behave identically.
        #[arg(long)]
        all_in_one: bool,

        /// Path to a TOML config file (default: ./meridian.toml if present).
        #[arg(long, value_name = "PATH")]
        config: Option<PathBuf>,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    match cli.command {
        Command::Serve { all_in_one, config } => match run_serve(all_in_one, config.as_deref()) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                // Telemetry may not be initialized yet (e.g. config errors),
                // so report on stderr as well as the log.
                tracing::error!(%error, "meridian serve failed");
                eprintln!("error: {error}");
                ExitCode::FAILURE
            }
        },
    }
}

fn run_serve(all_in_one: bool, config_path: Option<&std::path::Path>) -> Result<(), MeridianError> {
    let config = AppConfig::load(config_path)?;
    meridian_common::telemetry::init(&config.telemetry)?;

    if all_in_one {
        tracing::info!("--all-in-one: single-process mode (identical to plain serve in M0)");
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| MeridianError::internal("failed to start tokio runtime", e))?;

    runtime.block_on(async {
        let pool = meridian_store::connect(&config.database).await?;

        tracing::info!("applying database migrations");
        meridian_store::MIGRATOR
            .run(&pool)
            .await
            .map_err(|e| MeridianError::internal("database migration failed", e))?;
        tracing::info!("database migrations up to date");

        meridian_server::serve(config, pool).await
    })
}
