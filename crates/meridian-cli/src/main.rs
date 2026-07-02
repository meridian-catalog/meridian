//! The `meridian` binary.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use meridian_common::{AppConfig, MeridianError};
use serde_json::Value;

mod client;

use client::CliError;

/// Default server URL for the admin subcommands.
const DEFAULT_SERVER: &str = "http://127.0.0.1:8181";

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

    /// Manage warehouses (storage roots) on a running server.
    #[command(subcommand)]
    Warehouse(WarehouseCommand),

    /// Manage namespaces on a running server.
    #[command(subcommand)]
    Namespace(NamespaceCommand),

    /// Inspect tables on a running server.
    #[command(subcommand)]
    Table(TableCommand),
}

#[derive(Debug, Subcommand)]
enum TableCommand {
    /// List tables of a namespace.
    List {
        /// Namespace to list (dot-separated for multi-level).
        #[arg(value_name = "NAMESPACE")]
        namespace: String,

        /// Warehouse the namespace belongs to.
        #[arg(long)]
        warehouse: String,

        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
    },

    /// Show a table's identity, pointer, and key metadata fields.
    Describe {
        /// The table as namespace.table (dot-separated).
        #[arg(value_name = "NAMESPACE.TABLE")]
        table: String,

        /// Warehouse the table belongs to.
        #[arg(long)]
        warehouse: String,

        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
    },
}

#[derive(Debug, Subcommand)]
enum WarehouseCommand {
    /// Register a new warehouse.
    Create {
        /// Warehouse name; doubles as the Iceberg REST catalog prefix.
        #[arg(long)]
        name: String,

        /// Storage root URI, e.g. `s3://bucket/prefix`.
        #[arg(long, value_name = "URI")]
        storage_root: String,

        /// Non-secret storage option as key=value (repeatable).
        #[arg(long = "storage-option", value_name = "KEY=VALUE")]
        storage_options: Vec<String>,

        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
    },

    /// List registered warehouses.
    List {
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
    },
}

#[derive(Debug, Subcommand)]
enum NamespaceCommand {
    /// Create a namespace (multi-level as dot-separated, e.g. accounting.tax).
    Create {
        /// The namespace, levels separated by dots.
        #[arg(value_name = "NAMESPACE")]
        namespace: String,

        /// Warehouse the namespace belongs to.
        #[arg(long)]
        warehouse: String,

        /// Namespace property as key=value (repeatable).
        #[arg(long = "property", value_name = "KEY=VALUE")]
        properties: Vec<String>,

        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
    },

    /// List namespaces, optionally underneath a parent namespace.
    List {
        /// Warehouse to list namespaces of.
        #[arg(long)]
        warehouse: String,

        /// Parent namespace (dot-separated) to list underneath.
        #[arg(long, value_name = "NAMESPACE")]
        parent: Option<String>,

        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let result = match cli.command {
        Command::Serve { all_in_one, config } => {
            run_serve(all_in_one, config.as_deref()).map_err(|e| CliError(e.to_string()))
        }
        Command::Warehouse(command) => run_async(run_warehouse(command)),
        Command::Namespace(command) => run_async(run_namespace(command)),
        Command::Table(command) => run_async(run_table(command)),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

/// Runs an admin-command future on a fresh current-thread runtime.
fn run_async(future: impl Future<Output = Result<(), CliError>>) -> Result<(), CliError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| CliError(format!("failed to start tokio runtime: {e}")))?
        .block_on(future)
}

/// Splits a dot-separated namespace argument into levels.
fn parse_namespace_arg(raw: &str) -> Result<Vec<String>, CliError> {
    let levels: Vec<String> = raw.split('.').map(str::to_owned).collect();
    if levels.iter().any(String::is_empty) {
        return Err(CliError(format!(
            "invalid namespace {raw:?}: levels must be non-empty (dot-separated)"
        )));
    }
    Ok(levels)
}

/// Parses repeated `key=value` flags.
fn parse_pairs(raw: &[String]) -> Result<Vec<(String, String)>, CliError> {
    raw.iter().map(|kv| client::parse_key_value(kv)).collect()
}

async fn run_warehouse(command: WarehouseCommand) -> Result<(), CliError> {
    match command {
        WarehouseCommand::Create {
            name,
            storage_root,
            storage_options,
            server,
        } => {
            let options = parse_pairs(&storage_options)?;
            let created = client::warehouse_create(&server, &name, &storage_root, &options).await?;
            let id = created.get("id").and_then(Value::as_str).unwrap_or("?");
            println!("created warehouse {name} (id {id})");
            Ok(())
        }
        WarehouseCommand::List { server } => {
            let body = client::warehouse_list(&server).await?;
            let warehouses = body
                .get("warehouses")
                .and_then(Value::as_array)
                .ok_or_else(|| CliError("malformed response: missing warehouses".to_owned()))?;
            let rows: Vec<Vec<String>> = warehouses
                .iter()
                .map(|w| {
                    vec![
                        w.get("name")
                            .and_then(Value::as_str)
                            .unwrap_or("?")
                            .to_owned(),
                        w.get("storage_root")
                            .and_then(Value::as_str)
                            .unwrap_or("?")
                            .to_owned(),
                        w.get("created_at")
                            .and_then(Value::as_str)
                            .unwrap_or("?")
                            .to_owned(),
                    ]
                })
                .collect();
            print!(
                "{}",
                client::render_table(&["NAME", "STORAGE ROOT", "CREATED"], &rows)
            );
            Ok(())
        }
    }
}

async fn run_namespace(command: NamespaceCommand) -> Result<(), CliError> {
    match command {
        NamespaceCommand::Create {
            namespace,
            warehouse,
            properties,
            server,
        } => {
            let levels = parse_namespace_arg(&namespace)?;
            let props = parse_pairs(&properties)?;
            client::namespace_create(&server, &warehouse, &levels, &props).await?;
            println!("created namespace {namespace} in warehouse {warehouse}");
            Ok(())
        }
        NamespaceCommand::List {
            warehouse,
            parent,
            server,
        } => {
            let parent_levels = parent.as_deref().map(parse_namespace_arg).transpose()?;
            let body =
                client::namespace_list(&server, &warehouse, parent_levels.as_deref()).await?;
            let namespaces = body
                .get("namespaces")
                .and_then(Value::as_array)
                .ok_or_else(|| CliError("malformed response: missing namespaces".to_owned()))?;
            let rows: Vec<Vec<String>> = namespaces
                .iter()
                .map(|ns| {
                    let joined = ns.as_array().map_or_else(
                        || "?".to_owned(),
                        |levels| {
                            levels
                                .iter()
                                .map(|l| l.as_str().unwrap_or("?"))
                                .collect::<Vec<_>>()
                                .join(".")
                        },
                    );
                    vec![joined]
                })
                .collect();
            print!("{}", client::render_table(&["NAMESPACE"], &rows));
            Ok(())
        }
    }
}

async fn run_table(command: TableCommand) -> Result<(), CliError> {
    match command {
        TableCommand::List {
            namespace,
            warehouse,
            server,
        } => {
            let levels = parse_namespace_arg(&namespace)?;
            let body = client::table_list(&server, &warehouse, &levels).await?;
            let identifiers = body
                .get("identifiers")
                .and_then(Value::as_array)
                .ok_or_else(|| CliError("malformed response: missing identifiers".to_owned()))?;
            let rows: Vec<Vec<String>> = identifiers
                .iter()
                .map(|ident| {
                    let ns = ident
                        .get("namespace")
                        .and_then(Value::as_array)
                        .map_or_else(
                            || "?".to_owned(),
                            |levels| {
                                levels
                                    .iter()
                                    .map(|l| l.as_str().unwrap_or("?"))
                                    .collect::<Vec<_>>()
                                    .join(".")
                            },
                        );
                    let name = ident.get("name").and_then(Value::as_str).unwrap_or("?");
                    vec![ns, name.to_owned()]
                })
                .collect();
            print!("{}", client::render_table(&["NAMESPACE", "TABLE"], &rows));
            Ok(())
        }
        TableCommand::Describe {
            table,
            warehouse,
            server,
        } => {
            let mut levels = parse_namespace_arg(&table)?;
            if levels.len() < 2 {
                return Err(CliError(format!(
                    "invalid table {table:?}: expected namespace.table"
                )));
            }
            let name = levels.pop().unwrap_or_default();
            let body = client::table_load(&server, &warehouse, &levels, &name).await?;

            let metadata = body.get("metadata").cloned().unwrap_or(Value::Null);
            let string_at = |value: &Value, key: &str| {
                value.get(key).map_or_else(|| "-".to_owned(), render_scalar)
            };
            let snapshot_count = metadata
                .get("snapshots")
                .and_then(Value::as_array)
                .map_or(0, Vec::len);

            let rows = vec![
                vec!["table".to_owned(), format!("{}.{name}", levels.join("."))],
                vec![
                    "metadata-location".to_owned(),
                    string_at(&body, "metadata-location"),
                ],
                vec!["table-uuid".to_owned(), string_at(&metadata, "table-uuid")],
                vec![
                    "format-version".to_owned(),
                    string_at(&metadata, "format-version"),
                ],
                vec!["location".to_owned(), string_at(&metadata, "location")],
                vec![
                    "current-schema-id".to_owned(),
                    string_at(&metadata, "current-schema-id"),
                ],
                vec![
                    "current-snapshot-id".to_owned(),
                    string_at(&metadata, "current-snapshot-id"),
                ],
                vec!["snapshots".to_owned(), snapshot_count.to_string()],
                vec![
                    "last-updated-ms".to_owned(),
                    string_at(&metadata, "last-updated-ms"),
                ],
            ];
            print!("{}", client::render_table(&["FIELD", "VALUE"], &rows));
            Ok(())
        }
    }
}

/// Renders a scalar JSON value without quotes; non-scalars fall back to
/// compact JSON.
fn render_scalar(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => "-".to_owned(),
        other => other.to_string(),
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
