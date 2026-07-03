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
        /// single-binary topology. `serve` already embeds the event
        /// workers (outbox relay, webhook dispatcher) alongside the API
        /// server. TODO(M3): once workers can be split out into their own
        /// processes, plain `serve` will run only the API server and this
        /// flag will opt back into embedding; today both forms behave
        /// identically.
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

    /// Search tables, views, and namespaces on a running server.
    Search {
        /// The query text (name, column, comment, ... fragments).
        #[arg(value_name = "QUERY")]
        query: String,

        /// Restrict to one warehouse by name.
        #[arg(long)]
        warehouse: Option<String>,

        /// Comma-separated asset types to include: table, view, namespace.
        #[arg(long = "type", value_name = "TYPES")]
        kinds: Option<String>,

        /// Maximum number of results (1-100).
        #[arg(long, default_value_t = 20)]
        limit: i64,

        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,

        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// Manage RBAC roles on a running server.
    #[command(subcommand)]
    Role(RoleCommand),

    /// Manage RBAC grants on a running server.
    #[command(subcommand)]
    Grant(GrantCommand),

    /// Follow the catalog event feed on a running server.
    #[command(subcommand)]
    Events(EventsCommand),
}

#[derive(Debug, Subcommand)]
enum EventsCommand {
    /// Print events as `CloudEvents` JSON lines and follow the feed.
    ///
    /// Starts at the current end of the feed by default (like `tail -f`);
    /// use --from-start or --after to replay history. Stop with ctrl-c.
    Tail {
        /// Start from the beginning of the feed instead of the end.
        #[arg(long, conflicts_with = "after")]
        from_start: bool,

        /// Start after this cursor (an event id from a previous run).
        #[arg(long, value_name = "CURSOR")]
        after: Option<String>,

        /// Only these event types, comma-separated
        /// (e.g. com.meridian.table.committed,com.meridian.table.created).
        #[arg(long, value_name = "TYPES")]
        types: Option<String>,

        /// Poll interval in seconds while waiting for new events.
        #[arg(long, default_value_t = 2, value_name = "SECONDS")]
        interval: u64,

        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,

        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum RoleCommand {
    /// List roles.
    List {
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,

        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// Create a role.
    Create {
        /// Role name.
        #[arg(value_name = "NAME")]
        name: String,

        /// Optional human description.
        #[arg(long)]
        description: Option<String>,

        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,

        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum GrantCommand {
    /// Create a grant: a privilege on a securable for a role or principal.
    Add {
        /// Privilege to grant, e.g. READ, COMMIT, `CREATE_TABLE`.
        #[arg(long, value_name = "PRIVILEGE")]
        privilege: String,

        /// Grantee role name (exactly one of --role / --principal).
        #[arg(long, value_name = "NAME")]
        role: Option<String>,

        /// Grantee principal id (exactly one of --role / --principal).
        #[arg(long, value_name = "PRINCIPAL_ID")]
        principal: Option<String>,

        /// Warehouse the securable lives in (the securable itself when
        /// neither --namespace nor --table is given).
        #[arg(long)]
        warehouse: String,

        /// Namespace (dot-separated); the securable when --table is absent.
        #[arg(long, value_name = "NAMESPACE")]
        namespace: Option<String>,

        /// Table name; makes the securable a table (requires --namespace).
        #[arg(long, value_name = "TABLE")]
        table: Option<String>,

        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,

        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// List grants.
    List {
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,

        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// Delete a grant by id.
    Rm {
        /// Grant id (from `meridian grant list`).
        #[arg(value_name = "GRANT_ID")]
        id: String,

        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,

        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
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
        Command::Search {
            query,
            warehouse,
            kinds,
            limit,
            server,
            token,
        } => run_async(run_search(query, warehouse, kinds, limit, server, token)),
        Command::Role(command) => run_async(run_role(command)),
        Command::Grant(command) => run_async(run_grant(command)),
        Command::Events(command) => run_async(run_events(command)),
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

/// `meridian search <QUERY>` — ranked search over tables/views/namespaces.
async fn run_search(
    query: String,
    warehouse: Option<String>,
    kinds: Option<String>,
    limit: i64,
    server: String,
    token: Option<String>,
) -> Result<(), CliError> {
    let body = client::search(
        &server,
        token.as_deref(),
        &query,
        warehouse.as_deref(),
        kinds.as_deref(),
        limit,
    )
    .await?;
    let results = body
        .get("results")
        .and_then(Value::as_array)
        .ok_or_else(|| CliError("malformed response: missing results".to_owned()))?;
    let rows: Vec<Vec<String>> = results
        .iter()
        .map(|hit| {
            let kind = hit.get("type").and_then(Value::as_str).unwrap_or("?");
            let warehouse = hit.get("warehouse").and_then(Value::as_str).unwrap_or("?");
            let namespace =
                hit.get("namespace")
                    .and_then(Value::as_array)
                    .map_or_else(String::new, |levels| {
                        levels
                            .iter()
                            .map(|l| l.as_str().unwrap_or("?"))
                            .collect::<Vec<_>>()
                            .join(".")
                    });
            let name = hit.get("name").and_then(Value::as_str).unwrap_or("?");
            let ident = if kind == "namespace" || namespace.is_empty() {
                format!("{warehouse}.{namespace}")
            } else {
                format!("{warehouse}.{namespace}.{name}")
            };
            let snippet = hit
                .get("snippet")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned();
            vec![kind.to_owned(), ident, snippet]
        })
        .collect();
    print!(
        "{}",
        client::render_table(&["TYPE", "IDENTIFIER", "MATCH"], &rows)
    );
    if let Some(next) = body.get("next_page_token").and_then(Value::as_str) {
        println!("(more results available; refine the query or raise --limit; token: {next})");
    }
    Ok(())
}

async fn run_role(command: RoleCommand) -> Result<(), CliError> {
    match command {
        RoleCommand::List { server, token } => {
            let body = client::role_list(&server, token.as_deref()).await?;
            let roles = body
                .get("roles")
                .and_then(Value::as_array)
                .ok_or_else(|| CliError("malformed response: missing roles".to_owned()))?;
            let rows: Vec<Vec<String>> = roles
                .iter()
                .map(|role| {
                    vec![
                        role.get("name")
                            .and_then(Value::as_str)
                            .unwrap_or("?")
                            .to_owned(),
                        if role.get("built_in").and_then(Value::as_bool) == Some(true) {
                            "yes".to_owned()
                        } else {
                            "no".to_owned()
                        },
                        role.get("description")
                            .and_then(Value::as_str)
                            .unwrap_or("-")
                            .to_owned(),
                    ]
                })
                .collect();
            print!(
                "{}",
                client::render_table(&["NAME", "BUILT-IN", "DESCRIPTION"], &rows)
            );
            Ok(())
        }
        RoleCommand::Create {
            name,
            description,
            server,
            token,
        } => {
            let created =
                client::role_create(&server, token.as_deref(), &name, description.as_deref())
                    .await?;
            let id = created.get("id").and_then(Value::as_str).unwrap_or("?");
            println!("created role {name} (id {id})");
            Ok(())
        }
    }
}

async fn run_grant(command: GrantCommand) -> Result<(), CliError> {
    match command {
        GrantCommand::Add {
            privilege,
            role,
            principal,
            warehouse,
            namespace,
            table,
            server,
            token,
        } => {
            let securable_type = match (&namespace, &table) {
                (None, None) => "warehouse",
                (Some(_), None) => "namespace",
                (Some(_), Some(_)) => "table",
                (None, Some(_)) => {
                    return Err(CliError("--table requires --namespace".to_owned()));
                }
            };
            let namespace_levels = namespace.as_deref().map(parse_namespace_arg).transpose()?;
            let body = serde_json::json!({
                "privilege": privilege,
                "role": role,
                "principal_id": principal,
                "securable": {
                    "type": securable_type,
                    "warehouse": warehouse,
                    "namespace": namespace_levels,
                    "table": table,
                },
            });
            let created = client::grant_add(&server, token.as_deref(), &body).await?;
            let id = created.get("id").and_then(Value::as_str).unwrap_or("?");
            println!("created grant {id}");
            Ok(())
        }
        GrantCommand::List { server, token } => {
            let body = client::grant_list(&server, token.as_deref()).await?;
            let grants = body
                .get("grants")
                .and_then(Value::as_array)
                .ok_or_else(|| CliError("malformed response: missing grants".to_owned()))?;
            let field = |grant: &Value, key: &str| {
                grant
                    .get(key)
                    .and_then(Value::as_str)
                    .unwrap_or("-")
                    .to_owned()
            };
            let rows: Vec<Vec<String>> = grants
                .iter()
                .map(|grant| {
                    let grantee = grant.get("role").and_then(Value::as_str).map_or_else(
                        || format!("principal:{}", field(grant, "principal_id")),
                        |role| format!("role:{role}"),
                    );
                    vec![
                        field(grant, "id"),
                        field(grant, "privilege"),
                        grantee,
                        field(grant, "securable_type"),
                        field(grant, "securable_id"),
                    ]
                })
                .collect();
            print!(
                "{}",
                client::render_table(
                    &["ID", "PRIVILEGE", "GRANTEE", "SECURABLE", "SECURABLE ID"],
                    &rows
                )
            );
            Ok(())
        }
        GrantCommand::Rm { id, server, token } => {
            client::grant_remove(&server, token.as_deref(), &id).await?;
            println!("deleted grant {id}");
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

async fn run_events(command: EventsCommand) -> Result<(), CliError> {
    match command {
        EventsCommand::Tail {
            from_start,
            after,
            types,
            interval,
            server,
            token,
        } => {
            // Resolution order: explicit cursor > --from-start (empty
            // cursor = beginning) > the "latest" sentinel (server resolves
            // the current end of the feed).
            let mut cursor = match after {
                Some(after) => after,
                None if from_start => String::new(),
                None => "latest".to_owned(),
            };
            let poll = std::time::Duration::from_secs(interval.max(1));
            loop {
                let body =
                    client::events_list(&server, token.as_deref(), &cursor, types.as_deref(), 500)
                        .await?;
                let events = body
                    .get("events")
                    .and_then(Value::as_array)
                    .ok_or_else(|| CliError("malformed response: missing events".to_owned()))?;
                for event in events {
                    // One compact CloudEvents JSON object per line.
                    println!("{event}");
                }
                cursor = body
                    .get("next_cursor")
                    .and_then(Value::as_str)
                    .ok_or_else(|| CliError("malformed response: missing next_cursor".to_owned()))?
                    .to_owned();
                if events.is_empty() {
                    tokio::time::sleep(poll).await;
                }
            }
        }
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

        // Bootstrap the first administrator (idempotent): grants the
        // built-in admin role to the configured identity so a
        // deny-by-default oidc deployment has a way in.
        if let Some(bootstrap) = &config.auth.bootstrap_admin {
            meridian_store::rbac::bootstrap_admin(
                &pool,
                meridian_store::tenancy::default_workspace_id(),
                &bootstrap.issuer,
                &bootstrap.subject,
            )
            .await?;
        }

        meridian_server::serve(config, pool).await
    })
}
