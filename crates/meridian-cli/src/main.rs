//! The `meridian` binary.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use meridian_common::{AppConfig, MeridianError};
use serde_json::Value;

mod bundle;
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

    /// Inspect table health and drive autonomous maintenance (Pillar C).
    #[command(subcommand)]
    Maintenance(MaintenanceCommand),

    /// Manage catalog mirrors — external catalogs Meridian tracks (Pillar B).
    #[command(subcommand)]
    Mirror(MirrorCommand),

    /// Manage governance tags and their assignments (Pillar D).
    #[command(subcommand)]
    Tag(TagCommand),

    /// Manage governance policies — row filters, column masks, ABAC (Pillar D).
    #[command(subcommand)]
    Policy(PolicyCommand),

    /// Governance analytics: effective policy, who-can-see, coverage, drift,
    /// evidence (Pillar D).
    #[command(subcommand)]
    Govern(GovernCommand),

    /// Show the cross-catalog sprawl summary (Pillar B).
    ///
    /// Rolls up across every catalog Meridian knows (its warehouses and
    /// registered mirrors): per-source asset counts, duplicate storage
    /// locations, stale mirrors, ownership gaps, and a health roll-up.
    Sprawl {
        /// Staleness threshold in seconds (default 86400 = 24h).
        #[arg(long, value_name = "SECONDS")]
        stale_threshold_s: Option<i64>,

        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,

        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// Diff a catalog-as-code bundle against a running server (read-only).
    ///
    /// Prints a create/update/noop/would-delete report. Deletes are never
    /// applied by `apply`; they are printed here as warnings only.
    Plan {
        /// Path to the bundle YAML file.
        #[arg(short = 'f', long = "file", value_name = "PATH")]
        file: PathBuf,

        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,

        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// Reconcile a running server toward a catalog-as-code bundle.
    ///
    /// Idempotent: re-applying an unchanged bundle is a no-op. Creates and
    /// updates only — never deletes. Exits non-zero if any resource fails.
    Apply {
        /// Path to the bundle YAML file.
        #[arg(short = 'f', long = "file", value_name = "PATH")]
        file: PathBuf,

        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,

        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
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

/// Common `--warehouse NS.TABLE` addressing shared by table-scoped
/// maintenance subcommands.
#[derive(Debug, clap::Args)]
struct TableTarget {
    /// The table as namespace.table (dot-separated namespace).
    #[arg(value_name = "NAMESPACE.TABLE")]
    table: String,

    /// Warehouse the table belongs to.
    #[arg(long)]
    warehouse: String,

    /// Base URL of the Meridian server.
    #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
    server: String,

    /// Bearer token (required when the server runs auth.mode = "oidc").
    #[arg(long, value_name = "TOKEN")]
    token: Option<String>,
}

#[derive(Debug, Subcommand)]
enum MaintenanceCommand {
    /// Show a table's health score, metrics, and top recommendations.
    Health {
        #[command(flatten)]
        target: TableTarget,
    },

    /// Trigger a compaction job on a table (queued for the worker).
    Compact {
        #[command(flatten)]
        target: TableTarget,

        /// Plan only: run the executor's dry-run, staging nothing.
        #[arg(long)]
        dry_run: bool,
    },

    /// Trigger a snapshot-expiry job on a table (metadata-only).
    Expire {
        #[command(flatten)]
        target: TableTarget,
    },

    /// List maintenance jobs.
    Jobs {
        /// Filter by state (queued, running, succeeded, failed, cancelled).
        #[arg(long)]
        state: Option<String>,

        /// Maximum number of jobs to show.
        #[arg(long, default_value_t = 50)]
        limit: i64,

        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,

        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// Cancel a queued or running job by id.
    Cancel {
        /// The job id.
        #[arg(value_name = "JOB_ID")]
        id: String,

        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,

        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// List maintenance policies.
    Policies {
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,

        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// The monthly savings roll-up ("Meridian saved X bytes / Y files").
    Savings {
        /// Number of months to roll up.
        #[arg(long, default_value_t = 12)]
        months: i64,

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
enum MirrorCommand {
    /// Register a mirror of an external catalog.
    Create {
        /// Mirror name, unique per workspace.
        #[arg(long)]
        name: String,

        /// Source kind: iceberg-rest | glue.
        #[arg(long)]
        kind: String,

        /// Connection endpoint (IRC base URI, or AWS region for Glue).
        #[arg(long, value_name = "URI_OR_REGION")]
        endpoint: String,

        /// Remote catalog id within the endpoint, when applicable.
        #[arg(long, value_name = "ID")]
        remote_catalog: Option<String>,

        /// Non-secret connection option as key=value (repeatable).
        #[arg(long = "config", value_name = "KEY=VALUE")]
        config: Vec<String>,

        /// Register the mirror disabled (do not sync until enabled).
        #[arg(long)]
        disabled: bool,

        /// Desired sync cadence in seconds (default 3600).
        #[arg(long, default_value_t = 3600, value_name = "SECONDS")]
        sync_interval_s: i32,

        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,

        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// List registered mirrors with their sync status.
    List {
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,

        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// Request an immediate sync of a mirror.
    Sync {
        /// Mirror name.
        #[arg(value_name = "NAME")]
        name: String,

        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,

        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum TagCommand {
    /// Create a classification tag (`key:value`, e.g. pii:email).
    Create {
        /// Tag key, e.g. pii.
        #[arg(long)]
        key: String,
        /// Tag value, e.g. email.
        #[arg(long)]
        value: String,
        /// Optional description.
        #[arg(long)]
        description: Option<String>,
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
    /// List all tags.
    List {
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
    /// Delete a tag by id.
    Rm {
        /// Tag id.
        #[arg(value_name = "TAG_ID")]
        id: String,
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
    /// Assign a tag to a table, namespace, or column.
    Assign {
        /// Tag id to assign.
        #[arg(long)]
        tag: String,
        /// Securable kind: table | namespace | column.
        #[arg(long = "type", value_name = "KIND")]
        securable_type: String,
        /// Warehouse the securable lives in.
        #[arg(long)]
        warehouse: String,
        /// Dotted namespace.
        #[arg(long)]
        namespace: String,
        /// Table name (for table/column targets).
        #[arg(long)]
        table: Option<String>,
        /// Column name (for a column target).
        #[arg(long)]
        column: Option<String>,
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum PolicyCommand {
    /// Create a policy. `--definition` is the JSON `AbacRule` (see docs).
    Create {
        /// Policy name (unique per workspace).
        #[arg(long)]
        name: String,
        /// Kind: `row_filter` | `column_mask` | `abac`.
        #[arg(long)]
        kind: String,
        /// The rule definition as JSON (an `AbacRule`).
        #[arg(long, value_name = "JSON")]
        definition: String,
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
    /// List all policies.
    List {
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
    /// Enable or disable a policy (bumps its version).
    SetEnabled {
        /// Policy id.
        #[arg(value_name = "POLICY_ID")]
        id: String,
        /// Whether the policy is in force.
        #[arg(long, action = clap::ArgAction::Set)]
        enabled: bool,
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
    /// Delete a policy by id.
    Rm {
        /// Policy id.
        #[arg(value_name = "POLICY_ID")]
        id: String,
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
    /// Bind a policy to a tag, table, or namespace.
    Bind {
        /// Policy id.
        #[arg(value_name = "POLICY_ID")]
        id: String,
        /// Target kind: tag | table | namespace.
        #[arg(long = "type", value_name = "KIND")]
        target_type: String,
        /// Tag id (for a tag binding).
        #[arg(long)]
        tag: Option<String>,
        /// Warehouse (for table/namespace bindings).
        #[arg(long)]
        warehouse: Option<String>,
        /// Dotted namespace (for table/namespace bindings).
        #[arg(long)]
        namespace: Option<String>,
        /// Table name (for a table binding).
        #[arg(long)]
        table: Option<String>,
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
    /// Dry-run a proposed policy against principals on one table.
    DryRun {
        /// Kind: `row_filter` | `column_mask` | `abac`.
        #[arg(long)]
        kind: String,
        /// The proposed rule definition as JSON.
        #[arg(long, value_name = "JSON")]
        definition: String,
        /// Comma-separated principal audit strings (e.g. user:alice).
        #[arg(long, value_name = "PRINCIPALS")]
        principals: String,
        #[arg(long)]
        warehouse: String,
        #[arg(long)]
        namespace: String,
        #[arg(long)]
        table: String,
        /// Declared purpose to evaluate with.
        #[arg(long)]
        purpose: Option<String>,
        /// A tag to assume the table carries (preview a not-yet-bound tag).
        #[arg(long)]
        assume_tag: Option<String>,
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum GovernCommand {
    /// The effective policy for a (principal, table): masks, filter, decision.
    Effective {
        /// Principal audit string, e.g. user:alice@example.com.
        #[arg(long)]
        principal: String,
        #[arg(long)]
        warehouse: String,
        #[arg(long)]
        namespace: String,
        #[arg(long)]
        table: String,
        /// Declared purpose.
        #[arg(long)]
        purpose: Option<String>,
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
    /// A principal's effective permissions (who-can-see-what).
    WhoCanSee {
        /// Principal audit string.
        #[arg(long)]
        principal: String,
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
    /// Classification coverage for a warehouse (optionally a namespace).
    Coverage {
        #[arg(long)]
        warehouse: String,
        #[arg(long)]
        namespace: Option<String>,
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
    /// Policy-drift alerts for a warehouse (classified-but-unmasked columns).
    Drift {
        #[arg(long)]
        warehouse: String,
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
    /// Audit-ready evidence pack (policy/tag inventory + decision trail).
    Evidence {
        /// Max audit rows to include.
        #[arg(long)]
        limit: Option<i64>,
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
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
        Command::Maintenance(command) => run_async(run_maintenance(command)),
        Command::Mirror(command) => run_async(run_mirror(command)),
        Command::Tag(command) => run_async(run_tag(command)),
        Command::Policy(command) => run_async(run_policy(command)),
        Command::Govern(command) => run_async(run_govern(command)),
        Command::Sprawl {
            stale_threshold_s,
            server,
            token,
        } => run_async(run_sprawl(stale_threshold_s, server, token)),
        Command::Plan {
            file,
            server,
            token,
        } => run_async(run_plan(file, server, token)),
        Command::Apply {
            file,
            server,
            token,
        } => run_async(run_apply(file, server, token)),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

/// `meridian plan -f bundle.yaml` — diff a bundle against the server.
async fn run_plan(file: PathBuf, server: String, token: Option<String>) -> Result<(), CliError> {
    let bundle = bundle::load_file(&file).map_err(|e| CliError(e.to_string()))?;
    let state = bundle::plan::load_server_state(&server, token.as_deref()).await?;
    let plan = bundle::plan::compute(&bundle, &server, token.as_deref(), &state)
        .await
        .map_err(|e| CliError(e.to_string()))?;
    print!("{}", bundle::plan::render(&plan));
    Ok(())
}

/// `meridian apply -f bundle.yaml` — reconcile the server toward a bundle.
async fn run_apply(file: PathBuf, server: String, token: Option<String>) -> Result<(), CliError> {
    let bundle = bundle::load_file(&file).map_err(|e| CliError(e.to_string()))?;

    // Compute a plan first so we can surface the same would-delete /
    // would-update warnings apply refuses to act on.
    let state = bundle::plan::load_server_state(&server, token.as_deref()).await?;
    let plan = bundle::plan::compute(&bundle, &server, token.as_deref(), &state)
        .await
        .map_err(|e| CliError(e.to_string()))?;

    let mut report = bundle::apply::apply(&bundle, &server, token.as_deref())
        .await
        .map_err(|e| CliError(e.to_string()))?;
    report
        .outcomes
        .extend(bundle::apply::warnings_from_plan(&plan));

    print!("{}", report.render());

    if report.failures() > 0 {
        return Err(CliError(format!(
            "{} resource(s) failed to apply",
            report.failures()
        )));
    }
    Ok(())
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
            let created =
                client::warehouse_create(&server, None, &name, &storage_root, &options).await?;
            let id = created.get("id").and_then(Value::as_str).unwrap_or("?");
            println!("created warehouse {name} (id {id})");
            Ok(())
        }
        WarehouseCommand::List { server } => {
            let body = client::warehouse_list(&server, None).await?;
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
            client::namespace_create(&server, None, &warehouse, &levels, &props).await?;
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
                client::namespace_list(&server, None, &warehouse, parent_levels.as_deref()).await?;
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

/// Splits a `namespace.table` argument into `(levels, table_name)`.
fn split_table_arg(raw: &str) -> Result<(Vec<String>, String), CliError> {
    let mut levels = parse_namespace_arg(raw)?;
    if levels.len() < 2 {
        return Err(CliError(format!(
            "invalid table {raw:?}: expected namespace.table (dot-separated)"
        )));
    }
    let name = levels.pop().unwrap_or_default();
    Ok((levels, name))
}

/// `meridian maintenance ...` — health, triggers, jobs, policies, savings.
async fn run_maintenance(command: MaintenanceCommand) -> Result<(), CliError> {
    match command {
        MaintenanceCommand::Health { target } => maintenance_health(target).await,
        MaintenanceCommand::Compact { target, dry_run } => {
            maintenance_trigger(target, "compaction", dry_run).await
        }
        MaintenanceCommand::Expire { target } => {
            maintenance_trigger(target, "expire_snapshots", false).await
        }
        MaintenanceCommand::Jobs {
            state,
            limit,
            server,
            token,
        } => maintenance_jobs(&server, token.as_deref(), state.as_deref(), limit).await,
        MaintenanceCommand::Cancel { id, server, token } => {
            let body = client::maintenance_cancel(&server, token.as_deref(), &id).await?;
            let state = body.get("state").and_then(Value::as_str).unwrap_or("?");
            println!("job {id} is now {state}");
            Ok(())
        }
        MaintenanceCommand::Policies { server, token } => {
            maintenance_policies(&server, token.as_deref()).await
        }
        MaintenanceCommand::Savings {
            months,
            server,
            token,
        } => maintenance_savings(&server, token.as_deref(), months).await,
    }
}

/// `meridian maintenance health` — the score, metric table, and recommendations.
async fn maintenance_health(target: TableTarget) -> Result<(), CliError> {
    let (levels, name) = split_table_arg(&target.table)?;
    let body = client::maintenance_health(
        &target.server,
        target.token.as_deref(),
        &target.warehouse,
        &levels,
        &name,
    )
    .await?;
    let score = body.get("score").and_then(Value::as_i64).unwrap_or(-1);
    let metrics = body.get("metrics").cloned().unwrap_or(Value::Null);
    let num = |key: &str| {
        metrics
            .get(key)
            .map_or_else(|| "-".to_owned(), render_scalar)
    };
    let rows = vec![
        vec!["score".to_owned(), format!("{score} / 100")],
        vec!["data_files".to_owned(), num("data_file_count")],
        vec!["total_bytes".to_owned(), num("total_bytes")],
        vec!["small_file_ratio".to_owned(), num("small_file_ratio")],
        vec!["avg_file_bytes".to_owned(), num("avg_file_bytes")],
        vec!["snapshot_count".to_owned(), num("snapshot_count")],
        vec!["delete_debt_ratio".to_owned(), num("delete_debt_ratio")],
        vec!["manifest_count".to_owned(), num("manifest_count")],
    ];
    print!("{}", client::render_table(&["METRIC", "VALUE"], &rows));
    let recs = body
        .get("recommendations")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if recs.is_empty() {
        println!("\nno recommendations — table is healthy");
    } else {
        println!("\nrecommendations:");
        for rec in recs {
            let action = rec.get("action").and_then(Value::as_str).unwrap_or("?");
            let reason = rec.get("reason").and_then(Value::as_str).unwrap_or("");
            println!("  - {action}: {reason}");
        }
    }
    Ok(())
}

/// `meridian maintenance compact|expire` — enqueue a job on a table.
async fn maintenance_trigger(
    target: TableTarget,
    job_type: &str,
    dry_run: bool,
) -> Result<(), CliError> {
    let (levels, name) = split_table_arg(&target.table)?;
    let body = client::maintenance_trigger(
        &target.server,
        target.token.as_deref(),
        &target.warehouse,
        &levels,
        &name,
        job_type,
        dry_run,
    )
    .await?;
    let id = body.get("id").and_then(Value::as_str).unwrap_or("?");
    let state = body.get("state").and_then(Value::as_str).unwrap_or("?");
    println!("enqueued {job_type} job {id} ({state})");
    Ok(())
}

/// `meridian maintenance jobs` — the job queue as a table.
async fn maintenance_jobs(
    server: &str,
    token: Option<&str>,
    state: Option<&str>,
    limit: i64,
) -> Result<(), CliError> {
    let body = client::maintenance_jobs(server, token, state, limit).await?;
    let jobs = body
        .get("jobs")
        .and_then(Value::as_array)
        .ok_or_else(|| CliError("malformed response: missing jobs".to_owned()))?;
    let rows: Vec<Vec<String>> = jobs
        .iter()
        .map(|j| {
            vec![
                j.get("id")
                    .and_then(Value::as_str)
                    .unwrap_or("?")
                    .to_owned(),
                j.get("job_type")
                    .and_then(Value::as_str)
                    .unwrap_or("?")
                    .to_owned(),
                j.get("state")
                    .and_then(Value::as_str)
                    .unwrap_or("?")
                    .to_owned(),
                j.get("attempts")
                    .and_then(Value::as_i64)
                    .unwrap_or(0)
                    .to_string(),
                j.get("created_by")
                    .and_then(Value::as_str)
                    .unwrap_or("?")
                    .to_owned(),
            ]
        })
        .collect();
    print!(
        "{}",
        client::render_table(&["ID", "TYPE", "STATE", "ATTEMPTS", "BY"], &rows)
    );
    Ok(())
}

/// `meridian maintenance policies` — the policy list as a table.
async fn maintenance_policies(server: &str, token: Option<&str>) -> Result<(), CliError> {
    let body = client::maintenance_policies(server, token).await?;
    let policies = body
        .get("policies")
        .and_then(Value::as_array)
        .ok_or_else(|| CliError("malformed response: missing policies".to_owned()))?;
    let rows: Vec<Vec<String>> = policies
        .iter()
        .map(|p| {
            vec![
                p.get("scope_label")
                    .and_then(Value::as_str)
                    .unwrap_or("?")
                    .to_owned(),
                p.get("target_file_size_bytes")
                    .map_or_else(|| "-".to_owned(), render_scalar),
                p.get("snapshot_retention_count")
                    .map_or_else(|| "-".to_owned(), render_scalar),
                p.get("enabled")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                    .to_string(),
            ]
        })
        .collect();
    print!(
        "{}",
        client::render_table(
            &["SCOPE", "TARGET_BYTES", "KEEP_SNAPSHOTS", "ENABLED"],
            &rows
        )
    );
    Ok(())
}

/// `meridian maintenance savings` — the monthly roll-up as a table.
async fn maintenance_savings(
    server: &str,
    token: Option<&str>,
    months: i64,
) -> Result<(), CliError> {
    let body = client::maintenance_savings_rollup(server, token, months).await?;
    let rollup = body
        .get("rollup")
        .and_then(Value::as_array)
        .ok_or_else(|| CliError("malformed response: missing rollup".to_owned()))?;
    if rollup.is_empty() {
        println!("no savings recorded yet");
        return Ok(());
    }
    let rows: Vec<Vec<String>> = rollup
        .iter()
        .map(|r| {
            vec![
                r.get("period")
                    .map_or_else(|| "?".to_owned(), render_scalar),
                r.get("job_count")
                    .and_then(Value::as_i64)
                    .unwrap_or(0)
                    .to_string(),
                r.get("bytes_saved")
                    .and_then(Value::as_i64)
                    .unwrap_or(0)
                    .to_string(),
                r.get("files_removed")
                    .and_then(Value::as_i64)
                    .unwrap_or(0)
                    .to_string(),
            ]
        })
        .collect();
    print!(
        "{}",
        client::render_table(&["PERIOD", "JOBS", "BYTES_SAVED", "FILES_REMOVED"], &rows)
    );
    Ok(())
}

// ---- federation (Pillar B): mirror + sprawl -------------------------------

async fn run_mirror(command: MirrorCommand) -> Result<(), CliError> {
    match command {
        MirrorCommand::Create {
            name,
            kind,
            endpoint,
            remote_catalog,
            config,
            disabled,
            sync_interval_s,
            server,
            token,
        } => {
            let cfg = parse_pairs(&config)?;
            let created = client::mirror_create(
                &server,
                token.as_deref(),
                &name,
                &kind,
                &endpoint,
                remote_catalog.as_deref(),
                &cfg,
                !disabled,
                sync_interval_s,
            )
            .await?;
            let id = created.get("id").and_then(Value::as_str).unwrap_or("?");
            println!("created mirror {name} (id {id}, kind {kind})");
            Ok(())
        }
        MirrorCommand::List { server, token } => {
            let body = client::mirror_list(&server, token.as_deref()).await?;
            let mirrors = body
                .get("mirrors")
                .and_then(Value::as_array)
                .ok_or_else(|| CliError("malformed response: missing mirrors".to_owned()))?;
            if mirrors.is_empty() {
                println!("no mirrors registered");
                return Ok(());
            }
            let rows: Vec<Vec<String>> = mirrors
                .iter()
                .map(|m| {
                    vec![
                        field_str(m, "name"),
                        field_str(m, "kind"),
                        field_str(m, "endpoint"),
                        m.get("enabled")
                            .and_then(Value::as_bool)
                            .unwrap_or(false)
                            .to_string(),
                        m.get("asset_count")
                            .and_then(Value::as_i64)
                            .unwrap_or(0)
                            .to_string(),
                        m.get("last_sync_status")
                            .and_then(Value::as_str)
                            .unwrap_or("never")
                            .to_owned(),
                        m.get("last_synced_at")
                            .and_then(Value::as_str)
                            .unwrap_or("-")
                            .to_owned(),
                    ]
                })
                .collect();
            print!(
                "{}",
                client::render_table(
                    &[
                        "NAME",
                        "KIND",
                        "ENDPOINT",
                        "ENABLED",
                        "ASSETS",
                        "STATUS",
                        "LAST_SYNCED"
                    ],
                    &rows
                )
            );
            Ok(())
        }
        MirrorCommand::Sync {
            name,
            server,
            token,
        } => {
            let run = client::mirror_sync(&server, token.as_deref(), &name).await?;
            let status = run.get("status").and_then(Value::as_str).unwrap_or("?");
            println!("sync requested for mirror {name} (status {status})");
            Ok(())
        }
    }
}

/// `meridian sprawl` — the cross-catalog sprawl summary as a set of tables.
async fn run_sprawl(
    stale_threshold_s: Option<i64>,
    server: String,
    token: Option<String>,
) -> Result<(), CliError> {
    let body = client::sprawl(&server, token.as_deref(), stale_threshold_s).await?;

    let n = |k: &str| body.get(k).and_then(Value::as_i64).unwrap_or(0);
    println!(
        "sources: {}  (warehouses {}, mirrors {})   total assets: {}",
        n("source_count"),
        n("warehouse_count"),
        n("mirror_count"),
        n("total_assets"),
    );
    println!(
        "ownership gaps: {}   owned mirror assets: {}",
        n("ownership_gaps"),
        n("owned_mirror_assets"),
    );

    print_sprawl_sources(&body);
    print_sprawl_duplicates(&body);
    print_sprawl_stale(&body);
    print_sprawl_health(&body);
    Ok(())
}

fn print_sprawl_sources(body: &Value) {
    let Some(sources) = body.get("sources").and_then(Value::as_array) else {
        return;
    };
    if sources.is_empty() {
        return;
    }
    println!("\nper-source asset counts:");
    let rows: Vec<Vec<String>> = sources
        .iter()
        .map(|s| {
            vec![
                field_str(s, "source_type"),
                field_str(s, "name"),
                field_str(s, "kind"),
                field_i64(s, "asset_count").to_string(),
            ]
        })
        .collect();
    print!(
        "{}",
        client::render_table(&["TYPE", "NAME", "KIND", "ASSETS"], &rows)
    );
}

fn print_sprawl_duplicates(body: &Value) {
    let Some(dups) = body.get("duplicates").and_then(Value::as_array) else {
        return;
    };
    if dups.is_empty() {
        return;
    }
    println!("\nduplicate storage locations (registered in >1 source):");
    let rows: Vec<Vec<String>> = dups
        .iter()
        .map(|d| {
            let sources = d
                .get("sources")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            vec![
                field_str(d, "storage_location"),
                field_i64(d, "source_count").to_string(),
                sources,
            ]
        })
        .collect();
    print!(
        "{}",
        client::render_table(&["LOCATION", "SOURCES", "REGISTERED_IN"], &rows)
    );
}

fn print_sprawl_stale(body: &Value) {
    let Some(stale) = body.get("stale_mirrors").and_then(Value::as_array) else {
        return;
    };
    if stale.is_empty() {
        return;
    }
    println!("\nstale mirrors:");
    let rows: Vec<Vec<String>> = stale
        .iter()
        .map(|m| {
            vec![
                field_str(m, "name"),
                m.get("last_synced_at")
                    .and_then(Value::as_str)
                    .unwrap_or("never")
                    .to_owned(),
                m.get("age_seconds")
                    .and_then(Value::as_i64)
                    .map_or_else(|| "-".to_owned(), |s| format!("{s}s")),
            ]
        })
        .collect();
    print!(
        "{}",
        client::render_table(&["NAME", "LAST_SYNCED", "AGE"], &rows)
    );
}

fn print_sprawl_health(body: &Value) {
    let Some(health) = body.get("health").and_then(Value::as_object) else {
        return;
    };
    let hn = |k: &str| health.get(k).and_then(Value::as_i64).unwrap_or(0);
    let avg = health
        .get("avg_score")
        .and_then(Value::as_f64)
        .unwrap_or(0.0);
    println!(
        "\nnative health rollup: {} tables scored, avg {:.0} \
         (healthy {}, degraded {}, unhealthy {})",
        hn("tables_scored"),
        avg,
        hn("healthy_count"),
        hn("degraded_count"),
        hn("unhealthy_count"),
    );
}

/// Reads a string field, defaulting to `-`.
fn field_str(v: &Value, key: &str) -> String {
    v.get(key).and_then(Value::as_str).unwrap_or("-").to_owned()
}

/// Reads an integer field, defaulting to 0.
fn field_i64(v: &Value, key: &str) -> i64 {
    v.get(key).and_then(Value::as_i64).unwrap_or(0)
}

/// Parses a `--definition` JSON argument into a value, with a clear error.
fn parse_definition(raw: &str) -> Result<Value, CliError> {
    serde_json::from_str(raw).map_err(|e| CliError(format!("--definition is not valid JSON: {e}")))
}

async fn run_tag(command: TagCommand) -> Result<(), CliError> {
    match command {
        TagCommand::Create {
            key,
            value,
            description,
            server,
            token,
        } => {
            let body =
                serde_json::json!({ "key": key, "value": value, "description": description });
            let created =
                client::gov_post(&server, token.as_deref(), "/api/v2/governance/tags", &body)
                    .await?;
            println!(
                "created tag {} ({})",
                created.get("id").and_then(Value::as_str).unwrap_or("?"),
                created
                    .get("rendered")
                    .and_then(Value::as_str)
                    .unwrap_or("?"),
            );
            Ok(())
        }
        TagCommand::List { server, token } => {
            let body =
                client::gov_get(&server, token.as_deref(), "/api/v2/governance/tags", &[]).await?;
            let tags = body
                .get("tags")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let rows: Vec<Vec<String>> = tags
                .iter()
                .map(|t| {
                    vec![
                        field_str(t, "id"),
                        field_str(t, "rendered"),
                        field_str(t, "description"),
                    ]
                })
                .collect();
            print!(
                "{}",
                client::render_table(&["ID", "TAG", "DESCRIPTION"], &rows)
            );
            Ok(())
        }
        TagCommand::Rm { id, server, token } => {
            client::gov_delete(
                &server,
                token.as_deref(),
                &format!("/api/v2/governance/tags/{id}"),
            )
            .await?;
            println!("deleted tag {id}");
            Ok(())
        }
        TagCommand::Assign {
            tag,
            securable_type,
            warehouse,
            namespace,
            table,
            column,
            server,
            token,
        } => {
            let body = serde_json::json!({
                "tag_id": tag,
                "target": {
                    "securable_type": securable_type,
                    "warehouse": warehouse,
                    "namespace": namespace,
                    "table": table,
                    "column": column,
                }
            });
            let created = client::gov_post(
                &server,
                token.as_deref(),
                "/api/v2/governance/tags/assignments",
                &body,
            )
            .await?;
            println!(
                "assigned tag (assignment {})",
                created.get("id").and_then(Value::as_str).unwrap_or("?")
            );
            Ok(())
        }
    }
}

#[allow(clippy::too_many_lines)] // one match arm per policy subcommand
async fn run_policy(command: PolicyCommand) -> Result<(), CliError> {
    match command {
        PolicyCommand::Create {
            name,
            kind,
            definition,
            server,
            token,
        } => {
            let body = serde_json::json!({
                "name": name, "kind": kind, "definition": parse_definition(&definition)?
            });
            let created = client::gov_post(
                &server,
                token.as_deref(),
                "/api/v2/governance/policies",
                &body,
            )
            .await?;
            println!(
                "created policy {} (v{})",
                created.get("id").and_then(Value::as_str).unwrap_or("?"),
                created.get("version").and_then(Value::as_i64).unwrap_or(0),
            );
            Ok(())
        }
        PolicyCommand::List { server, token } => {
            let body = client::gov_get(
                &server,
                token.as_deref(),
                "/api/v2/governance/policies",
                &[],
            )
            .await?;
            let policies = body
                .get("policies")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let rows: Vec<Vec<String>> = policies
                .iter()
                .map(|p| {
                    vec![
                        field_str(p, "id"),
                        field_str(p, "name"),
                        field_str(p, "kind"),
                        field_i64(p, "version").to_string(),
                        p.get("enabled")
                            .and_then(Value::as_bool)
                            .unwrap_or(false)
                            .to_string(),
                    ]
                })
                .collect();
            print!(
                "{}",
                client::render_table(&["ID", "NAME", "KIND", "VER", "ENABLED"], &rows)
            );
            Ok(())
        }
        PolicyCommand::SetEnabled {
            id,
            enabled,
            server,
            token,
        } => {
            let body = serde_json::json!({ "enabled": enabled });
            let updated = client::gov_patch(
                &server,
                token.as_deref(),
                &format!("/api/v2/governance/policies/{id}"),
                &body,
            )
            .await?;
            println!(
                "policy {id} enabled={} (v{})",
                enabled,
                updated.get("version").and_then(Value::as_i64).unwrap_or(0),
            );
            Ok(())
        }
        PolicyCommand::Rm { id, server, token } => {
            client::gov_delete(
                &server,
                token.as_deref(),
                &format!("/api/v2/governance/policies/{id}"),
            )
            .await?;
            println!("deleted policy {id}");
            Ok(())
        }
        PolicyCommand::Bind {
            id,
            target_type,
            tag,
            warehouse,
            namespace,
            table,
            server,
            token,
        } => {
            let body = serde_json::json!({
                "target_type": target_type,
                "tag_id": tag,
                "warehouse": warehouse,
                "namespace": namespace,
                "table": table,
            });
            let created = client::gov_post(
                &server,
                token.as_deref(),
                &format!("/api/v2/governance/policies/{id}/bindings"),
                &body,
            )
            .await?;
            println!(
                "bound policy {id} (binding {})",
                created.get("id").and_then(Value::as_str).unwrap_or("?")
            );
            Ok(())
        }
        PolicyCommand::DryRun {
            kind,
            definition,
            principals,
            warehouse,
            namespace,
            table,
            purpose,
            assume_tag,
            server,
            token,
        } => {
            let principal_list: Vec<String> = principals
                .split(',')
                .map(|s| s.trim().to_owned())
                .filter(|s| !s.is_empty())
                .collect();
            let body = serde_json::json!({
                "kind": kind,
                "definition": parse_definition(&definition)?,
                "principals": principal_list,
                "warehouse": warehouse,
                "namespace": namespace,
                "table": table,
                "purpose": purpose,
                "assume_table_tag": assume_tag,
            });
            let result = client::gov_post(
                &server,
                token.as_deref(),
                "/api/v2/governance/policies/dry-run",
                &body,
            )
            .await?;
            let results = result
                .get("results")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let rows: Vec<Vec<String>> = results
                .iter()
                .map(|r| {
                    let masked = r
                        .get("masked_columns")
                        .and_then(Value::as_array)
                        .map(|a| {
                            a.iter()
                                .filter_map(Value::as_str)
                                .collect::<Vec<_>>()
                                .join(",")
                        })
                        .unwrap_or_default();
                    vec![
                        field_str(r, "principal"),
                        r.get("denied")
                            .and_then(Value::as_bool)
                            .unwrap_or(false)
                            .to_string(),
                        r.get("row_filtered")
                            .and_then(Value::as_bool)
                            .unwrap_or(false)
                            .to_string(),
                        masked,
                    ]
                })
                .collect();
            print!(
                "{}",
                client::render_table(&["PRINCIPAL", "DENIED", "ROW_FILTERED", "MASKED"], &rows)
            );
            Ok(())
        }
    }
}

#[allow(clippy::too_many_lines)] // one match arm per govern subcommand
async fn run_govern(command: GovernCommand) -> Result<(), CliError> {
    match command {
        GovernCommand::Effective {
            principal,
            warehouse,
            namespace,
            table,
            purpose,
            server,
            token,
        } => {
            let mut query = vec![
                ("principal", principal),
                ("warehouse", warehouse),
                ("namespace", namespace),
                ("table", table),
            ];
            if let Some(p) = purpose {
                query.push(("purpose", p));
            }
            let body = client::gov_get(
                &server,
                token.as_deref(),
                "/api/v2/governance/effective-policy",
                &query,
            )
            .await?;
            let masked = body
                .get("masked_columns")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .unwrap_or_default();
            println!(
                "denied={}  masked=[{}]  row_filter={}\nreason: {}",
                body.get("denied").and_then(Value::as_bool).unwrap_or(false),
                masked,
                body.get("row_filter")
                    .map_or("none".to_owned(), std::string::ToString::to_string),
                field_str(&body, "reason"),
            );
            Ok(())
        }
        GovernCommand::WhoCanSee {
            principal,
            server,
            token,
        } => {
            let body = client::gov_get(
                &server,
                token.as_deref(),
                "/api/v2/governance/who-can-see",
                &[("principal", principal)],
            )
            .await?;
            let perms = body
                .get("permissions")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let rows: Vec<Vec<String>> = perms
                .iter()
                .map(|p| {
                    vec![
                        field_str(p, "privilege"),
                        field_str(p, "securable_type"),
                        field_str(p, "securable_id"),
                        field_str(p, "via_role"),
                    ]
                })
                .collect();
            print!(
                "{}",
                client::render_table(
                    &["PRIVILEGE", "SECURABLE", "SECURABLE ID", "VIA ROLE"],
                    &rows
                )
            );
            Ok(())
        }
        GovernCommand::Coverage {
            warehouse,
            namespace,
            server,
            token,
        } => {
            let mut query = vec![("warehouse", warehouse)];
            if let Some(ns) = namespace {
                query.push(("namespace", ns));
            }
            let body = client::gov_get(
                &server,
                token.as_deref(),
                "/api/v2/governance/tags/coverage",
                &query,
            )
            .await?;
            println!(
                "tables: {}   with any tag: {}",
                field_i64(&body, "total_tables"),
                field_i64(&body, "tables_with_any_tag"),
            );
            Ok(())
        }
        GovernCommand::Drift {
            warehouse,
            server,
            token,
        } => {
            let body = client::gov_get(
                &server,
                token.as_deref(),
                "/api/v2/governance/drift",
                &[("warehouse", warehouse)],
            )
            .await?;
            let alerts = body
                .get("alerts")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            println!("drift alerts: {}", alerts.len());
            let rows: Vec<Vec<String>> = alerts
                .iter()
                .map(|a| {
                    vec![
                        field_str(a, "table_id"),
                        field_str(a, "column"),
                        field_str(a, "tag"),
                    ]
                })
                .collect();
            print!(
                "{}",
                client::render_table(&["TABLE ID", "COLUMN", "TAG"], &rows)
            );
            Ok(())
        }
        GovernCommand::Evidence {
            limit,
            server,
            token,
        } => {
            let query: Vec<(&str, String)> = limit
                .map(|l| vec![("limit", l.to_string())])
                .unwrap_or_default();
            let body = client::gov_get(
                &server,
                token.as_deref(),
                "/api/v2/governance/evidence",
                &query,
            )
            .await?;
            println!(
                "evidence pack: {} policies, {} tags, {} decision(s) in the trail",
                field_i64(&body, "policy_count"),
                field_i64(&body, "tag_count"),
                body.get("audit_trail")
                    .and_then(Value::as_array)
                    .map_or(0, Vec::len),
            );
            Ok(())
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
