//! The `meridian` binary.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use meridian_common::{AppConfig, MeridianError};
use serde_json::{Value, json};

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

    /// Manage catalog branches & tags — Data CI/CD (Pillar K).
    ///
    /// A branch is a zero-copy divergent view of the catalog, mountable by any
    /// engine as `warehouse@branch`. Commit to it from any engine, diff it,
    /// gate it, and merge to main. `branch tag` manages immutable release
    /// points. (Distinct from `tag`, which manages Pillar D classification
    /// tags.)
    #[command(subcommand)]
    Branch(BranchCommand),

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

    /// Manage zero-scan data-quality monitors (Pillar E).
    #[command(subcommand)]
    Monitor(MonitorCommand),

    /// Manage data-quality incidents (Pillar E).
    #[command(subcommand)]
    Incident(IncidentCommand),

    /// Inspect data-quality: per-table status and trust score (Pillar E).
    #[command(subcommand)]
    Quality(QualityCommand),

    /// Analyze the downstream blast radius of a change; a CI gate (Pillar F).
    ///
    /// Prints every downstream asset a change would affect and the owners to
    /// notify. With `--fail-on-downstream`, exits non-zero when the change
    /// breaks any downstream asset — drop it into a dbt/SQL CI job to block a
    /// breaking change before it merges.
    Impact {
        /// The change: `drop_table` or `drop_column:<name>`.
        #[arg(long, value_name = "CHANGE")]
        change: String,

        /// The asset the change is to (`warehouse.namespace.table`).
        #[arg(long, value_name = "ASSET")]
        asset: String,

        /// Traversal depth (1-20); default server-side (3).
        #[arg(long)]
        depth: Option<u32>,

        /// Exit non-zero when the change breaks any downstream asset.
        #[arg(long)]
        fail_on_downstream: bool,

        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,

        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// Manage metrics — first-class semantic objects (Pillar G, G-F2).
    #[command(subcommand)]
    Metric(MetricCommand),

    /// Manage the business glossary — terms and asset links (Pillar G, G-F3).
    #[command(subcommand)]
    Glossary(GlossaryCommand),

    /// Manage certified data products — named bundles (Pillar G, G-F4).
    #[command(subcommand)]
    Product(ProductCommand),

    /// Manage cross-org data shares (Pillar J, J-F1).
    #[command(subcommand)]
    Share(ShareCommand),

    /// Browse the internal data marketplace and request access (Pillar J, J-F2).
    #[command(subcommand)]
    Marketplace(MarketplaceCommand),

    /// Translate a SQL statement between engine dialects (Pillar G, G-F1).
    ///
    /// Deterministic `SQLGlot` via the sidecar; prints the translation with its
    /// honest status (`verified` | `best_effort` | `unsupported`) and diagnostics.
    Transpile {
        /// The SQL statement to translate.
        #[arg(long)]
        sql: String,
        /// Source dialect (e.g. spark, trino, snowflake).
        #[arg(long = "from")]
        from_dialect: String,
        /// Target dialect (e.g. trino, duckdb).
        #[arg(long = "to")]
        to_dialect: String,
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

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

    /// Manage generic AI assets — filesets, models, vector datasets (Pillar I).
    #[command(subcommand)]
    Asset(AssetCommand),

    /// Pin and inspect immutable training runs (Pillar I, I-F2).
    #[command(subcommand)]
    TrainingRun(TrainingRunCommand),

    /// Per-model provenance + the EU AI Act GPAI summary (Pillar I, I-F3).
    #[command(subcommand)]
    Provenance(ProvenanceCommand),

    /// GDPR deletion campaigns — "right to be forgotten" evidence (Pillar I, I-F4).
    #[command(subcommand)]
    Deletion(DeletionCommand),
}

/// Generic AI assets (Pillar I, I-F1).
#[derive(Debug, Subcommand)]
enum AssetCommand {
    /// Register an asset. `--kind fileset` also needs `--warehouse` +
    /// `--storage-prefix`; models/vector datasets take `--metadata` JSON.
    Create {
        /// Asset kind: `fileset`, `model`, or `vector_dataset`.
        #[arg(long)]
        kind: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        description: Option<String>,
        #[arg(long)]
        owner: Option<String>,
        /// Warehouse name (filesets only).
        #[arg(long)]
        warehouse: Option<String>,
        /// The fileset storage prefix, `s3://bucket/prefix` (filesets only).
        #[arg(long)]
        storage_prefix: Option<String>,
        /// Kind-specific metadata as a JSON object.
        #[arg(long)]
        metadata: Option<String>,
        /// Repeatable key:value tag (e.g. --tag license:cc-by).
        #[arg(long = "tag", value_name = "KEY:VALUE")]
        tags: Vec<String>,
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
    /// List assets, optionally of one kind.
    List {
        #[arg(long = "type")]
        kind: Option<String>,
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
    /// Show one asset by id.
    Get {
        id: String,
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
    /// Vend scoped, short-lived credentials for a fileset (bound to its prefix).
    Credentials {
        /// The fileset asset id.
        id: String,
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
}

/// Training-run pinning (Pillar I, I-F2).
#[derive(Debug, Subcommand)]
enum TrainingRunCommand {
    /// Pin a model version to exact table snapshots. Inputs repeat as
    /// `--input table_ref=<ref>,snapshot_id=<id>[,table_id=<id>]`.
    Pin {
        #[arg(long)]
        model: String,
        #[arg(long)]
        model_version: String,
        /// Optional registered model asset id to link.
        #[arg(long)]
        model_asset_id: Option<String>,
        /// Repeatable input pin: `table_ref=<ref>,snapshot_id=<n>[,table_id=<id>]`.
        #[arg(long = "input", value_name = "SPEC", required = true)]
        inputs: Vec<String>,
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
    /// Show an immutable training run by id.
    Get {
        id: String,
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
}

/// Per-model provenance + AI Act summary (Pillar I, I-F3).
#[derive(Debug, Subcommand)]
enum ProvenanceCommand {
    /// The per-model lineage + propagated tags + dataset cards.
    Show {
        model: String,
        #[arg(long)]
        version: Option<String>,
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
    /// The EU AI Act GPAI training-content summary for a model.
    AiActSummary {
        model: String,
        #[arg(long)]
        version: Option<String>,
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
}

/// GDPR deletion campaigns (Pillar I, I-F4).
#[derive(Debug, Subcommand)]
enum DeletionCommand {
    /// Open a deletion campaign for an erasure subject.
    Open {
        #[arg(long)]
        name: String,
        #[arg(long)]
        subject: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
    /// Add affected snapshots and freeze the model-exposure evidence. Snapshots
    /// repeat as `--snapshot table_ref=<ref>,snapshot_id=<n>[,table_id=<id>][,branch=<b>]`.
    AddSnapshots {
        /// The campaign id.
        id: String,
        #[arg(long = "snapshot", value_name = "SPEC", required = true)]
        snapshots: Vec<String>,
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
    /// Show the full GDPR evidence record for a campaign.
    Evidence {
        id: String,
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
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

        /// Bearer token (from --token or the MERIDIAN_TOKEN env var; required
        /// when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN", env = "MERIDIAN_TOKEN")]
        token: Option<String>,
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

        /// Bearer token (from --token or the MERIDIAN_TOKEN env var; required
        /// when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN", env = "MERIDIAN_TOKEN")]
        token: Option<String>,
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

        /// Bearer token (from --token or the MERIDIAN_TOKEN env var; required
        /// when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN", env = "MERIDIAN_TOKEN")]
        token: Option<String>,
    },

    /// List registered warehouses.
    List {
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,

        /// Bearer token (from --token or the MERIDIAN_TOKEN env var; required
        /// when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN", env = "MERIDIAN_TOKEN")]
        token: Option<String>,
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

/// Catalog branches & tags — Data CI/CD (Pillar K).
#[derive(Debug, Subcommand)]
enum BranchCommand {
    /// Create a branch (K-F1). Zero-copy: it shares main's metadata until a
    /// table diverges. Mount it from any engine as `warehouse@<name>`.
    Create {
        /// Branch name, unique per workspace.
        #[arg(value_name = "NAME")]
        name: String,
        /// The ref to diverge from (default `main`).
        #[arg(long, default_value = "main")]
        base: String,
        /// Warehouse whose namespaces to scope to (required with --namespace).
        #[arg(long)]
        warehouse: Option<String>,
        /// A namespace (dotted levels) the branch spans; repeatable. Omit for
        /// all namespaces.
        #[arg(long = "namespace", value_name = "NS")]
        namespaces: Vec<String>,
        /// Make it an ephemeral PR branch expiring in this many seconds (K-F3).
        #[arg(long, value_name = "SECONDS")]
        expires_in_s: Option<i64>,
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// List branches (and tags).
    List {
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// Show the schema + snapshot + row-count delta of a branch vs main (K-F1).
    Diff {
        /// Branch name.
        #[arg(value_name = "NAME")]
        name: String,
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// Check the merge gate: contracts on the branch head (K-F3). Exits
    /// non-zero when the gate fails — drop into a CI job before merging.
    Gate {
        /// Branch name.
        #[arg(value_name = "NAME")]
        name: String,
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// Merge a branch into main (K-F1): gate + conflict checked, fast-forward.
    Merge {
        /// Branch name.
        #[arg(value_name = "NAME")]
        name: String,
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// Delete a branch (K-F3 teardown).
    Delete {
        /// Branch name.
        #[arg(value_name = "NAME")]
        name: String,
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// Delete expired ephemeral branches (K-F3).
    Sweep {
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// Manage immutable catalog tags — release points like `q2-close` (K-F1).
    #[command(subcommand)]
    Tag(CatalogTagCommand),
}

/// Immutable catalog tags (Pillar K, K-F1) — distinct from Pillar D
/// classification tags (`meridian tag`).
#[derive(Debug, Subcommand)]
enum CatalogTagCommand {
    /// Create an immutable tag pinning a ref's current state.
    Create {
        /// Tag name, unique per workspace.
        #[arg(value_name = "NAME")]
        name: String,
        /// The ref to freeze (default `main`).
        #[arg(long = "from", default_value = "main")]
        from_ref: String,
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
    /// List tags.
    List {
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
    /// Delete a tag.
    Delete {
        /// Tag name.
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

        /// Bearer token (from --token or the MERIDIAN_TOKEN env var; required
        /// when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN", env = "MERIDIAN_TOKEN")]
        token: Option<String>,
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

        /// Bearer token (from --token or the MERIDIAN_TOKEN env var; required
        /// when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN", env = "MERIDIAN_TOKEN")]
        token: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum MonitorCommand {
    /// List all monitors.
    List {
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// Create a monitor on a table or namespace.
    Create {
        /// Human name, unique per workspace.
        #[arg(long)]
        name: String,
        /// The zero-scan signal to compute (freshness, volume, schema change,
        /// file size, snapshot debt, or commit failure). See the docs for the
        /// exact kind tokens.
        #[arg(long)]
        kind: String,
        /// Warehouse the bound securable lives in.
        #[arg(long)]
        warehouse: String,
        /// Bind to `table` or `namespace`.
        #[arg(long, default_value = "table")]
        bound_to: String,
        /// The dotted namespace (the namespace itself, or the table's).
        #[arg(long)]
        namespace: String,
        /// The table name (required for a table binding).
        #[arg(long)]
        table: Option<String>,
        /// Incident severity: low, medium, high.
        #[arg(long)]
        severity: Option<String>,
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// Enable or disable a monitor.
    Set {
        /// The monitor id.
        #[arg(value_name = "ID")]
        id: String,
        /// Enable the monitor.
        #[arg(long, conflicts_with = "disable")]
        enable: bool,
        /// Disable the monitor.
        #[arg(long)]
        disable: bool,
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// Delete a monitor.
    Rm {
        /// The monitor id.
        #[arg(value_name = "ID")]
        id: String,
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// Show recent monitor evaluation results.
    Results {
        /// Restrict to one monitor id.
        #[arg(long)]
        monitor: Option<String>,
        /// Max rows.
        #[arg(long, default_value_t = 50)]
        limit: i64,
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum IncidentCommand {
    /// List incidents (newest first).
    List {
        /// Only live (open + acknowledged) incidents.
        #[arg(long)]
        live: bool,
        /// Restrict to one status: open, acknowledged, resolved.
        #[arg(long)]
        status: Option<String>,
        /// Max rows.
        #[arg(long, default_value_t = 50)]
        limit: i64,
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// Acknowledge an open incident.
    Ack {
        /// The incident id.
        #[arg(value_name = "ID")]
        id: String,
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// Resolve an incident.
    Resolve {
        /// The incident id.
        #[arg(value_name = "ID")]
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
enum QualityCommand {
    /// Show a table's traffic-light status (worst live incident severity).
    Status {
        /// Warehouse the table lives in.
        #[arg(long)]
        warehouse: String,
        /// The dotted namespace.
        #[arg(long)]
        namespace: String,
        /// The table name.
        #[arg(long)]
        table: String,
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },

    /// Show a table's composite quality / trust score with its components.
    Score {
        /// Warehouse the table lives in.
        #[arg(long)]
        warehouse: String,
        /// The dotted namespace.
        #[arg(long)]
        namespace: String,
        /// The table name.
        #[arg(long)]
        table: String,
        /// Base URL of the Meridian server.
        #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
        server: String,
        /// Bearer token (required when the server runs auth.mode = "oidc").
        #[arg(long, value_name = "TOKEN")]
        token: Option<String>,
    },
}

/// Shared server/token args for the semantics subcommands (kept as a flatten
/// group so every leaf carries the same two flags without repetition).
#[derive(Debug, clap::Args)]
struct ServerArgs {
    /// Base URL of the Meridian server.
    #[arg(long, default_value = DEFAULT_SERVER, value_name = "URL")]
    server: String,
    /// Bearer token (required when the server runs auth.mode = "oidc").
    #[arg(long, value_name = "TOKEN")]
    token: Option<String>,
}

#[derive(Debug, Subcommand)]
enum MetricCommand {
    /// List metrics.
    List {
        #[command(flatten)]
        server: ServerArgs,
    },
    /// Show one metric by id.
    Get {
        /// The metric id.
        id: String,
        #[command(flatten)]
        server: ServerArgs,
    },
    /// Create a metric.
    Create {
        /// Machine name (unique per workspace).
        #[arg(long)]
        name: String,
        /// Source table/view identifier (dotted).
        #[arg(long)]
        source: String,
        /// Measure aggregation expression, e.g. "SUM(amount)".
        #[arg(long)]
        expression: String,
        /// Canonical dialect the fragments are authored in.
        #[arg(long, default_value = "trino")]
        dialect: String,
        /// A default group-by dimension (repeatable).
        #[arg(long = "dimension", value_name = "DIM")]
        dimensions: Vec<String>,
        /// A default filter fragment (repeatable).
        #[arg(long = "filter", value_name = "SQL")]
        filters: Vec<String>,
        /// Grain description.
        #[arg(long)]
        grain: Option<String>,
        /// Certification: draft | certified | deprecated.
        #[arg(long, default_value = "draft")]
        certification: String,
        #[command(flatten)]
        server: ServerArgs,
    },
    /// Delete a metric by id.
    Delete {
        /// The metric id.
        id: String,
        #[command(flatten)]
        server: ServerArgs,
    },
    /// Compile a metric to a chosen engine's SQL.
    Compile {
        /// The metric id.
        id: String,
        /// Target engine dialect (e.g. trino, duckdb).
        #[arg(long)]
        engine: String,
        #[command(flatten)]
        server: ServerArgs,
    },
}

#[derive(Debug, Subcommand)]
enum GlossaryCommand {
    /// List glossary terms.
    List {
        #[command(flatten)]
        server: ServerArgs,
    },
    /// Show one term (with its links) by id.
    Get {
        /// The term id.
        id: String,
        #[command(flatten)]
        server: ServerArgs,
    },
    /// Create a glossary term.
    Create {
        /// Term name (unique per workspace).
        #[arg(long)]
        name: String,
        /// The definition (markdown).
        #[arg(long)]
        definition: String,
        /// Certification: draft | certified | deprecated.
        #[arg(long, default_value = "draft")]
        certification: String,
        #[command(flatten)]
        server: ServerArgs,
    },
    /// Delete a term by id (and its links).
    Delete {
        /// The term id.
        id: String,
        #[command(flatten)]
        server: ServerArgs,
    },
    /// Link a term to an asset.
    Link {
        /// The term id.
        id: String,
        /// Asset kind: table | view | metric.
        #[arg(long)]
        kind: String,
        /// Stable asset reference, e.g. "table:<id>".
        #[arg(long = "ref", value_name = "REF")]
        asset_ref: String,
        #[command(flatten)]
        server: ServerArgs,
    },
}

#[derive(Debug, Subcommand)]
enum ProductCommand {
    /// List data products.
    List {
        #[command(flatten)]
        server: ServerArgs,
    },
    /// Show one product (with its members) by id.
    Get {
        /// The product id.
        id: String,
        #[command(flatten)]
        server: ServerArgs,
    },
    /// Create a data product.
    Create {
        /// Machine name (unique per workspace).
        #[arg(long)]
        name: String,
        /// Description (markdown).
        #[arg(long)]
        description: Option<String>,
        /// Free-text SLA statement.
        #[arg(long)]
        sla: Option<String>,
        /// Certification: draft | certified | deprecated.
        #[arg(long, default_value = "draft")]
        certification: String,
        #[command(flatten)]
        server: ServerArgs,
    },
    /// Delete a product by id (and its membership rows).
    Delete {
        /// The product id.
        id: String,
        #[command(flatten)]
        server: ServerArgs,
    },
    /// Add a member to a product.
    AddMember {
        /// The product id.
        id: String,
        /// Member kind: table | view | metric | `glossary_term` | contract.
        #[arg(long)]
        kind: String,
        /// Stable member reference.
        #[arg(long = "ref", value_name = "REF")]
        member_ref: String,
        #[command(flatten)]
        server: ServerArgs,
    },
    /// Show a product's status page (certification + members + health rollup).
    Status {
        /// The product id.
        id: String,
        #[command(flatten)]
        server: ServerArgs,
    },
}

/// `meridian share ...` — cross-org data sharing (Pillar J, J-F1).
#[derive(Debug, Subcommand)]
enum ShareCommand {
    /// List the workspace's shares (tokens are never shown).
    List {
        #[command(flatten)]
        server: ServerArgs,
    },
    /// Show one share with its grants.
    Get {
        /// The share id.
        id: String,
        #[command(flatten)]
        server: ServerArgs,
    },
    /// Create a share for an external recipient. The token is printed once —
    /// deliver it to the recipient over a secure channel.
    Create {
        /// Machine name (unique per workspace).
        #[arg(long)]
        name: String,
        /// External recipient identifier (e.g. org:acme).
        #[arg(long)]
        recipient: String,
        /// Optional terms of use the recipient must accept before data serves.
        #[arg(long)]
        terms: Option<String>,
        #[command(flatten)]
        server: ServerArgs,
    },
    /// Add a securable to a share, with optional row filter / column mask.
    Grant {
        /// The share id.
        id: String,
        /// Securable kind: table | view | `data_product`.
        #[arg(long)]
        kind: String,
        /// Stable securable reference (e.g. table:<id>).
        #[arg(long = "ref", value_name = "REF")]
        securable_ref: String,
        /// Optional advisory row filter (a boolean SQL predicate).
        #[arg(long)]
        row_filter: Option<String>,
        /// Optional column mask (repeatable): a column to hide from the recipient.
        #[arg(long = "mask", value_name = "COLUMN")]
        mask: Vec<String>,
        #[command(flatten)]
        server: ServerArgs,
    },
    /// Remove a grant from a share by grant id.
    Ungrant {
        /// The grant id.
        grant_id: String,
        #[command(flatten)]
        server: ServerArgs,
    },
    /// Revoke a share (instant: the recipient is denied and creds expire).
    Revoke {
        /// The share id.
        id: String,
        #[command(flatten)]
        server: ServerArgs,
    },
    /// Delete a share and its grants (prefer revoke to retain history).
    Delete {
        /// The share id.
        id: String,
        #[command(flatten)]
        server: ServerArgs,
    },
}

/// `meridian marketplace ...` — the internal data marketplace (Pillar J, J-F2).
#[derive(Debug, Subcommand)]
enum MarketplaceCommand {
    /// Browse the certified-data-product gallery (certified first).
    Products {
        #[command(flatten)]
        server: ServerArgs,
    },
    /// Request access to an asset (creates a pending access request).
    Request {
        /// Securable type: warehouse | namespace | table | view.
        #[arg(long)]
        kind: String,
        /// Stable securable reference.
        #[arg(long = "ref", value_name = "REF")]
        securable_ref: String,
        /// Requested privilege (default READ).
        #[arg(long, default_value = "READ")]
        privilege: String,
        /// Declared purpose (purpose-based access).
        #[arg(long)]
        purpose: String,
        #[command(flatten)]
        server: ServerArgs,
    },
    /// List access requests (optionally filtered by state).
    Requests {
        /// State filter: pending | approved | denied | expired.
        #[arg(long)]
        state: Option<String>,
        #[command(flatten)]
        server: ServerArgs,
    },
    /// Approve or deny a pending access request.
    Decide {
        /// The request id.
        id: String,
        /// Approve (default is deny).
        #[arg(long)]
        approve: bool,
        /// Optional decision reason.
        #[arg(long)]
        reason: Option<String>,
        #[command(flatten)]
        server: ServerArgs,
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
        Command::Branch(command) => run_async(run_branch(command)),
        Command::Tag(command) => run_async(run_tag(command)),
        Command::Policy(command) => run_async(run_policy(command)),
        Command::Govern(command) => run_async(run_govern(command)),
        Command::Monitor(command) => run_async(run_monitor(command)),
        Command::Incident(command) => run_async(run_incident(command)),
        Command::Quality(command) => run_async(run_quality(command)),
        Command::Impact {
            change,
            asset,
            depth,
            fail_on_downstream,
            server,
            token,
        } => run_async(run_impact(
            change,
            asset,
            depth,
            fail_on_downstream,
            server,
            token,
        )),
        Command::Metric(command) => run_async(run_metric(command)),
        Command::Glossary(command) => run_async(run_glossary(command)),
        Command::Product(command) => run_async(run_product(command)),
        Command::Share(command) => run_async(run_share(command)),
        Command::Marketplace(command) => run_async(run_marketplace(command)),
        Command::Transpile {
            sql,
            from_dialect,
            to_dialect,
            server,
            token,
        } => run_async(run_transpile(sql, from_dialect, to_dialect, server, token)),
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
        Command::Asset(command) => run_async(run_asset(command)),
        Command::TrainingRun(command) => run_async(run_training_run(command)),
        Command::Provenance(command) => run_async(run_provenance(command)),
        Command::Deletion(command) => run_async(run_deletion(command)),
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
            token,
        } => {
            let options = parse_pairs(&storage_options)?;
            let created =
                client::warehouse_create(&server, token.as_deref(), &name, &storage_root, &options)
                    .await?;
            let id = created.get("id").and_then(Value::as_str).unwrap_or("?");
            println!("created warehouse {name} (id {id})");
            Ok(())
        }
        WarehouseCommand::List { server, token } => {
            let body = client::warehouse_list(&server, token.as_deref()).await?;
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
            token,
        } => {
            let levels = parse_namespace_arg(&namespace)?;
            let props = parse_pairs(&properties)?;
            client::namespace_create(&server, token.as_deref(), &warehouse, &levels, &props)
                .await?;
            println!("created namespace {namespace} in warehouse {warehouse}");
            Ok(())
        }
        NamespaceCommand::List {
            warehouse,
            parent,
            server,
            token,
        } => {
            let parent_levels = parent.as_deref().map(parse_namespace_arg).transpose()?;
            let body = client::namespace_list(
                &server,
                token.as_deref(),
                &warehouse,
                parent_levels.as_deref(),
            )
            .await?;
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
            token,
        } => {
            let levels = parse_namespace_arg(&namespace)?;
            let body = client::table_list(&server, token.as_deref(), &warehouse, &levels).await?;
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
            token,
        } => {
            let mut levels = parse_namespace_arg(&table)?;
            if levels.len() < 2 {
                return Err(CliError(format!(
                    "invalid table {table:?}: expected namespace.table"
                )));
            }
            let name = levels.pop().unwrap_or_default();
            let body =
                client::table_load(&server, token.as_deref(), &warehouse, &levels, &name).await?;

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

/// `meridian branch ...` — catalog branches & tags, Data CI/CD (Pillar K).
#[allow(clippy::too_many_lines)] // one match arm per branch subcommand
async fn run_branch(command: BranchCommand) -> Result<(), CliError> {
    match command {
        BranchCommand::Create {
            name,
            base,
            warehouse,
            namespaces,
            expires_in_s,
            server,
            token,
        } => {
            let mut body = json!({ "name": name, "base_ref": base });
            if !namespaces.is_empty() {
                body["namespaces"] = json!(namespaces);
                if let Some(wh) = &warehouse {
                    body["warehouse"] = json!(wh);
                }
            }
            if let Some(exp) = expires_in_s {
                body["expires_in_s"] = json!(exp);
            }
            let created =
                client::gov_post(&server, token.as_deref(), "/api/v2/branches", &body).await?;
            let id = created.get("id").and_then(Value::as_str).unwrap_or("?");
            println!("created branch {name} (id {id}, base {base})");
            println!(
                "  mount from any engine as:  {}@{name}",
                warehouse.as_deref().unwrap_or("<warehouse>")
            );
            Ok(())
        }
        BranchCommand::List { server, token } => {
            let body = client::gov_get(&server, token.as_deref(), "/api/v2/branches", &[]).await?;
            print_branch_list(&body);
            Ok(())
        }
        BranchCommand::Diff {
            name,
            server,
            token,
        } => {
            let body = client::gov_get(
                &server,
                token.as_deref(),
                &format!("/api/v2/branches/{name}/diff"),
                &[],
            )
            .await?;
            print_branch_diff(&name, &body);
            Ok(())
        }
        BranchCommand::Gate {
            name,
            server,
            token,
        } => {
            let body = client::gov_get(
                &server,
                token.as_deref(),
                &format!("/api/v2/branches/{name}/gate"),
                &[],
            )
            .await?;
            print_branch_gate(&name, &body)
        }
        BranchCommand::Merge {
            name,
            server,
            token,
        } => {
            let body = client::gov_post(
                &server,
                token.as_deref(),
                &format!("/api/v2/branches/{name}/merge"),
                &json!({}),
            )
            .await?;
            let merged = body
                .get("merged_tables")
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            println!("merged branch {name} into main: {merged} table(s) fast-forwarded");
            Ok(())
        }
        BranchCommand::Delete {
            name,
            server,
            token,
        } => {
            client::gov_delete(
                &server,
                token.as_deref(),
                &format!("/api/v2/branches/{name}"),
            )
            .await?;
            println!("deleted branch {name}");
            Ok(())
        }
        BranchCommand::Sweep { server, token } => {
            let body = client::gov_post(
                &server,
                token.as_deref(),
                "/api/v2/branches/sweep",
                &json!({}),
            )
            .await?;
            let swept = body.get("swept").and_then(Value::as_array);
            match swept.filter(|s| !s.is_empty()) {
                Some(s) => {
                    println!("swept {} expired branch(es):", s.len());
                    for name in s {
                        println!("  - {}", name.as_str().unwrap_or("?"));
                    }
                }
                None => println!("no expired branches to sweep"),
            }
            Ok(())
        }
        BranchCommand::Tag(command) => run_catalog_tag(command).await,
    }
}

/// `meridian branch tag ...` — immutable catalog tags (Pillar K).
async fn run_catalog_tag(command: CatalogTagCommand) -> Result<(), CliError> {
    match command {
        CatalogTagCommand::Create {
            name,
            from_ref,
            server,
            token,
        } => {
            let created = client::gov_post(
                &server,
                token.as_deref(),
                "/api/v2/tags",
                &json!({ "name": name, "from_ref": from_ref }),
            )
            .await?;
            let id = created.get("id").and_then(Value::as_str).unwrap_or("?");
            println!("created tag {name} (id {id}, from {from_ref})");
            Ok(())
        }
        CatalogTagCommand::List { server, token } => {
            let body = client::gov_get(&server, token.as_deref(), "/api/v2/tags", &[]).await?;
            let tags = body.get("tags").and_then(Value::as_array);
            match tags.filter(|t| !t.is_empty()) {
                None => {
                    println!("no tags");
                    Ok(())
                }
                Some(tags) => {
                    let rows: Vec<Vec<String>> = tags
                        .iter()
                        .map(|t| vec![field_str(t, "name"), field_str(t, "base_ref")])
                        .collect();
                    print!("{}", client::render_table(&["NAME", "FROM"], &rows));
                    Ok(())
                }
            }
        }
        CatalogTagCommand::Delete {
            name,
            server,
            token,
        } => {
            client::gov_delete(&server, token.as_deref(), &format!("/api/v2/tags/{name}")).await?;
            println!("deleted tag {name}");
            Ok(())
        }
    }
}

/// Renders the `branch list` response as a table of branches and tags.
fn print_branch_list(body: &Value) {
    let branches = body.get("branches").and_then(Value::as_array);
    let tags = body.get("tags").and_then(Value::as_array);
    if branches.is_none_or(Vec::is_empty) && tags.is_none_or(Vec::is_empty) {
        println!("no branches or tags");
        return;
    }
    let rows: Vec<Vec<String>> = branches
        .into_iter()
        .flatten()
        .chain(tags.into_iter().flatten())
        .map(|b| {
            vec![
                field_str(b, "name"),
                field_str(b, "kind"),
                field_str(b, "base_ref"),
                field_str(b, "state"),
                b.get("diverged_tables")
                    .and_then(Value::as_i64)
                    .unwrap_or(0)
                    .to_string(),
                b.get("expires_at")
                    .and_then(Value::as_str)
                    .unwrap_or("-")
                    .to_owned(),
            ]
        })
        .collect();
    print!(
        "{}",
        client::render_table(
            &["NAME", "KIND", "BASE", "STATE", "DIVERGED", "EXPIRES"],
            &rows
        )
    );
}

/// Renders the `branch diff` response.
fn print_branch_diff(name: &str, body: &Value) {
    let count = body
        .get("diverged_table_count")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    println!(
        "branch {name} vs {}: {count} diverged table(s)",
        field_str(body, "base")
    );
    let Some(tables) = body.get("tables").and_then(Value::as_array) else {
        return;
    };
    let cols = |t: &Value, key: &str| {
        t.get("schema")
            .and_then(|s| s.get(key))
            .and_then(Value::as_array)
            .map_or(0, Vec::len)
    };
    let rows: Vec<Vec<String>> = tables
        .iter()
        .map(|t| {
            let row_delta = t
                .get("rows")
                .and_then(|r| r.get("delta"))
                .map_or_else(|| "-".to_owned(), ToString::to_string);
            vec![
                field_str(t, "table"),
                format!(
                    "+{}/-{}/~{}",
                    cols(t, "added_columns"),
                    cols(t, "dropped_columns"),
                    cols(t, "type_changed_columns")
                ),
                row_delta,
            ]
        })
        .collect();
    print!(
        "{}",
        client::render_table(&["TABLE", "COLS(+/-/~)", "ROW_DELTA"], &rows)
    );
}

/// Renders the `branch gate` response; returns non-zero (an `Err`) on failure
/// so a CI job blocks a merge when the gate fails.
fn print_branch_gate(name: &str, body: &Value) -> Result<(), CliError> {
    let passes = body.get("passes").and_then(Value::as_bool).unwrap_or(false);
    if passes {
        println!("gate PASS for branch {name}");
        if let Some(warnings) = body
            .get("warnings")
            .and_then(Value::as_array)
            .filter(|w| !w.is_empty())
        {
            println!("  {} warning(s):", warnings.len());
            for entry in warnings {
                println!(
                    "  - {} on {}",
                    field_str(entry, "contract"),
                    field_str(entry, "table")
                );
            }
        }
        Ok(())
    } else {
        println!("gate FAIL for branch {name}");
        for entry in body
            .get("blocking")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            println!(
                "  - {} on {} ({})",
                field_str(entry, "contract"),
                field_str(entry, "table"),
                field_str(entry, "mode")
            );
        }
        Err(CliError(format!("merge gate failed for branch {name}")))
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

/// Parses a `k=v,k=v` spec string into its pairs (CLI input pins / snapshots).
fn parse_kv_spec(spec: &str) -> Result<std::collections::HashMap<String, String>, CliError> {
    let mut map = std::collections::HashMap::new();
    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (k, v) = part.split_once('=').ok_or_else(|| {
            CliError(format!(
                "malformed spec segment {part:?}: expected key=value"
            ))
        })?;
        map.insert(k.trim().to_owned(), v.trim().to_owned());
    }
    Ok(map)
}

/// Prints a JSON value pretty (reports that are read whole, not tabulated).
fn print_json(v: &Value) {
    println!(
        "{}",
        serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
    );
}

/// `meridian asset ...` (Pillar I, I-F1).
async fn run_asset(command: AssetCommand) -> Result<(), CliError> {
    match command {
        AssetCommand::Create {
            kind,
            name,
            description,
            owner,
            warehouse,
            storage_prefix,
            metadata,
            tags,
            server,
            token,
        } => {
            let metadata_val: Value = match metadata {
                Some(raw) => serde_json::from_str(&raw)
                    .map_err(|e| CliError(format!("--metadata is not valid JSON: {e}")))?,
                None => serde_json::json!({}),
            };
            let body = serde_json::json!({
                "kind": kind,
                "name": name,
                "description": description,
                "owner": owner,
                "warehouse": warehouse,
                "storage_prefix": storage_prefix,
                "metadata": metadata_val,
                "tags": tags,
            });
            let created =
                client::gov_post(&server, token.as_deref(), "/api/v2/assets", &body).await?;
            println!(
                "created {} asset {} ({})",
                field_str(&created, "kind"),
                field_str(&created, "id"),
                field_str(&created, "name"),
            );
            Ok(())
        }
        AssetCommand::List {
            kind,
            server,
            token,
        } => {
            let query: Vec<(&str, String)> = kind.map(|k| vec![("type", k)]).unwrap_or_default();
            let body = client::gov_get(&server, token.as_deref(), "/api/v2/assets", &query).await?;
            let assets = body
                .get("assets")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let rows: Vec<Vec<String>> = assets
                .iter()
                .map(|a| {
                    vec![
                        field_str(a, "id"),
                        field_str(a, "kind"),
                        field_str(a, "name"),
                        field_str(a, "storage_prefix"),
                    ]
                })
                .collect();
            print!(
                "{}",
                client::render_table(&["ID", "KIND", "NAME", "STORAGE PREFIX"], &rows)
            );
            Ok(())
        }
        AssetCommand::Get { id, server, token } => {
            let body = client::gov_get(
                &server,
                token.as_deref(),
                &format!("/api/v2/assets/{id}"),
                &[],
            )
            .await?;
            print_json(&body);
            Ok(())
        }
        AssetCommand::Credentials { id, server, token } => {
            let body = client::gov_post(
                &server,
                token.as_deref(),
                &format!("/api/v2/assets/{id}/credentials"),
                &serde_json::json!({}),
            )
            .await?;
            print_json(&body);
            Ok(())
        }
    }
}

/// `meridian training-run ...` (Pillar I, I-F2).
async fn run_training_run(command: TrainingRunCommand) -> Result<(), CliError> {
    match command {
        TrainingRunCommand::Pin {
            model,
            model_version,
            model_asset_id,
            inputs,
            server,
            token,
        } => {
            let mut input_bodies = Vec::with_capacity(inputs.len());
            for spec in &inputs {
                let kv = parse_kv_spec(spec)?;
                let table_ref = kv
                    .get("table_ref")
                    .ok_or_else(|| CliError(format!("input {spec:?} is missing table_ref")))?;
                let snapshot_id: i64 = kv
                    .get("snapshot_id")
                    .ok_or_else(|| CliError(format!("input {spec:?} is missing snapshot_id")))?
                    .parse()
                    .map_err(|_| {
                        CliError(format!("input {spec:?} has a non-integer snapshot_id"))
                    })?;
                input_bodies.push(serde_json::json!({
                    "table_ref": table_ref,
                    "table_id": kv.get("table_id"),
                    "snapshot_id": snapshot_id,
                }));
            }
            let body = serde_json::json!({
                "model": model,
                "model_version": model_version,
                "model_asset_id": model_asset_id,
                "inputs": input_bodies,
            });
            let created =
                client::gov_post(&server, token.as_deref(), "/api/v2/training-runs", &body).await?;
            println!(
                "pinned training run {} for {}@{} ({} inputs)",
                field_str(&created, "id"),
                field_str(&created, "model"),
                field_str(&created, "model_version"),
                created
                    .get("inputs")
                    .and_then(Value::as_array)
                    .map_or(0, Vec::len),
            );
            Ok(())
        }
        TrainingRunCommand::Get { id, server, token } => {
            let body = client::gov_get(
                &server,
                token.as_deref(),
                &format!("/api/v2/training-runs/{id}"),
                &[],
            )
            .await?;
            print_json(&body);
            Ok(())
        }
    }
}

/// `meridian provenance ...` (Pillar I, I-F3).
async fn run_provenance(command: ProvenanceCommand) -> Result<(), CliError> {
    match command {
        ProvenanceCommand::Show {
            model,
            version,
            server,
            token,
        } => {
            let query: Vec<(&str, String)> =
                version.map(|v| vec![("version", v)]).unwrap_or_default();
            let body = client::gov_get(
                &server,
                token.as_deref(),
                &format!("/api/v2/models/{model}/provenance"),
                &query,
            )
            .await?;
            print_json(&body);
            Ok(())
        }
        ProvenanceCommand::AiActSummary {
            model,
            version,
            server,
            token,
        } => {
            let query: Vec<(&str, String)> =
                version.map(|v| vec![("version", v)]).unwrap_or_default();
            let body = client::gov_get(
                &server,
                token.as_deref(),
                &format!("/api/v2/models/{model}/ai-act-summary"),
                &query,
            )
            .await?;
            print_json(&body);
            Ok(())
        }
    }
}

/// `meridian deletion ...` (Pillar I, I-F4).
async fn run_deletion(command: DeletionCommand) -> Result<(), CliError> {
    match command {
        DeletionCommand::Open {
            name,
            subject,
            reason,
            server,
            token,
        } => {
            let body = serde_json::json!({
                "name": name,
                "subject": subject,
                "reason": reason,
            });
            let created = client::gov_post(
                &server,
                token.as_deref(),
                "/api/v2/deletion-campaigns",
                &body,
            )
            .await?;
            println!(
                "opened deletion campaign {} ({})",
                field_str(&created, "id"),
                field_str(&created, "name"),
            );
            Ok(())
        }
        DeletionCommand::AddSnapshots {
            id,
            snapshots,
            server,
            token,
        } => {
            let mut snapshot_bodies = Vec::with_capacity(snapshots.len());
            for spec in &snapshots {
                let kv = parse_kv_spec(spec)?;
                let table_ref = kv
                    .get("table_ref")
                    .ok_or_else(|| CliError(format!("snapshot {spec:?} is missing table_ref")))?;
                let snapshot_id: i64 = kv
                    .get("snapshot_id")
                    .ok_or_else(|| CliError(format!("snapshot {spec:?} is missing snapshot_id")))?
                    .parse()
                    .map_err(|_| {
                        CliError(format!("snapshot {spec:?} has a non-integer snapshot_id"))
                    })?;
                snapshot_bodies.push(serde_json::json!({
                    "table_ref": table_ref,
                    "table_id": kv.get("table_id"),
                    "snapshot_id": snapshot_id,
                    "branch": kv.get("branch"),
                }));
            }
            let body = serde_json::json!({ "snapshots": snapshot_bodies });
            let result = client::gov_post(
                &server,
                token.as_deref(),
                &format!("/api/v2/deletion-campaigns/{id}/snapshots"),
                &body,
            )
            .await?;
            println!(
                "recorded {} model exposure(s) for campaign {id}",
                result
                    .get("model_exposures_recorded")
                    .and_then(Value::as_i64)
                    .unwrap_or(0),
            );
            Ok(())
        }
        DeletionCommand::Evidence { id, server, token } => {
            let body = client::gov_get(
                &server,
                token.as_deref(),
                &format!("/api/v2/deletion-campaigns/{id}/evidence"),
                &[],
            )
            .await?;
            print_json(&body);
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

// ---------------------------------------------------------------------------
// Data quality (Pillar E): monitors, incidents, quality score
// ---------------------------------------------------------------------------

async fn run_monitor(command: MonitorCommand) -> Result<(), CliError> {
    match command {
        MonitorCommand::List { server, token } => monitor_list(&server, token.as_deref()).await,
        MonitorCommand::Create {
            name,
            kind,
            warehouse,
            bound_to,
            namespace,
            table,
            severity,
            server,
            token,
        } => {
            let mut body = serde_json::json!({
                "name": name,
                "kind": kind,
                "warehouse": warehouse,
                "bound_to": bound_to,
                "namespace": namespace,
            });
            if let Some(table) = table {
                body["table"] = Value::String(table);
            }
            if let Some(severity) = severity {
                body["severity"] = Value::String(severity);
            }
            let created = client::q_post(
                &server,
                token.as_deref(),
                "/api/v2/quality/monitors",
                Some(&body),
            )
            .await?;
            println!("created monitor {}", field_str(&created, "id"));
            Ok(())
        }
        MonitorCommand::Set {
            id,
            enable,
            disable,
            server,
            token,
        } => {
            if !enable && !disable {
                return Err(CliError("pass --enable or --disable".to_owned()));
            }
            let body = serde_json::json!({ "enabled": enable && !disable });
            let path = format!("/api/v2/quality/monitors/{id}");
            let updated = client::q_patch(&server, token.as_deref(), &path, &body).await?;
            println!(
                "monitor {} enabled={}",
                field_str(&updated, "id"),
                updated
                    .get("enabled")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
            );
            Ok(())
        }
        MonitorCommand::Rm { id, server, token } => {
            let path = format!("/api/v2/quality/monitors/{id}");
            client::q_delete(&server, token.as_deref(), &path).await?;
            println!("deleted monitor {id}");
            Ok(())
        }
        MonitorCommand::Results {
            monitor,
            limit,
            server,
            token,
        } => monitor_results(&server, token.as_deref(), monitor.as_deref(), limit).await,
    }
}

/// `meridian monitor list` — renders the monitor table.
async fn monitor_list(server: &str, token: Option<&str>) -> Result<(), CliError> {
    let body = client::q_get(server, token, "/api/v2/quality/monitors", &[]).await?;
    let empty = Vec::new();
    let monitors = body
        .get("monitors")
        .and_then(Value::as_array)
        .unwrap_or(&empty);
    if monitors.is_empty() {
        println!("no monitors");
        return Ok(());
    }
    let rows: Vec<Vec<String>> = monitors
        .iter()
        .map(|m| {
            vec![
                field_str(m, "id"),
                field_str(m, "name"),
                field_str(m, "kind"),
                field_str(m, "bound_to"),
                field_str(m, "severity"),
                m.get("enabled")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                    .to_string(),
            ]
        })
        .collect();
    print!(
        "{}",
        client::render_table(
            &["ID", "NAME", "KIND", "BOUND", "SEVERITY", "ENABLED"],
            &rows
        )
    );
    Ok(())
}

/// `meridian monitor results` — renders the recent evaluation series.
async fn monitor_results(
    server: &str,
    token: Option<&str>,
    monitor: Option<&str>,
    limit: i64,
) -> Result<(), CliError> {
    let mut query: Vec<(&str, String)> = vec![("limit", limit.to_string())];
    if let Some(monitor) = monitor {
        query.push(("monitor_id", monitor.to_owned()));
    }
    let body = client::q_get(server, token, "/api/v2/quality/monitors/results", &query).await?;
    let empty = Vec::new();
    let results = body
        .get("results")
        .and_then(Value::as_array)
        .unwrap_or(&empty);
    if results.is_empty() {
        println!("no results");
        return Ok(());
    }
    let rows: Vec<Vec<String>> = results
        .iter()
        .map(|r| {
            vec![
                field_str(r, "kind"),
                field_str(r, "status"),
                field_str(r, "result_kind"),
                field_str(r, "detail"),
            ]
        })
        .collect();
    print!(
        "{}",
        client::render_table(&["KIND", "STATUS", "RESULT", "DETAIL"], &rows)
    );
    Ok(())
}

async fn run_incident(command: IncidentCommand) -> Result<(), CliError> {
    match command {
        IncidentCommand::List {
            live,
            status,
            limit,
            server,
            token,
        } => {
            let mut query: Vec<(&str, String)> = vec![("limit", limit.to_string())];
            if live {
                query.push(("live", "true".to_owned()));
            }
            if let Some(status) = &status {
                query.push(("status", status.clone()));
            }
            let body = client::q_get(
                &server,
                token.as_deref(),
                "/api/v2/quality/incidents",
                &query,
            )
            .await?;
            let empty = Vec::new();
            let incidents = body
                .get("incidents")
                .and_then(Value::as_array)
                .unwrap_or(&empty);
            if incidents.is_empty() {
                println!("no incidents");
                return Ok(());
            }
            let rows: Vec<Vec<String>> = incidents
                .iter()
                .map(|i| {
                    let blast = i
                        .get("blast_radius")
                        .and_then(Value::as_array)
                        .map_or(0, Vec::len);
                    vec![
                        field_str(i, "id"),
                        field_str(i, "status"),
                        field_str(i, "severity"),
                        field_str(i, "table_ident"),
                        field_str(i, "kind"),
                        blast.to_string(),
                    ]
                })
                .collect();
            print!(
                "{}",
                client::render_table(
                    &["ID", "STATUS", "SEVERITY", "TABLE", "KIND", "DOWNSTREAM"],
                    &rows
                )
            );
        }
        IncidentCommand::Ack { id, server, token } => {
            let path = format!("/api/v2/quality/incidents/{id}/ack");
            let body = client::q_post(&server, token.as_deref(), &path, None).await?;
            println!("acknowledged incident {}", field_str(&body, "id"));
        }
        IncidentCommand::Resolve { id, server, token } => {
            let path = format!("/api/v2/quality/incidents/{id}/resolve");
            let body = client::q_post(&server, token.as_deref(), &path, None).await?;
            println!("resolved incident {}", field_str(&body, "id"));
        }
    }
    Ok(())
}

async fn run_quality(command: QualityCommand) -> Result<(), CliError> {
    match command {
        QualityCommand::Status {
            warehouse,
            namespace,
            table,
            server,
            token,
        } => {
            let path = format!(
                "/api/v2/quality/tables/{warehouse}/{}/{table}/status",
                encode_ns_dotted(&namespace)
            );
            let body = client::q_get(&server, token.as_deref(), &path, &[]).await?;
            println!(
                "{}  status={}  live={} (high {} / medium {} / low {})",
                field_str(&body, "ident"),
                field_str(&body, "status"),
                field_i64(&body, "live_incidents"),
                field_i64(&body, "high"),
                field_i64(&body, "medium"),
                field_i64(&body, "low"),
            );
        }
        QualityCommand::Score {
            warehouse,
            namespace,
            table,
            server,
            token,
        } => {
            let path = format!(
                "/api/v2/quality/tables/{warehouse}/{}/{table}/score",
                encode_ns_dotted(&namespace)
            );
            let body = client::q_get(&server, token.as_deref(), &path, &[]).await?;
            println!(
                "{}  score={} (grade {})",
                field_str(&body, "ident"),
                field_i64(&body, "score"),
                field_str(&body, "grade"),
            );
            if let Some(c) = body.get("components").and_then(Value::as_object) {
                let f = |k: &str| c.get(k).and_then(Value::as_f64).unwrap_or(0.0);
                println!(
                    "  monitors {:.2}  contract {:.2}  ownership {:.2}  docs {:.2}  freshness {:.2}",
                    f("monitors"),
                    f("contract"),
                    f("ownership"),
                    f("docs"),
                    f("freshness"),
                );
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Semantics: metrics (G-F2), glossary (G-F3), products (G-F4), transpile (G-F1)
// ---------------------------------------------------------------------------

/// `meridian metric ...` — manage semantic metrics.
async fn run_metric(command: MetricCommand) -> Result<(), CliError> {
    match command {
        MetricCommand::List { server } => {
            let body = client::q_get(
                &server.server,
                server.token.as_deref(),
                "/api/v2/metrics",
                &[],
            )
            .await?;
            let empty = Vec::new();
            let metrics = body["metrics"].as_array().unwrap_or(&empty);
            if metrics.is_empty() {
                println!("(no metrics)");
            }
            for m in metrics {
                println!(
                    "{}  {}  source={}  [{}]",
                    field_str(m, "id"),
                    field_str(m, "name"),
                    field_str(m, "source"),
                    field_str(m, "certification"),
                );
            }
        }
        MetricCommand::Get { id, server } => {
            let body = client::q_get(
                &server.server,
                server.token.as_deref(),
                &format!("/api/v2/metrics/{id}"),
                &[],
            )
            .await?;
            print_metric(&body);
        }
        MetricCommand::Create {
            name,
            source,
            expression,
            dialect,
            dimensions,
            filters,
            grain,
            certification,
            server,
        } => {
            let payload = serde_json::json!({
                "name": name,
                "source": source,
                "expression": expression,
                "dialect": dialect,
                "dimensions": dimensions,
                "filters": filters,
                "grain": grain,
                "certification": certification,
            });
            let body = client::q_post(
                &server.server,
                server.token.as_deref(),
                "/api/v2/metrics",
                Some(&payload),
            )
            .await?;
            println!("created metric {}", field_str(&body, "id"));
        }
        MetricCommand::Delete { id, server } => {
            client::q_delete(
                &server.server,
                server.token.as_deref(),
                &format!("/api/v2/metrics/{id}"),
            )
            .await?;
            println!("deleted metric {id}");
        }
        MetricCommand::Compile { id, engine, server } => {
            let body = client::q_get(
                &server.server,
                server.token.as_deref(),
                &format!("/api/v2/metrics/{id}/compile"),
                &[("engine", engine)],
            )
            .await?;
            println!("status: {}", field_str(&body, "status"));
            match body.get("sql").and_then(Value::as_str) {
                Some(sql) => println!("{sql}"),
                None => println!("(no SQL — unsupported)"),
            }
            print_diagnostics(&body);
        }
    }
    Ok(())
}

/// `meridian glossary ...` — manage the business glossary.
async fn run_glossary(command: GlossaryCommand) -> Result<(), CliError> {
    match command {
        GlossaryCommand::List { server } => {
            let body = client::q_get(
                &server.server,
                server.token.as_deref(),
                "/api/v2/glossary/terms",
                &[],
            )
            .await?;
            let empty = Vec::new();
            let terms = body["terms"].as_array().unwrap_or(&empty);
            if terms.is_empty() {
                println!("(no terms)");
            }
            for t in terms {
                println!(
                    "{}  {}  [{}]",
                    field_str(t, "id"),
                    field_str(t, "name"),
                    field_str(t, "certification"),
                );
            }
        }
        GlossaryCommand::Get { id, server } => {
            let body = client::q_get(
                &server.server,
                server.token.as_deref(),
                &format!("/api/v2/glossary/terms/{id}"),
                &[],
            )
            .await?;
            println!(
                "{}  [{}]",
                field_str(&body, "name"),
                field_str(&body, "certification")
            );
            println!("{}", field_str(&body, "definition"));
            if let Some(links) = body.get("links").and_then(Value::as_array) {
                for l in links {
                    println!(
                        "  -> {} {}",
                        field_str(l, "asset_kind"),
                        field_str(l, "asset_ref")
                    );
                }
            }
        }
        GlossaryCommand::Create {
            name,
            definition,
            certification,
            server,
        } => {
            let payload = serde_json::json!({ "name": name, "definition": definition, "certification": certification });
            let body = client::q_post(
                &server.server,
                server.token.as_deref(),
                "/api/v2/glossary/terms",
                Some(&payload),
            )
            .await?;
            println!("created term {}", field_str(&body, "id"));
        }
        GlossaryCommand::Delete { id, server } => {
            client::q_delete(
                &server.server,
                server.token.as_deref(),
                &format!("/api/v2/glossary/terms/{id}"),
            )
            .await?;
            println!("deleted term {id}");
        }
        GlossaryCommand::Link {
            id,
            kind,
            asset_ref,
            server,
        } => {
            let payload = serde_json::json!({ "asset_kind": kind, "asset_ref": asset_ref });
            let body = client::q_post(
                &server.server,
                server.token.as_deref(),
                &format!("/api/v2/glossary/terms/{id}/links"),
                Some(&payload),
            )
            .await?;
            println!("linked ({})", field_str(&body, "id"));
        }
    }
    Ok(())
}

/// `meridian product ...` — manage certified data products.
#[allow(clippy::too_many_lines)] // one match arm per product subcommand
async fn run_product(command: ProductCommand) -> Result<(), CliError> {
    match command {
        ProductCommand::List { server } => {
            let body = client::q_get(
                &server.server,
                server.token.as_deref(),
                "/api/v2/products",
                &[],
            )
            .await?;
            let empty = Vec::new();
            let products = body["products"].as_array().unwrap_or(&empty);
            if products.is_empty() {
                println!("(no products)");
            }
            for p in products {
                println!(
                    "{}  {}  [{}]",
                    field_str(p, "id"),
                    field_str(p, "name"),
                    field_str(p, "certification"),
                );
            }
        }
        ProductCommand::Get { id, server } => {
            let body = client::q_get(
                &server.server,
                server.token.as_deref(),
                &format!("/api/v2/products/{id}"),
                &[],
            )
            .await?;
            println!(
                "{}  [{}]",
                field_str(&body, "name"),
                field_str(&body, "certification")
            );
            if let Some(members) = body.get("members").and_then(Value::as_array) {
                for m in members {
                    println!(
                        "  - {} {}",
                        field_str(m, "member_kind"),
                        field_str(m, "member_ref")
                    );
                }
            }
        }
        ProductCommand::Create {
            name,
            description,
            sla,
            certification,
            server,
        } => {
            let payload = serde_json::json!({
                "name": name,
                "description": description,
                "sla": sla,
                "certification": certification,
            });
            let body = client::q_post(
                &server.server,
                server.token.as_deref(),
                "/api/v2/products",
                Some(&payload),
            )
            .await?;
            println!("created product {}", field_str(&body, "id"));
        }
        ProductCommand::Delete { id, server } => {
            client::q_delete(
                &server.server,
                server.token.as_deref(),
                &format!("/api/v2/products/{id}"),
            )
            .await?;
            println!("deleted product {id}");
        }
        ProductCommand::AddMember {
            id,
            kind,
            member_ref,
            server,
        } => {
            let payload = serde_json::json!({ "member_kind": kind, "member_ref": member_ref });
            let body = client::q_post(
                &server.server,
                server.token.as_deref(),
                &format!("/api/v2/products/{id}/members"),
                Some(&payload),
            )
            .await?;
            println!("added member ({})", field_str(&body, "id"));
        }
        ProductCommand::Status { id, server } => {
            let body = client::q_get(
                &server.server,
                server.token.as_deref(),
                &format!("/api/v2/products/{id}/status"),
                &[],
            )
            .await?;
            println!(
                "{}  [{}]  health={}  members={}",
                field_str(&body["product"], "name"),
                field_str(&body["product"], "certification"),
                field_str(&body, "health_rollup"),
                field_i64(&body, "member_total"),
            );
        }
    }
    Ok(())
}

/// `meridian share ...` — cross-org data sharing (Pillar J, J-F1).
#[allow(clippy::too_many_lines)] // one match arm per share subcommand
async fn run_share(command: ShareCommand) -> Result<(), CliError> {
    match command {
        ShareCommand::List { server } => {
            let body = client::q_get(
                &server.server,
                server.token.as_deref(),
                "/api/v2/shares",
                &[],
            )
            .await?;
            let empty = Vec::new();
            let list = body["shares"].as_array().unwrap_or(&empty);
            if list.is_empty() {
                println!("(no shares)");
            }
            for s in list {
                let state = if s["revoked"].as_bool().unwrap_or(false) {
                    "revoked"
                } else {
                    "active"
                };
                println!(
                    "{}  {}  -> {}  [{}]",
                    field_str(s, "id"),
                    field_str(s, "name"),
                    field_str(s, "recipient"),
                    state,
                );
            }
        }
        ShareCommand::Get { id, server } => {
            let body = client::q_get(
                &server.server,
                server.token.as_deref(),
                &format!("/api/v2/shares/{id}"),
                &[],
            )
            .await?;
            println!(
                "{}  -> {}  revoked={}",
                field_str(&body, "name"),
                field_str(&body, "recipient"),
                body["revoked"].as_bool().unwrap_or(false),
            );
            if let Some(grants) = body.get("grants").and_then(Value::as_array) {
                for g in grants {
                    println!(
                        "  - {} {}{}{}",
                        field_str(g, "securable_kind"),
                        field_str(g, "securable_ref"),
                        g["row_filter"]
                            .as_str()
                            .map(|f| format!("  filter[{f}]"))
                            .unwrap_or_default(),
                        g["column_mask"]
                            .as_array()
                            .filter(|m| !m.is_empty())
                            .map(|m| format!("  mask{m:?}"))
                            .unwrap_or_default(),
                    );
                }
            }
        }
        ShareCommand::Create {
            name,
            recipient,
            terms,
            server,
        } => {
            let payload = serde_json::json!({
                "name": name,
                "recipient": recipient,
                "terms": terms,
            });
            let body = client::q_post(
                &server.server,
                server.token.as_deref(),
                "/api/v2/shares",
                Some(&payload),
            )
            .await?;
            println!("created share {}", field_str(&body, "id"));
            // The token is shown exactly once — the operator must copy it now.
            println!("token: {}", field_str(&body, "token"));
            println!("(deliver the token to the recipient over a secure channel)");
        }
        ShareCommand::Grant {
            id,
            kind,
            securable_ref,
            row_filter,
            mask,
            server,
        } => {
            let payload = serde_json::json!({
                "securable_kind": kind,
                "securable_ref": securable_ref,
                "row_filter": row_filter,
                "column_mask": if mask.is_empty() { None } else { Some(mask) },
            });
            let body = client::q_post(
                &server.server,
                server.token.as_deref(),
                &format!("/api/v2/shares/{id}/grants"),
                Some(&payload),
            )
            .await?;
            println!("added grant {}", field_str(&body, "id"));
        }
        ShareCommand::Ungrant { grant_id, server } => {
            client::q_delete(
                &server.server,
                server.token.as_deref(),
                &format!("/api/v2/shares/grants/{grant_id}"),
            )
            .await?;
            println!("removed grant {grant_id}");
        }
        ShareCommand::Revoke { id, server } => {
            client::q_post(
                &server.server,
                server.token.as_deref(),
                &format!("/api/v2/shares/{id}/revoke"),
                None,
            )
            .await?;
            println!("revoked share {id}");
        }
        ShareCommand::Delete { id, server } => {
            client::q_delete(
                &server.server,
                server.token.as_deref(),
                &format!("/api/v2/shares/{id}"),
            )
            .await?;
            println!("deleted share {id}");
        }
    }
    Ok(())
}

/// `meridian marketplace ...` — the internal data marketplace (Pillar J, J-F2).
async fn run_marketplace(command: MarketplaceCommand) -> Result<(), CliError> {
    match command {
        MarketplaceCommand::Products { server } => {
            let body = client::q_get(
                &server.server,
                server.token.as_deref(),
                "/api/v2/marketplace/products",
                &[],
            )
            .await?;
            let empty = Vec::new();
            let list = body["products"].as_array().unwrap_or(&empty);
            if list.is_empty() {
                println!("(no products)");
            }
            for p in list {
                println!(
                    "{}  {}  [{}]",
                    field_str(p, "id"),
                    field_str(p, "name"),
                    field_str(p, "certification"),
                );
            }
        }
        MarketplaceCommand::Request {
            kind,
            securable_ref,
            privilege,
            purpose,
            server,
        } => {
            let payload = serde_json::json!({
                "securable_type": kind,
                "securable_id": securable_ref,
                "privilege": privilege,
                "purpose": purpose,
            });
            let body = client::q_post(
                &server.server,
                server.token.as_deref(),
                "/api/v2/marketplace/requests",
                Some(&payload),
            )
            .await?;
            println!(
                "created access request {} [{}]",
                field_str(&body, "id"),
                field_str(&body, "state"),
            );
        }
        MarketplaceCommand::Requests { state, server } => {
            let query: Vec<(&str, String)> = state.map(|s| vec![("state", s)]).unwrap_or_default();
            let body = client::q_get(
                &server.server,
                server.token.as_deref(),
                "/api/v2/marketplace/requests",
                &query,
            )
            .await?;
            let empty = Vec::new();
            let list = body["requests"].as_array().unwrap_or(&empty);
            if list.is_empty() {
                println!("(no requests)");
            }
            for r in list {
                println!(
                    "{}  {} on {}  [{}]  by {}",
                    field_str(r, "id"),
                    field_str(r, "privilege"),
                    field_str(r, "securable_id"),
                    field_str(r, "state"),
                    field_str(r, "principal"),
                );
            }
        }
        MarketplaceCommand::Decide {
            id,
            approve,
            reason,
            server,
        } => {
            let payload = serde_json::json!({ "approve": approve, "reason": reason });
            let body = client::q_post(
                &server.server,
                server.token.as_deref(),
                &format!("/api/v2/marketplace/requests/{id}/decide"),
                Some(&payload),
            )
            .await?;
            println!("request {id} is now [{}]", field_str(&body, "state"));
        }
    }
    Ok(())
}

/// `meridian transpile ...` — translate SQL between engine dialects (G-F1).
async fn run_transpile(
    sql: String,
    from_dialect: String,
    to_dialect: String,
    server: String,
    token: Option<String>,
) -> Result<(), CliError> {
    let payload =
        serde_json::json!({ "sql": sql, "from_dialect": from_dialect, "to_dialect": to_dialect });
    let body = client::q_post(
        &server,
        token.as_deref(),
        "/api/v2/sql/transpile",
        Some(&payload),
    )
    .await?;
    println!("status: {}", field_str(&body, "status"));
    match body.get("sql").and_then(Value::as_str) {
        Some(translated) => println!("{translated}"),
        None => println!("(no SQL — unsupported)"),
    }
    print_diagnostics(&body);
    Ok(())
}

/// Prints a metric record's fields.
fn print_metric(body: &Value) {
    println!(
        "{}  [{}]",
        field_str(body, "name"),
        field_str(body, "certification")
    );
    println!("  source:     {}", field_str(body, "source"));
    println!("  expression: {}", field_str(body, "expression"));
    println!("  dialect:    {}", field_str(body, "dialect"));
    if let Some(dims) = body.get("dimensions").and_then(Value::as_array)
        && !dims.is_empty()
    {
        let joined: Vec<String> = dims
            .iter()
            .filter_map(|d| d.as_str().map(str::to_owned))
            .collect();
        println!("  dimensions: {}", joined.join(", "));
    }
    if let Some(grain) = body.get("grain").and_then(Value::as_str) {
        println!("  grain:      {grain}");
    }
}

/// Prints the `diagnostics` array of a transpile/compile response, if any.
fn print_diagnostics(body: &Value) {
    if let Some(diagnostics) = body.get("diagnostics").and_then(Value::as_array) {
        for d in diagnostics {
            println!(
                "  [{}] {}: {}",
                field_str(d, "severity"),
                field_str(d, "code"),
                field_str(d, "message"),
            );
        }
    }
}

/// The impact CI gate (F-F5). Prints the downstream blast radius; with
/// `--fail-on-downstream`, returns an error (non-zero exit) when any downstream
/// asset is affected — the CI-blocking behavior for dbt/SQL repos.
async fn run_impact(
    change: String,
    asset: String,
    depth: Option<u32>,
    fail_on_downstream: bool,
    server: String,
    token: Option<String>,
) -> Result<(), CliError> {
    let report = client::impact(&server, token.as_deref(), &asset, &change, depth).await?;
    let empty = Vec::new();
    let affected = report
        .get("affected")
        .and_then(Value::as_array)
        .unwrap_or(&empty);

    println!(
        "impact of {} on {}: {} downstream asset(s) affected",
        field_str(&report, "change"),
        field_str(&report, "asset"),
        affected.len(),
    );

    if !affected.is_empty() {
        let rows: Vec<Vec<String>> = affected
            .iter()
            .map(|a| {
                vec![
                    a.get("ident")
                        .and_then(Value::as_str)
                        .map_or_else(|| field_str(a, "table_id"), str::to_owned),
                    field_i64(a, "depth").to_string(),
                    a.get("via_column")
                        .and_then(Value::as_str)
                        .unwrap_or("-")
                        .to_owned(),
                    a.get("owner")
                        .and_then(Value::as_str)
                        .unwrap_or("-")
                        .to_owned(),
                ]
            })
            .collect();
        print!(
            "{}",
            client::render_table(&["ASSET", "DEPTH", "VIA COLUMN", "OWNER"], &rows)
        );
    }

    let owners = report
        .get("owners")
        .and_then(Value::as_array)
        .unwrap_or(&empty);
    if !owners.is_empty() {
        let names: Vec<&str> = owners.iter().filter_map(Value::as_str).collect();
        println!("owners to notify: {}", names.join(", "));
    }

    if fail_on_downstream && !affected.is_empty() {
        return Err(CliError(format!(
            "{} downstream asset(s) would break — failing (--fail-on-downstream)",
            affected.len()
        )));
    }
    Ok(())
}

/// Encodes a dotted namespace for a URL path segment (levels joined by the
/// URL-encoded unit separator), matching the server's `decode_namespace_param`.
fn encode_ns_dotted(namespace: &str) -> String {
    namespace.split('.').collect::<Vec<_>>().join("%1F")
}
