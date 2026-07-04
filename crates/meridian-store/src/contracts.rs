//! Data contracts (Pillar E / E-F3) and the violation ledger the circuit
//! breaker (E-F4) writes.
//!
//! This module owns three things, cleanly separated:
//!
//! 1. **The model** — [`Contract`], its versioned [`ContractSpec`], and the
//!    [`ContractViolation`] record. Persistence mirrors the policy-versioning
//!    discipline in [`crate::policy`]: `contracts` holds the current version
//!    plus a denormalized spec, `contract_versions` is the append-only
//!    history, every mutation writes its audit row + outbox event on the same
//!    transaction as the state change.
//!
//! 2. **The pure evaluation engine** — [`ContractSpec::evaluate`],
//!    [`classify_schema_evolution`], and [`is_widening`] are pure functions of
//!    the base and staged [`Schema`]s (and the staged snapshot summary). They
//!    do **no I/O** and are exhaustively unit-tested here, independent of any
//!    database. This is the code the commit-path hook calls before the pointer
//!    CAS; it must be O(schema size), never O(rows) — it reads metadata only.
//!
//! 3. **Resolution + recording** — [`resolve_for_table`] finds the enabled
//!    contracts that bind to a table (directly or via its namespace chain);
//!    [`record_violation_in_tx`] writes a violation on the *caller's* commit
//!    transaction (warn / quarantine: atomic with the pointer swap), and
//!    [`record_violation`] writes one in a dedicated transaction (block: there
//!    is no commit transaction to join).
//!
//! The exact circuit-breaker semantics and the commit-invariant preservation
//! argument live in `docs/design/contracts-circuit-breaker.md`.

use chrono::{DateTime, Utc};
use meridian_common::id::WorkspaceId;
use meridian_common::{MeridianError, Result};
use meridian_iceberg::spec::{PrimitiveType, Schema, StructField, Type};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::PgPool;
use ulid::Ulid;

use crate::audit::{self, NewAuditEntry};
use crate::map_sqlx_error;
use crate::outbox::{self, NewOutboxEvent};

/// True when the error is a Postgres unique-constraint violation.
fn is_unique_violation(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
}

/// The default Iceberg branch a quarantined commit is retargeted onto.
pub const DEFAULT_QUARANTINE_BRANCH: &str = "meridian_quarantine";

// ===========================================================================
// Enums
// ===========================================================================

/// What the circuit breaker does when a contract is violated at commit time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EnforcementMode {
    /// Let the commit land; fire a violation event + record. Advisory.
    Warn,
    /// Retarget the violating commit onto an audit branch; `main` is not
    /// advanced (managed WAP, single-branch — see the design doc).
    Quarantine,
    /// Reject the commit atomically with a machine-readable error; nothing
    /// durable.
    Block,
}

impl EnforcementMode {
    /// The database/wire rendering (matches the 0018 CHECK constraint).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Warn => "warn",
            Self::Quarantine => "quarantine",
            Self::Block => "block",
        }
    }

    /// Parses the wire rendering back into the enum.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "warn" => Some(Self::Warn),
            "quarantine" => Some(Self::Quarantine),
            "block" => Some(Self::Block),
            _ => None,
        }
    }
}

impl std::fmt::Display for EnforcementMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What a contract binds to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BoundTo {
    /// A single table (evaluated on commits to that table).
    Table,
    /// A namespace (evaluated on commits to every table under it, resolved at
    /// evaluation time).
    Namespace,
}

impl BoundTo {
    /// The database/wire rendering (matches the 0018 CHECK constraint).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Namespace => "namespace",
        }
    }

    /// Parses the wire rendering back into the enum.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "table" => Some(Self::Table),
            "namespace" => Some(Self::Namespace),
            _ => None,
        }
    }
}

impl std::fmt::Display for BoundTo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ===========================================================================
// The spec (typed jsonb)
// ===========================================================================

/// The allowed shape of schema evolution under a schema contract.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AllowedEvolution {
    /// Only *adding* columns is allowed; changing or removing an existing
    /// field (by id) is a violation. Strictly stronger than `no_narrowing`.
    AdditiveOnly,
    /// Adding columns and *widening* types is fine; narrowing a type, making
    /// an optional column required, or dropping a column is a violation.
    #[default]
    NoNarrowing,
    /// Any schema change is a violation (the schema is frozen).
    None,
}

/// A cheap, synchronous predicate over the staged metadata. Evaluated without
/// touching data files (schema + snapshot summary only).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Predicate {
    /// Asserts the named column exists **and is `required`** in the staged
    /// schema — the only non-null signal available synchronously without
    /// scanning data (a required column cannot hold nulls, per the spec). This
    /// is a schema-level non-null guarantee, not a data-level null count.
    NonNull {
        /// The column that must be present and required.
        column: String,
    },
    /// The staged current snapshot's `total-records` must be at least `value`.
    /// Skipped (not failed) when the summary carries no `total-records`.
    RowCountMin {
        /// The inclusive lower bound.
        value: i64,
    },
    /// The staged current snapshot's `total-records` must be at most `value`.
    /// Skipped (not failed) when the summary carries no `total-records`.
    RowCountMax {
        /// The inclusive upper bound.
        value: i64,
    },
}

/// The schema-evolution half of a contract spec.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SchemaContract {
    /// The allowed shape of schema evolution.
    #[serde(default)]
    pub allowed_evolution: AllowedEvolution,
    /// Columns (by name) that may never be dropped or renamed, regardless of
    /// `allowed_evolution`.
    #[serde(default)]
    pub protected_columns: Vec<String>,
    /// Columns (by name) that must be present in the staged schema.
    #[serde(default)]
    pub required_columns: Vec<String>,
}

/// The full, typed contract spec (stored as jsonb).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ContractSpec {
    /// Schema-evolution rules. Absent means "no schema constraint".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaContract>,
    /// Cheap synchronous predicates over the staged metadata.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub predicates: Vec<Predicate>,
}

// ===========================================================================
// Violations (the evaluation result)
// ===========================================================================

/// One detected contract violation: a stable machine `kind` + a human
/// `detail`. Produced by [`ContractSpec::evaluate`]; stored on
/// `contract_violations` and emitted on the violation event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Violation {
    /// Stable machine token (e.g. `protected-column-dropped`).
    pub kind: String,
    /// Human-readable detail.
    pub detail: String,
}

impl Violation {
    fn new(kind: &str, detail: impl Into<String>) -> Self {
        Self {
            kind: kind.to_owned(),
            detail: detail.into(),
        }
    }
}

// ===========================================================================
// The pure evaluation engine
// ===========================================================================

/// Whether promoting `from` to `to` is a spec-allowed *widening* (as opposed
/// to a narrowing that would break readers). Follows the Iceberg type-promotion
/// rules: `int→long`, `float→double`, `decimal(P,S)→decimal(P',S)` with
/// `P' ≥ P` and equal scale, and `date→timestamp`/`timestamp_ns`.
///
/// Equal types are trivially "widening" (no change). Any other type change is
/// treated as narrowing.
#[must_use]
pub fn is_widening(from: &Type, to: &Type) -> bool {
    if from == to {
        return true;
    }
    let (Type::Primitive(from), Type::Primitive(to)) = (from, to) else {
        // Structural type changes (struct/list/map reshapes) are not
        // promotions; treat as narrowing (a reader built on the old shape can
        // break).
        return false;
    };
    match (from, to) {
        (PrimitiveType::Int, PrimitiveType::Long)
        | (PrimitiveType::Float, PrimitiveType::Double)
        | (PrimitiveType::Date, PrimitiveType::Timestamp | PrimitiveType::TimestampNs) => true,
        (
            PrimitiveType::Decimal {
                precision: p0,
                scale: s0,
            },
            PrimitiveType::Decimal {
                precision: p1,
                scale: s1,
            },
        ) => s0 == s1 && p1 >= p0,
        _ => false,
    }
}

/// Flattens a schema's *top-level* fields into `(id, field)` — evolution is
/// classified against top-level columns, which is where drop/narrow/require
/// contracts apply. (Nested reshapes inside a struct are treated as a type
/// change on the containing field by [`is_widening`].)
fn top_level_by_id(schema: &Schema) -> Vec<(i32, &StructField)> {
    schema.fields.iter().map(|f| (f.id, f)).collect()
}

/// Finds a top-level field by name in a schema.
fn field_by_name<'a>(schema: &'a Schema, name: &str) -> Option<&'a StructField> {
    schema.fields.iter().find(|f| f.name == name)
}

/// Classifies the evolution from `base` to `staged` under `allowed`, appending
/// any violations. Pure; the core of the circuit breaker's schema logic.
///
/// Evolution is classified by **field id** (stable across Iceberg evolution):
/// an id in base but not staged is a *drop*; an id in both with a different
/// type is a *type change* (widening allowed under `no_narrowing`, any change
/// rejected under `additive_only`); an id whose `required` went `false→true`
/// is a *nullability tighten* (narrowing); an id only in staged is an *add*.
pub fn classify_schema_evolution(
    base: &Schema,
    staged: &Schema,
    allowed: AllowedEvolution,
    out: &mut Vec<Violation>,
) {
    let base_fields = top_level_by_id(base);
    let staged_by_id: std::collections::BTreeMap<i32, &StructField> =
        staged.fields.iter().map(|f| (f.id, f)).collect();

    // `none`: any structural change at all is a violation. Compare the full
    // field lists (ignoring only the schema id, which the builder assigns).
    if matches!(allowed, AllowedEvolution::None) && base.fields != staged.fields {
        out.push(Violation::new(
            "schema-frozen",
            "the schema is frozen by contract; no schema change is allowed",
        ));
        // A frozen schema needs no finer classification.
        return;
    }

    for (id, base_field) in &base_fields {
        match staged_by_id.get(id) {
            None => {
                // Field id gone from staged: a dropped column.
                out.push(Violation::new(
                    "schema-narrowed",
                    format!("column {:?} (field id {id}) was dropped", base_field.name),
                ));
            }
            Some(staged_field) => {
                let type_changed = base_field.field_type != staged_field.field_type;
                let tightened = !base_field.required && staged_field.required;
                match allowed {
                    AllowedEvolution::AdditiveOnly => {
                        if type_changed
                            || tightened
                            || base_field.required != staged_field.required
                            || base_field.name != staged_field.name
                        {
                            out.push(Violation::new(
                                "additive-only-violated",
                                format!(
                                    "column {:?} (field id {id}) was modified; \
                                     the contract allows adding columns only",
                                    base_field.name
                                ),
                            ));
                        }
                    }
                    AllowedEvolution::NoNarrowing => {
                        if type_changed
                            && !is_widening(&base_field.field_type, &staged_field.field_type)
                        {
                            out.push(Violation::new(
                                "schema-narrowed",
                                format!(
                                    "column {:?} (field id {id}) type {} was narrowed to {}",
                                    base_field.name,
                                    render_type(&base_field.field_type),
                                    render_type(&staged_field.field_type),
                                ),
                            ));
                        }
                        if tightened {
                            out.push(Violation::new(
                                "schema-narrowed",
                                format!(
                                    "optional column {:?} (field id {id}) was made required",
                                    base_field.name
                                ),
                            ));
                        }
                    }
                    AllowedEvolution::None => {}
                }
            }
        }
    }
    // Added columns (ids only in staged) are always additive-safe under both
    // `additive_only` and `no_narrowing`; no classification needed.
}

/// Renders a type for a human-readable violation detail.
fn render_type(t: &Type) -> String {
    match t {
        Type::Primitive(p) => p.to_string(),
        Type::Struct(_) => "struct".to_owned(),
        Type::List(_) => "list".to_owned(),
        Type::Map(_) => "map".to_owned(),
    }
}

impl ContractSpec {
    /// Evaluates this spec against the base and staged schemas and the staged
    /// current-snapshot summary, returning every violation. Pure; no I/O.
    ///
    /// `base` is the current schema before the commit; `staged` is the schema
    /// the commit produces; `summary` is the staged current snapshot's summary
    /// map (`None` when the commit adds no snapshot — schema-only commits still
    /// evaluate the schema rules).
    #[must_use]
    pub fn evaluate(
        &self,
        base: &Schema,
        staged: &Schema,
        summary: Option<&std::collections::BTreeMap<String, String>>,
    ) -> Vec<Violation> {
        let mut out = Vec::new();

        if let Some(schema) = &self.schema {
            classify_schema_evolution(base, staged, schema.allowed_evolution, &mut out);

            // Protected columns: a protected name must still be carried by the
            // same field id it had in the base (drop OR rename trips it).
            for name in &schema.protected_columns {
                if let Some(base_field) = field_by_name(base, name) {
                    let still_there = staged
                        .fields
                        .iter()
                        .any(|f| f.id == base_field.id && f.name == *name);
                    if !still_there {
                        out.push(Violation::new(
                            "protected-column-dropped",
                            format!("column {name:?} is protected and was dropped or renamed"),
                        ));
                    }
                }
                // A protected name that never existed in the base is not a
                // violation (nothing to protect yet).
            }

            // Required columns: must be present (by name) in the staged schema.
            for name in &schema.required_columns {
                if field_by_name(staged, name).is_none() {
                    out.push(Violation::new(
                        "required-column-missing",
                        format!("required column {name:?} is absent from the schema"),
                    ));
                }
            }
        }

        for predicate in &self.predicates {
            match predicate {
                Predicate::NonNull { column } => match field_by_name(staged, column) {
                    None => out.push(Violation::new(
                        "predicate-non-null",
                        format!("non-null column {column:?} is absent from the schema"),
                    )),
                    Some(field) if !field.required => out.push(Violation::new(
                        "predicate-non-null",
                        format!("column {column:?} must be required (non-null) but is optional"),
                    )),
                    Some(_) => {}
                },
                Predicate::RowCountMin { value } => {
                    if let Some(total) = total_records(summary)
                        && total < *value
                    {
                        out.push(Violation::new(
                            "predicate-row-count",
                            format!("total-records {total} is below the contract minimum {value}"),
                        ));
                    }
                }
                Predicate::RowCountMax { value } => {
                    if let Some(total) = total_records(summary)
                        && total > *value
                    {
                        out.push(Violation::new(
                            "predicate-row-count",
                            format!("total-records {total} exceeds the contract maximum {value}"),
                        ));
                    }
                }
            }
        }

        out
    }
}

/// Reads `total-records` out of a snapshot summary, if present and parseable.
fn total_records(summary: Option<&std::collections::BTreeMap<String, String>>) -> Option<i64> {
    summary?.get("total-records")?.parse::<i64>().ok()
}

// ===========================================================================
// The persisted model
// ===========================================================================

/// A persisted contract (its current version).
#[derive(Debug, Clone)]
pub struct Contract {
    /// ULID of the contract.
    pub id: String,
    /// Owning workspace.
    pub workspace_id: String,
    /// Human name (unique per workspace).
    pub name: String,
    /// What the contract binds to.
    pub bound_to: BoundTo,
    /// The bound securable's id (table id or namespace id).
    pub securable_id: String,
    /// Current version (monotonic, starts at 1).
    pub version: i32,
    /// Whether the contract is in force (a disabled contract is retained and
    /// readable but skipped by the hook).
    pub enabled: bool,
    /// The circuit-breaker mode.
    pub mode: EnforcementMode,
    /// The typed spec.
    pub spec: ContractSpec,
    /// The Iceberg branch a quarantined commit is retargeted onto.
    pub quarantine_branch: String,
    /// Audit string of the creating principal.
    pub created_by: String,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

/// A contract row as read from Postgres.
#[derive(sqlx::FromRow)]
struct ContractRow {
    id: String,
    workspace_id: String,
    name: String,
    bound_to: String,
    securable_id: String,
    version: i32,
    enabled: bool,
    mode: String,
    spec: Value,
    quarantine_branch: String,
    created_by: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl TryFrom<ContractRow> for Contract {
    type Error = MeridianError;

    fn try_from(r: ContractRow) -> Result<Self> {
        Ok(Self {
            id: r.id,
            workspace_id: r.workspace_id,
            name: r.name,
            bound_to: BoundTo::parse(&r.bound_to).ok_or_else(|| {
                MeridianError::internal_msg(format!(
                    "contract row has unknown bound_to {:?}",
                    r.bound_to
                ))
            })?,
            securable_id: r.securable_id,
            version: r.version,
            enabled: r.enabled,
            mode: EnforcementMode::parse(&r.mode).ok_or_else(|| {
                MeridianError::internal_msg(format!("contract row has unknown mode {:?}", r.mode))
            })?,
            spec: serde_json::from_value(r.spec)
                .map_err(|e| MeridianError::internal("contract row has an unparseable spec", e))?,
            quarantine_branch: r.quarantine_branch,
            created_by: r.created_by,
            created_at: r.created_at,
            updated_at: r.updated_at,
        })
    }
}

const CONTRACT_COLUMNS: &str = "id, workspace_id, name, bound_to, securable_id, version, enabled, \
     mode, spec, quarantine_branch, created_by, created_at, updated_at";

/// One historical version of a contract.
#[derive(Debug, Clone)]
pub struct ContractVersion {
    /// The contract id.
    pub contract_id: String,
    /// The version number.
    pub version: i32,
    /// The mode at this version.
    pub mode: EnforcementMode,
    /// The enabled flag at this version.
    pub enabled: bool,
    /// The spec snapshot at this version.
    pub spec: ContractSpec,
    /// Audit string of the principal who created this version.
    pub created_by: String,
    /// When this version was created.
    pub created_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct VersionRow {
    contract_id: String,
    version: i32,
    mode: String,
    enabled: bool,
    spec: Value,
    created_by: String,
    created_at: DateTime<Utc>,
}

impl TryFrom<VersionRow> for ContractVersion {
    type Error = MeridianError;

    fn try_from(r: VersionRow) -> Result<Self> {
        Ok(Self {
            contract_id: r.contract_id,
            version: r.version,
            mode: EnforcementMode::parse(&r.mode).ok_or_else(|| {
                MeridianError::internal_msg(format!(
                    "contract version has unknown mode {:?}",
                    r.mode
                ))
            })?,
            enabled: r.enabled,
            spec: serde_json::from_value(r.spec).map_err(|e| {
                MeridianError::internal("contract version has an unparseable spec", e)
            })?,
            created_by: r.created_by,
            created_at: r.created_at,
        })
    }
}

const VERSION_COLUMNS: &str = "contract_id, version, mode, enabled, spec, created_by, created_at";

/// A persisted violation record.
#[derive(Debug, Clone)]
pub struct ContractViolation {
    /// ULID of the violation record.
    pub id: String,
    /// The violated contract.
    pub contract_id: String,
    /// The table the violating commit targeted.
    pub table_id: String,
    /// The head snapshot involved, when known.
    pub snapshot_id: Option<i64>,
    /// Stable machine token.
    pub kind: String,
    /// Human-readable detail.
    pub detail: String,
    /// Whether the commit was rejected (block mode).
    pub commit_rejected: bool,
    /// Whether the commit was quarantined onto the audit branch.
    pub quarantined: bool,
    /// When it occurred.
    pub occurred_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct ViolationRow {
    id: String,
    contract_id: String,
    table_id: String,
    snapshot_id: Option<i64>,
    kind: String,
    detail: String,
    commit_rejected: bool,
    quarantined: bool,
    occurred_at: DateTime<Utc>,
}

impl From<ViolationRow> for ContractViolation {
    fn from(r: ViolationRow) -> Self {
        Self {
            id: r.id,
            contract_id: r.contract_id,
            table_id: r.table_id,
            snapshot_id: r.snapshot_id,
            kind: r.kind,
            detail: r.detail,
            commit_rejected: r.commit_rejected,
            quarantined: r.quarantined,
            occurred_at: r.occurred_at,
        }
    }
}

const VIOLATION_COLUMNS: &str = "id, contract_id, table_id, snapshot_id, kind, detail, \
     commit_rejected, quarantined, occurred_at";

// ===========================================================================
// CRUD + versioning
// ===========================================================================

/// Fields required to create a contract.
#[derive(Debug, Clone)]
pub struct NewContract<'a> {
    /// Human name, unique per workspace.
    pub name: &'a str,
    /// What the contract binds to.
    pub bound_to: BoundTo,
    /// The bound securable's id.
    pub securable_id: &'a str,
    /// The circuit-breaker mode.
    pub mode: EnforcementMode,
    /// The typed spec.
    pub spec: &'a ContractSpec,
    /// The quarantine branch (defaults applied by the caller).
    pub quarantine_branch: &'a str,
}

/// Creates a contract at version 1 (and its first `contract_versions` row).
///
/// Returns [`MeridianError::Conflict`] if the name is taken in the workspace,
/// or [`MeridianError::Validation`] if the name is empty.
pub async fn create(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    new: NewContract<'_>,
    principal: &str,
) -> Result<Contract> {
    if new.name.trim().is_empty() {
        return Err(MeridianError::Validation(
            "contract name must be non-empty".to_owned(),
        ));
    }

    let spec_json = serde_json::to_value(new.spec)
        .map_err(|e| MeridianError::internal("failed to serialize contract spec", e))?;

    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin contract create", e))?;

    let id = Ulid::new().to_string();
    let row: ContractRow = sqlx::query_as(&format!(
        "INSERT INTO contracts
             (id, workspace_id, name, bound_to, securable_id, version, enabled, mode, spec,
              quarantine_branch, created_by)
         VALUES ($1, $2, $3, $4, $5, 1, TRUE, $6, $7, $8, $9)
         RETURNING {CONTRACT_COLUMNS}"
    ))
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(new.name)
    .bind(new.bound_to.as_str())
    .bind(new.securable_id)
    .bind(new.mode.as_str())
    .bind(&spec_json)
    .bind(new.quarantine_branch)
    .bind(principal)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| {
        if is_unique_violation(&e) {
            MeridianError::Conflict(format!("contract {:?} already exists", new.name))
        } else {
            map_sqlx_error("failed to insert contract", e)
        }
    })?;

    insert_version_row(&mut tx, &id, 1, new.mode, true, &spec_json, principal).await?;

    let details = json!({
        "name": new.name,
        "bound_to": new.bound_to.as_str(),
        "securable_id": new.securable_id,
        "mode": new.mode.as_str(),
        "version": 1,
    });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("contract:{id}"),
            event_type: "quality.contract.created".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "quality.contract.create".to_owned(),
            resource: format!("contract:{id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit contract create", e))?;

    Contract::try_from(row)
}

/// Inserts one `contract_versions` row on the caller's transaction.
async fn insert_version_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    contract_id: &str,
    version: i32,
    mode: EnforcementMode,
    enabled: bool,
    spec_json: &Value,
    principal: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO contract_versions
             (contract_id, version, mode, enabled, spec, created_by)
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(contract_id)
    .bind(version)
    .bind(mode.as_str())
    .bind(enabled)
    .bind(spec_json)
    .bind(principal)
    .execute(&mut **tx)
    .await
    .map_err(|e| map_sqlx_error("failed to insert contract version", e))?;
    Ok(())
}

/// Fields an update may change. `None` fields are left unchanged (but the
/// version is always bumped and a full snapshot recorded, so history is
/// complete). The binding never changes — a different binding is a different
/// contract.
#[derive(Debug, Clone, Default)]
pub struct ContractUpdate {
    /// New spec, if changing.
    pub spec: Option<ContractSpec>,
    /// New mode, if changing.
    pub mode: Option<EnforcementMode>,
    /// New enabled flag, if changing.
    pub enabled: Option<bool>,
    /// New quarantine branch, if changing.
    pub quarantine_branch: Option<String>,
}

/// Updates a contract: bumps the version, records the full new snapshot in
/// `contract_versions`, updates the denormalized current row — all on one
/// transaction. Returns the new [`Contract`], or [`MeridianError::NotFound`].
pub async fn update(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    change: ContractUpdate,
    principal: &str,
) -> Result<Contract> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin contract update", e))?;

    let current: Option<ContractRow> = sqlx::query_as(&format!(
        "SELECT {CONTRACT_COLUMNS} FROM contracts WHERE workspace_id = $1 AND id = $2 FOR UPDATE"
    ))
    .bind(workspace_id.to_string())
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to load contract for update", e))?;
    let Some(current) = current else {
        return Err(MeridianError::NotFound(format!(
            "contract {id:?} does not exist"
        )));
    };
    let current = Contract::try_from(current)?;

    let new_version = current.version + 1;
    let new_spec = change.spec.unwrap_or(current.spec);
    let new_mode = change.mode.unwrap_or(current.mode);
    let new_enabled = change.enabled.unwrap_or(current.enabled);
    let new_branch = change
        .quarantine_branch
        .unwrap_or(current.quarantine_branch);
    let spec_json = serde_json::to_value(&new_spec)
        .map_err(|e| MeridianError::internal("failed to serialize contract spec", e))?;

    let row: ContractRow = sqlx::query_as(&format!(
        "UPDATE contracts
         SET version = $3, spec = $4, mode = $5, enabled = $6, quarantine_branch = $7,
             updated_at = now()
         WHERE workspace_id = $1 AND id = $2
         RETURNING {CONTRACT_COLUMNS}"
    ))
    .bind(workspace_id.to_string())
    .bind(id)
    .bind(new_version)
    .bind(&spec_json)
    .bind(new_mode.as_str())
    .bind(new_enabled)
    .bind(&new_branch)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| map_sqlx_error("failed to update contract", e))?;

    insert_version_row(
        &mut tx,
        id,
        new_version,
        new_mode,
        new_enabled,
        &spec_json,
        principal,
    )
    .await?;

    let details = json!({
        "name": current.name,
        "version": new_version,
        "mode": new_mode.as_str(),
        "enabled": new_enabled,
    });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("contract:{id}"),
            event_type: "quality.contract.updated".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "quality.contract.update".to_owned(),
            resource: format!("contract:{id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit contract update", e))?;

    Contract::try_from(row)
}

/// Deletes a contract (its versions and violations cascade). Returns
/// [`MeridianError::NotFound`] if it does not exist.
pub async fn delete(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
    principal: &str,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin contract delete", e))?;

    let deleted = sqlx::query("DELETE FROM contracts WHERE workspace_id = $1 AND id = $2")
        .bind(workspace_id.to_string())
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(|e| map_sqlx_error("failed to delete contract", e))?;
    if deleted.rows_affected() == 0 {
        return Err(MeridianError::NotFound(format!(
            "contract {id:?} does not exist"
        )));
    }

    let details = json!({ "contract_id": id });
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("contract:{id}"),
            event_type: "quality.contract.deleted".to_owned(),
            payload: details.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "quality.contract.delete".to_owned(),
            resource: format!("contract:{id}"),
            details,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit contract delete", e))?;
    Ok(())
}

/// Gets one contract by id.
pub async fn get(pool: &PgPool, workspace_id: WorkspaceId, id: &str) -> Result<Option<Contract>> {
    let row: Option<ContractRow> = sqlx::query_as(&format!(
        "SELECT {CONTRACT_COLUMNS} FROM contracts WHERE workspace_id = $1 AND id = $2"
    ))
    .bind(workspace_id.to_string())
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to load contract", e))?;
    row.map(Contract::try_from).transpose()
}

/// Lists contracts in a workspace, newest first.
pub async fn list(pool: &PgPool, workspace_id: WorkspaceId) -> Result<Vec<Contract>> {
    let rows: Vec<ContractRow> = sqlx::query_as(&format!(
        "SELECT {CONTRACT_COLUMNS} FROM contracts WHERE workspace_id = $1 ORDER BY id DESC"
    ))
    .bind(workspace_id.to_string())
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list contracts", e))?;
    rows.into_iter().map(Contract::try_from).collect()
}

/// Lists a contract's version history, newest first.
pub async fn versions(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    id: &str,
) -> Result<Vec<ContractVersion>> {
    // Scope through the workspace via the parent contract so a version read
    // cannot cross workspaces.
    let exists: Option<String> =
        sqlx::query_scalar("SELECT id FROM contracts WHERE workspace_id = $1 AND id = $2")
            .bind(workspace_id.to_string())
            .bind(id)
            .fetch_optional(pool)
            .await
            .map_err(|e| map_sqlx_error("failed to check contract for versions", e))?;
    if exists.is_none() {
        return Err(MeridianError::NotFound(format!(
            "contract {id:?} does not exist"
        )));
    }
    let rows: Vec<VersionRow> = sqlx::query_as(&format!(
        "SELECT {VERSION_COLUMNS} FROM contract_versions WHERE contract_id = $1 \
         ORDER BY version DESC"
    ))
    .bind(id)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list contract versions", e))?;
    rows.into_iter().map(ContractVersion::try_from).collect()
}

// ===========================================================================
// Resolution (which contracts apply to a table)
// ===========================================================================

/// Resolves the **enabled** contracts that bind to a table: those bound
/// directly to the table id, plus those bound to any namespace in the table's
/// self-and-ancestors chain. `namespace_ids` is that chain (as the RBAC scope
/// builder already computes it).
///
/// Returns them ready to evaluate. Disabled contracts are excluded (they are
/// retained for reading but never enforced).
pub async fn resolve_for_table(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    table_id: &str,
    namespace_ids: &[String],
) -> Result<Vec<Contract>> {
    let rows: Vec<ContractRow> = sqlx::query_as(&format!(
        "SELECT {CONTRACT_COLUMNS} FROM contracts
         WHERE workspace_id = $1
           AND enabled = TRUE
           AND (
                (bound_to = 'table' AND securable_id = $2)
             OR (bound_to = 'namespace' AND securable_id = ANY($3))
           )
         ORDER BY id"
    ))
    .bind(workspace_id.to_string())
    .bind(table_id)
    .bind(namespace_ids)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to resolve contracts for table", e))?;
    rows.into_iter().map(Contract::try_from).collect()
}

// ===========================================================================
// Violation recording
// ===========================================================================

/// The outcome a violation record describes (fixes `commit_rejected` +
/// `quarantined` consistently and the event type).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViolationOutcome {
    /// Warn mode: the commit landed on `main`.
    Warned,
    /// Quarantine mode: the commit landed on the audit branch, `main` frozen.
    Quarantined,
    /// Block mode: the commit was rejected; nothing durable.
    Blocked,
}

impl ViolationOutcome {
    fn commit_rejected(self) -> bool {
        matches!(self, Self::Blocked)
    }
    fn quarantined(self) -> bool {
        matches!(self, Self::Quarantined)
    }
    fn event_type(self) -> &'static str {
        match self {
            Self::Warned => "quality.contract.violated",
            Self::Quarantined => "quality.contract.quarantined",
            Self::Blocked => "quality.contract.blocked",
        }
    }
}

/// Everything needed to write a violation record, independent of transaction.
#[derive(Debug, Clone)]
pub struct ViolationRecord<'a> {
    /// The violated contract.
    pub contract_id: &'a str,
    /// The contract name (for the event payload).
    pub contract_name: &'a str,
    /// The table the commit targeted.
    pub table_id: &'a str,
    /// The head snapshot involved, when known.
    pub snapshot_id: Option<i64>,
    /// The mode that produced this outcome.
    pub mode: EnforcementMode,
    /// The outcome.
    pub outcome: ViolationOutcome,
    /// The violations detected (one row is written per violation).
    pub violations: &'a [Violation],
}

/// The **owned** form of a violation record, so it can be carried into the
/// commit backend and written on the pointer-swap transaction (warn /
/// quarantine, atomic with the commit). [`ViolationRecord`] is the borrowed
/// form used by the block path, which owns its own transaction.
#[derive(Debug, Clone)]
pub struct OwnedViolationRecord {
    /// The violated contract.
    pub contract_id: String,
    /// The contract name (for the event payload).
    pub contract_name: String,
    /// The table the commit targeted.
    pub table_id: String,
    /// The head snapshot involved, when known.
    pub snapshot_id: Option<i64>,
    /// The mode that produced this outcome.
    pub mode: EnforcementMode,
    /// The outcome.
    pub outcome: ViolationOutcome,
    /// The violations detected (one row is written per violation).
    pub violations: Vec<Violation>,
}

impl OwnedViolationRecord {
    /// Borrows this owned record as a [`ViolationRecord`] for the shared
    /// row-insert + event helpers.
    #[must_use]
    pub fn as_ref(&self) -> ViolationRecord<'_> {
        ViolationRecord {
            contract_id: &self.contract_id,
            contract_name: &self.contract_name,
            table_id: &self.table_id,
            snapshot_id: self.snapshot_id,
            mode: self.mode,
            outcome: self.outcome,
            violations: &self.violations,
        }
    }
}

/// Writes violation rows + one aggregate event on the **caller's**
/// transaction. This is the warn / quarantine path: the record joins the same
/// transaction as the pointer swap, so it is durable if and only if the commit
/// is (commit-protocol I4 extended to the violation record).
///
/// One `contract_violations` row per violation; one outbox event carrying all
/// of them. No audit row is written here — the caller's commit transaction
/// already writes the `table.commit` audit row (I6); the event is the
/// violation's durable signal.
pub async fn record_violation_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workspace_id: WorkspaceId,
    record: &ViolationRecord<'_>,
) -> Result<()> {
    for violation in record.violations {
        insert_violation_row(tx, workspace_id, record, violation).await?;
    }
    outbox::enqueue(
        &mut **tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("table:{}", record.table_id),
            event_type: record.outcome.event_type().to_owned(),
            payload: violation_event_payload(record),
        },
    )
    .await?;
    Ok(())
}

/// Writes violation rows + event + audit row in a **dedicated** transaction.
/// This is the block path: the commit was rejected and never opened its own
/// transaction, so the record write is itself the mutation and carries its own
/// audit row + outbox event atomically (audit+outbox discipline preserved).
pub async fn record_violation(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    principal: &str,
    record: &ViolationRecord<'_>,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| map_sqlx_error("failed to begin violation record", e))?;

    for violation in record.violations {
        insert_violation_row(&mut tx, workspace_id, record, violation).await?;
    }

    let payload = violation_event_payload(record);
    outbox::enqueue(
        &mut *tx,
        &NewOutboxEvent {
            workspace_id: Some(workspace_id),
            aggregate: format!("table:{}", record.table_id),
            event_type: record.outcome.event_type().to_owned(),
            payload: payload.clone(),
        },
    )
    .await?;
    audit::append_in_tx(
        &mut tx,
        NewAuditEntry {
            workspace_id: Some(workspace_id),
            principal: principal.to_owned(),
            action: "quality.contract.violation".to_owned(),
            resource: format!("contract:{}", record.contract_id),
            details: payload,
        },
    )
    .await?;

    tx.commit()
        .await
        .map_err(|e| map_sqlx_error("failed to commit violation record", e))?;
    Ok(())
}

/// Inserts one `contract_violations` row on the caller's transaction.
async fn insert_violation_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workspace_id: WorkspaceId,
    record: &ViolationRecord<'_>,
    violation: &Violation,
) -> Result<()> {
    let id = Ulid::new().to_string();
    sqlx::query(
        "INSERT INTO contract_violations
             (id, workspace_id, contract_id, table_id, snapshot_id, kind, detail,
              commit_rejected, quarantined)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(&id)
    .bind(workspace_id.to_string())
    .bind(record.contract_id)
    .bind(record.table_id)
    .bind(record.snapshot_id)
    .bind(&violation.kind)
    .bind(&violation.detail)
    .bind(record.outcome.commit_rejected())
    .bind(record.outcome.quarantined())
    .execute(&mut **tx)
    .await
    .map_err(|e| map_sqlx_error("failed to insert contract violation", e))?;
    Ok(())
}

/// The event/audit payload for a violation record.
fn violation_event_payload(record: &ViolationRecord<'_>) -> Value {
    json!({
        "contract_id": record.contract_id,
        "contract_name": record.contract_name,
        "table_id": record.table_id,
        "snapshot_id": record.snapshot_id,
        "mode": record.mode.as_str(),
        "commit_rejected": record.outcome.commit_rejected(),
        "quarantined": record.outcome.quarantined(),
        "violations": record.violations,
    })
}

// ===========================================================================
// Violations query
// ===========================================================================

/// Filter for a violations query.
#[derive(Debug, Clone, Default)]
pub struct ViolationQuery<'a> {
    /// Restrict to one contract.
    pub contract_id: Option<&'a str>,
    /// Restrict to one table.
    pub table_id: Option<&'a str>,
}

/// Lists violation records for a workspace, newest first, optionally filtered
/// by contract and/or table. Bounded by `limit`.
pub async fn list_violations(
    pool: &PgPool,
    workspace_id: WorkspaceId,
    filter: &ViolationQuery<'_>,
    limit: i64,
) -> Result<Vec<ContractViolation>> {
    let rows: Vec<ViolationRow> = sqlx::query_as(&format!(
        "SELECT {VIOLATION_COLUMNS} FROM contract_violations
         WHERE workspace_id = $1
           AND ($2::text IS NULL OR contract_id = $2)
           AND ($3::text IS NULL OR table_id = $3)
         ORDER BY occurred_at DESC, id DESC
         LIMIT $4"
    ))
    .bind(workspace_id.to_string())
    .bind(filter.contract_id)
    .bind(filter.table_id)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| map_sqlx_error("failed to list contract violations", e))?;
    Ok(rows.into_iter().map(ContractViolation::from).collect())
}

// ===========================================================================
// Unit tests — the pure evaluation engine (no database)
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use meridian_iceberg::spec::{PrimitiveType, StructField, Type};

    fn field(id: i32, name: &str, required: bool, ty: PrimitiveType) -> StructField {
        if required {
            StructField::required(id, name, Type::Primitive(ty))
        } else {
            StructField::optional(id, name, Type::Primitive(ty))
        }
    }

    fn schema(fields: Vec<StructField>) -> Schema {
        Schema::new(fields).with_schema_id(0)
    }

    fn base_schema() -> Schema {
        schema(vec![
            field(1, "id", true, PrimitiveType::Long),
            field(2, "email", false, PrimitiveType::String),
            field(3, "amount", false, PrimitiveType::Int),
        ])
    }

    #[test]
    fn additive_change_passes_no_narrowing_and_additive_only() {
        let base = base_schema();
        let mut staged_fields = base.fields.clone();
        staged_fields.push(field(4, "added", false, PrimitiveType::String));
        let staged = schema(staged_fields);

        for allowed in [
            AllowedEvolution::NoNarrowing,
            AllowedEvolution::AdditiveOnly,
        ] {
            let mut out = Vec::new();
            classify_schema_evolution(&base, &staged, allowed, &mut out);
            assert!(
                out.is_empty(),
                "additive change must pass under {allowed:?}: {out:?}"
            );
        }
    }

    #[test]
    fn widening_passes_no_narrowing_but_fails_additive_only() {
        let base = base_schema();
        // amount int -> long is a widening.
        let staged = schema(vec![
            field(1, "id", true, PrimitiveType::Long),
            field(2, "email", false, PrimitiveType::String),
            field(3, "amount", false, PrimitiveType::Long),
        ]);

        let mut out = Vec::new();
        classify_schema_evolution(&base, &staged, AllowedEvolution::NoNarrowing, &mut out);
        assert!(out.is_empty(), "widening must pass no_narrowing: {out:?}");

        let mut out = Vec::new();
        classify_schema_evolution(&base, &staged, AllowedEvolution::AdditiveOnly, &mut out);
        assert_eq!(out.len(), 1, "widening must fail additive_only");
        assert_eq!(out[0].kind, "additive-only-violated");
    }

    #[test]
    fn narrowing_type_is_rejected() {
        let base = base_schema();
        // id long -> int is a narrowing.
        let staged = schema(vec![
            field(1, "id", true, PrimitiveType::Int),
            field(2, "email", false, PrimitiveType::String),
            field(3, "amount", false, PrimitiveType::Int),
        ]);
        let mut out = Vec::new();
        classify_schema_evolution(&base, &staged, AllowedEvolution::NoNarrowing, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].kind, "schema-narrowed");
    }

    #[test]
    fn dropping_a_column_is_narrowing() {
        let base = base_schema();
        let staged = schema(vec![
            field(1, "id", true, PrimitiveType::Long),
            field(3, "amount", false, PrimitiveType::Int),
        ]);
        let mut out = Vec::new();
        classify_schema_evolution(&base, &staged, AllowedEvolution::NoNarrowing, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].kind, "schema-narrowed");
        assert!(out[0].detail.contains("email"), "{:?}", out[0]);
    }

    #[test]
    fn tightening_nullability_is_narrowing() {
        let base = base_schema();
        // email optional -> required.
        let staged = schema(vec![
            field(1, "id", true, PrimitiveType::Long),
            field(2, "email", true, PrimitiveType::String),
            field(3, "amount", false, PrimitiveType::Int),
        ]);
        let mut out = Vec::new();
        classify_schema_evolution(&base, &staged, AllowedEvolution::NoNarrowing, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].kind, "schema-narrowed");
    }

    #[test]
    fn frozen_schema_rejects_any_change_including_additive() {
        let base = base_schema();
        let mut staged_fields = base.fields.clone();
        staged_fields.push(field(4, "added", false, PrimitiveType::String));
        let staged = schema(staged_fields);
        let mut out = Vec::new();
        classify_schema_evolution(&base, &staged, AllowedEvolution::None, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].kind, "schema-frozen");
    }

    #[test]
    fn frozen_schema_passes_identical_schema() {
        let base = base_schema();
        let staged = base_schema();
        let mut out = Vec::new();
        classify_schema_evolution(&base, &staged, AllowedEvolution::None, &mut out);
        assert!(out.is_empty(), "unchanged schema must pass frozen: {out:?}");
    }

    #[test]
    fn protected_column_drop_is_rejected() {
        let base = base_schema();
        // Drop email (protected).
        let staged = schema(vec![
            field(1, "id", true, PrimitiveType::Long),
            field(3, "amount", false, PrimitiveType::Int),
        ]);
        let spec = ContractSpec {
            schema: Some(SchemaContract {
                // Use frozen-free additive so only the protected rule bites?
                // No: dropping is also a narrowing. Assert the protected kind
                // is present regardless.
                allowed_evolution: AllowedEvolution::NoNarrowing,
                protected_columns: vec!["email".to_owned()],
                required_columns: vec![],
            }),
            predicates: vec![],
        };
        let out = spec.evaluate(&base, &staged, None);
        assert!(
            out.iter().any(|v| v.kind == "protected-column-dropped"),
            "expected a protected-column-dropped violation: {out:?}"
        );
    }

    #[test]
    fn protected_column_rename_is_rejected() {
        let base = base_schema();
        // Rename email (same id 2) -> contact.
        let staged = schema(vec![
            field(1, "id", true, PrimitiveType::Long),
            field(2, "contact", false, PrimitiveType::String),
            field(3, "amount", false, PrimitiveType::Int),
        ]);
        let spec = ContractSpec {
            schema: Some(SchemaContract {
                allowed_evolution: AllowedEvolution::NoNarrowing,
                protected_columns: vec!["email".to_owned()],
                required_columns: vec![],
            }),
            predicates: vec![],
        };
        let out = spec.evaluate(&base, &staged, None);
        assert!(
            out.iter().any(|v| v.kind == "protected-column-dropped"),
            "rename of a protected column must trip the protected rule: {out:?}"
        );
    }

    #[test]
    fn required_column_absent_is_rejected() {
        let base = base_schema();
        let staged = base_schema(); // no `region` column
        let spec = ContractSpec {
            schema: Some(SchemaContract {
                allowed_evolution: AllowedEvolution::NoNarrowing,
                protected_columns: vec![],
                required_columns: vec!["region".to_owned()],
            }),
            predicates: vec![],
        };
        let out = spec.evaluate(&base, &staged, None);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].kind, "required-column-missing");
    }

    #[test]
    fn non_null_predicate_checks_required_flag() {
        let base = base_schema();
        let staged = base_schema();
        // email is optional -> non-null predicate fails.
        let spec = ContractSpec {
            schema: None,
            predicates: vec![Predicate::NonNull {
                column: "email".to_owned(),
            }],
        };
        let out = spec.evaluate(&base, &staged, None);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].kind, "predicate-non-null");

        // id is required -> passes.
        let spec = ContractSpec {
            schema: None,
            predicates: vec![Predicate::NonNull {
                column: "id".to_owned(),
            }],
        };
        assert!(spec.evaluate(&base, &staged, None).is_empty());
    }

    #[test]
    fn row_count_predicate_uses_summary_and_skips_when_absent() {
        let base = base_schema();
        let staged = base_schema();
        let spec = ContractSpec {
            schema: None,
            predicates: vec![Predicate::RowCountMin { value: 100 }],
        };
        // No summary -> skipped (not failed).
        assert!(spec.evaluate(&base, &staged, None).is_empty());

        // total-records below the minimum -> violation.
        let mut summary = std::collections::BTreeMap::new();
        summary.insert("total-records".to_owned(), "42".to_owned());
        let out = spec.evaluate(&base, &staged, Some(&summary));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].kind, "predicate-row-count");

        // total-records above the minimum -> pass.
        summary.insert("total-records".to_owned(), "200".to_owned());
        assert!(spec.evaluate(&base, &staged, Some(&summary)).is_empty());
    }

    #[test]
    fn is_widening_rules() {
        let p = Type::Primitive;
        assert!(is_widening(&p(PrimitiveType::Int), &p(PrimitiveType::Long)));
        assert!(is_widening(
            &p(PrimitiveType::Float),
            &p(PrimitiveType::Double)
        ));
        assert!(is_widening(
            &p(PrimitiveType::Date),
            &p(PrimitiveType::Timestamp)
        ));
        assert!(is_widening(
            &p(PrimitiveType::Decimal {
                precision: 10,
                scale: 2
            }),
            &p(PrimitiveType::Decimal {
                precision: 12,
                scale: 2
            })
        ));
        // Narrowing decimal (precision down) is not widening.
        assert!(!is_widening(
            &p(PrimitiveType::Decimal {
                precision: 12,
                scale: 2
            }),
            &p(PrimitiveType::Decimal {
                precision: 10,
                scale: 2
            })
        ));
        // Scale change is not a widening.
        assert!(!is_widening(
            &p(PrimitiveType::Decimal {
                precision: 10,
                scale: 2
            }),
            &p(PrimitiveType::Decimal {
                precision: 10,
                scale: 3
            })
        ));
        // long -> int is narrowing.
        assert!(!is_widening(
            &p(PrimitiveType::Long),
            &p(PrimitiveType::Int)
        ));
        // string -> long is narrowing.
        assert!(!is_widening(
            &p(PrimitiveType::String),
            &p(PrimitiveType::Long)
        ));
        // Equal is trivially widening.
        assert!(is_widening(
            &p(PrimitiveType::String),
            &p(PrimitiveType::String)
        ));
    }

    #[test]
    fn empty_spec_produces_no_violations() {
        let base = base_schema();
        let staged = schema(vec![
            field(1, "id", true, PrimitiveType::Long),
            // drop everything else
        ]);
        let spec = ContractSpec::default();
        assert!(spec.evaluate(&base, &staged, None).is_empty());
    }
}
