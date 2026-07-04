# AI Asset Governance (Pillar I)

Meridian's object model does not stop at Iceberg tables. Pillar I extends the
catalog to govern the **full AI supply chain** — the files a model trained on,
the model itself, the exact table snapshots that produced it, the rights that
data carried, and the evidence a regulator or a data-subject-erasure request
demands. This document covers the data model, the reproducibility guarantee,
the provenance and EU-AI-Act reporting, and the GDPR deletion-evidence path,
along with the honest boundaries of what is and is not modelled.

Everything here is built from data the platform already holds; nothing invents
facts. Where a fact does not exist yet (e.g. which agents use a model), the
surface says so explicitly rather than fabricating it.

## Object model

### Generic assets (I-F1)

A **generic asset** is one row in `assets` of a single `kind`:

- **`fileset`** — a directory-scoped asset on object storage. It carries a
  `storage_prefix` (`s3://bucket/prefix`) and a `warehouse_id`. A fileset is the
  file-oriented analogue of a table: it participates in RBAC grants and it vends
  scoped credentials for exactly its prefix.
- **`model`** — a registry entry. Its `metadata` holds `version`,
  `artifacts_location`, `framework`, and whatever else the registrant records;
  its `tags` carry lightweight labels (`license:cc-by`, `stage:prod`).
- **`vector_dataset`** — a vector store as a generic asset (Lance today;
  Iceberg-native vectors when the format lands). Kind-specific shape lives in
  `metadata`.

The model is deliberately extensible: a new kind appends to the `assets.kind`
CHECK and the `AssetKind` enum, and needs no new tables. Kind-specific fields
live in the `metadata` jsonb owned by `meridian_store::assets`; a fileset's
`storage_prefix` is the one exception, promoted to a first-class column because
credential vending scopes to it.

Assets are **workspace-scoped**, not namespace-scoped. They are named uniquely
per `(workspace, kind, name)`.

#### Assets as a grant securable

An asset is a first-class RBAC securable. `SecurableType::Asset` was admitted
into the `grants` securable-type CHECK the same way `view` was in migration
0007 — a single `ALTER … CHECK` — so no parallel grant machinery exists. An
asset is a **standalone leaf**: unlike a table, it has no warehouse/namespace
ancestors, so only a grant on the asset itself (or the `admin` role) decides.
The leaf-native privileges (`READ`, `WRITE`, `COMMIT`, `DROP`) apply; no new
privilege was introduced (which would have needed a migration to the 0005
privilege CHECK). A grant selector for an asset uses
`{ "type": "asset", "asset": "<asset-id>" }`.

#### Fileset credential vending

A fileset reuses the **exact table-vend path** (`meridian-vending`,
`crate::routes::vending`). The server builds the same `Vendor` from the
fileset's warehouse `vending` storage option (STS or static), parses the
fileset's `storage_prefix` with the same `TableScope::from_s3_location`, and
vends credentials scoped to that prefix — nothing wider. Access follows RBAC on
the asset securable: a principal with `WRITE`/`COMMIT`/`DROP` on the fileset
gets read-write credentials; one with only `READ` gets read-only. Every vend
writes an audit row and outbox event against `asset:{id}` in one transaction
before the credentials leave the server — the audit row is the product, exactly
as for tables.

### Training runs (I-F2)

A **training run** is an immutable provenance record binding a model version to
the exact table snapshots that trained it:

```
POST /api/v2/training-runs
{ "model": "recommender", "model_version": "2",
  "inputs": [ { "table_ref": "wh.sales.orders",
                "table_id": "01…", "snapshot_id": 8823066017012345678 },
              { "table_ref": "external.crm.contacts", "snapshot_id": -12345 } ] }
```

- **Append-only.** `create_training_run` is the only writer. There is no update
  or delete path for a run or its inputs in the store or the API. A run is
  written once, in one transaction with its audit row and outbox event.
- **Exact snapshot pinning.** An Iceberg snapshot id is a signed 64-bit integer;
  it is stored verbatim (`BIGINT`). Iceberg time-travel against that id
  reproduces the training inputs — that is the reproducibility guarantee.
- **Native or external inputs.** `table_id` links to a Meridian table when the
  input is one (not a foreign key: a dropped table must not erase the provenance
  that it fed a model — ULIDs are never reused). `table_ref` is always recorded
  so the record reads standalone; a purely external dataset is pinned by ref
  alone.
- **Model link.** `model_asset_id` optionally links to a registered `model`
  asset; the literal `model` + `model_version` are always recorded so the run
  stands even if the asset is later dropped.

## Provenance reporting (I-F3)

`GET /api/v2/models/{model}/provenance[?version=]` assembles, from the immutable
training runs:

- the **data → run → model** chain: every run of the model, each with its pinned
  inputs;
- the **propagated governance tags**: for each native-table input source, the
  approved tags on that table (resolved through the D-F3 tag model,
  `tags::resolve_table_tags`) — this is how "this model saw `license:cc-by` +
  `pii:masked` data" is answered from catalog facts;
- **auto-drafted dataset cards**: one per distinct source, listing the pinned
  snapshots the model saw and the source's tags.

### EU AI Act GPAI summary

`GET /api/v2/models/{model}/ai-act-summary[?version=]` generates the GPAI
training-content summary deterministically from the same material: the training
data sources, the reproducibility statement (each source pinned to an exact
snapshot), and the rights/consent posture derived from the propagated
`license:` / `consent:` tags. It is a **first-class draft for a compliance
officer to review — not a legal opinion**, and it says so.

### License/consent propagation

License and consent live in the existing governance tag model (Pillar D). A tag
on an input source table propagates into a model's provenance report and AI Act
summary via the pinned-input → source-table join. No new tag mechanism is
introduced; the propagation is a read over the training-run records and the
tags the sources already carry.

## Retention & deletion propagation (I-F4)

A **deletion campaign** produces GDPR "right to be forgotten" evidence:

1. `POST /api/v2/deletion-campaigns` opens a campaign for an erasure subject
   (a data-subject id, a DSAR ticket). Status starts `open`.
2. `POST /api/v2/deletion-campaigns/{id}/snapshots` adds the affected
   `(table, snapshot_id[, branch])` rows and, in the same transaction,
   **freezes the model-exposure evidence**: for each affected snapshot, every
   training run that pinned that exact `(table, snapshot)` is copied into
   `deletion_campaign_model_exposure`. The evidence is frozen (not a live
   re-query) so it is a durable record that cannot shift as runs are added
   later. The campaign advances to `evidence_ready`.
3. `GET /api/v2/deletion-campaigns/{id}/evidence` returns the durable pack: the
   affected snapshots with their expiry status, and which model versions saw
   them.
4. `POST /api/v2/deletion-campaigns/{id}/expire` records that an affected
   snapshot is **physically gone**. When nothing is pending, the campaign
   closes.

### Physical expiry integration point

This surface **tracks** which snapshots must expire and **records** model
exposure; it does **not** delete data from object storage. Physical snapshot
expiry is the maintenance **`ExpireSnapshots`** job (migration 0012 / Pillar C).
The `/expire` endpoint is the explicit tie-in the job (or an operator confirming
manual expiry) calls once a snapshot is gone. Wiring the maintenance job to call
`/expire` automatically after it expires a snapshot that a campaign targets is a
follow-up; the tracking, the evidence, and the integration point are in place.

## Persistence

Migration `0023_ai_assets`:

- `assets` — generic assets (+ a `search_tsv` trigger and GIN index for the
  workspace-scoped asset search).
- `training_runs`, `training_run_inputs` — append-only training provenance.
- `deletion_campaigns`, `deletion_campaign_snapshots`,
  `deletion_campaign_model_exposure` — the GDPR evidence record.
- `ALTER TABLE grants` admits `'asset'` into the securable-type CHECK.

## Honest scope

- **"Model → agents using it" is not modelled.** The spec's provenance chain
  ends at "agents using it". There is no model→agent binding today (agents run
  governed SQL; they do not declare model usage), so the provenance report
  returns `agents_using: []` — an explicit empty section, never a fabricated
  edge. When agent→model usage is captured, it slots into this section.
- **Lineage edges are not minted with a misleading provenance.** The
  `meridian-lineage` `Provenance` enum has `commit` / `openlineage` / `query_log`
  variants; none honestly describes a declared training-run edge. Rather than
  stamp a wrong provenance, the per-model lineage is assembled directly from the
  immutable training-run records. A dedicated declared-provenance edge kind is a
  clean future addition.
- **The AI Act summary is a draft, not a legal artifact.** It is generated from
  catalog metadata for a human to review.
- **Generic-asset search is separate.** Assets carry their own workspace-scoped
  full-text index rather than joining the namespace-scoped table/view/namespace
  search UNION (migration 0010), whose visibility model is namespace-hierarchy
  based and does not fit a standalone-leaf securable.
- **Physical expiry is referenced, not re-implemented.** See the integration
  point above.
