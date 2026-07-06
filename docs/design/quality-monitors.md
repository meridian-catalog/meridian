# Zero-scan monitors, incidents & the trust score

Status: implemented (Pillar E — E-F1, E-F5, E-F6). Companion to
[contracts-circuit-breaker.md](contracts-circuit-breaker.md): contracts are
*prevention* at commit time; this is *detection and operability* after it.

## The claim: zero data scans

Every monitor here is a pure function of metadata Meridian already indexes — it
never opens a data file. An Iceberg commit records, in its snapshot summary, how
many records / files / bytes the table now holds and how many the commit added,
and the commit timestamp records *when*. Freshness, volume, file-size,
snapshot-debt, and (with a two-metadata-JSON read) schema-change anomalies are
all functions of those numbers across recent commits. What genuinely needs a
scan — per-column null rates, value distributions — is **not** claimed here;
that is E-F2's executor work, and this module does not pretend to it. This
honesty is the whole point: the catalog already sees every write, so detection
is free.

The data the scorers read comes from two places already maintained by the
platform:

- **`table_snapshots`** — the write-through snapshot index (migration 0003),
  populated in the commit transaction. Each row carries the summary jsonb
  (`added-records`, `total-records`, `added-data-files`, `added-files-size`,
  `total-delete-files`, `operation`) and the commit timestamp.
- **`metrics_reports`** — raw metrics the engines POST; available for future
  signals.

## Off the sacred commit path

The commit path is sacred (spec §12.1): it must not take on monitor work
synchronously. So monitors are evaluated by a **post-commit worker**
(`meridian_server::quality_monitor`) that consumes the durable `table.committed`
outbox stream — the identical crash-safe pattern the lineage worker uses:

1. read the next batch of published `table.committed` events after a durable
   cursor (`event_consumers`, gap-free + totally ordered);
2. resolve the committed table, build a zero-scan `CommitObservation` (the new
   snapshot's numbers) + a baseline `History` (the recent prior commits,
   summarized) from `table_snapshots`;
3. evaluate every enabled monitor bound to the table (directly or via its
   namespace chain), record a `monitor_results` row, and on a breach open (or
   re-touch) an incident;
4. advance the cursor only after the batch is processed.

Processing is at-least-once; re-evaluation is idempotent at the incident level
(de-duplication, below), and a duplicate result row is a harmless redundant data
point. A per-event error is logged and skipped — a poisoned event never wedges
the stream, and the worker never blocks or fails a commit.

The worker also consumes the `quality.contract.violated|quarantined|blocked`
events the circuit breaker emits and opens **contract-sourced** incidents from
them, so the incident ledger is one pane of glass over both prevention and
detection.

## The anomaly scorers (pure)

`meridian_store::monitors` owns the scorers as pure functions, exhaustively
unit-tested without a database:

| Monitor | Signal | Breach |
| --- | --- | --- |
| `freshness` | gap since last commit | `> multiple ×` learned median inter-commit interval, or `>` a declared `max_staleness_secs` SLA |
| `volume` | commit `added-records` | `≥ factor ×` or `≤ median / factor` of the recent median (spike or collapse) |
| `schema_change` | schema evolution | breaking (drop / narrow / tighten, via the contract diff) always; additive only when configured |
| `file_size` | commit avg file bytes | `≤ median / factor` (small-file regression) |
| `snapshot_debt` | retained snapshots / delete files | either `≥ factor ×` its recent median |
| `commit_failure` | blocked-commit events in a window | `≥ threshold` |

The `schema_change` monitor detects a change only when the diffed snapshot pair
straddles it — i.e. the change is carried by (or is adjacent to) a data commit,
the normal engine case. A breaking change applied as a *metadata-only* commit
with no snapshot, followed later by an unrelated data commit, can slip past the
monitor because both compared metadata versions already carry the new schema.
This is a *detection* edge case only; the *prevention* path is unaffected — a
`block`/`warn` contract fires on the metadata-only schema commit itself.

Two properties keep this trustworthy:

- **A learning grace period.** With fewer than `MIN_HISTORY` prior commits the
  baseline is not trustworthy, so the scorer returns `ok` with a "learning"
  detail rather than a false positive. A brand-new table is quiet until it has a
  cadence.
- **Baseline floors.** A near-empty baseline (e.g. the first real load after an
  empty table) is treated as expected growth, not an anomaly — ratios against a
  ~0 baseline are meaningless and are suppressed.

Defaults are deliberately loud (5× volume, 3× freshness, 4× file-size) because a
real pipeline's per-commit shape is stable within a small factor; a config
(`MonitorConfig`, typed jsonb) overrides any threshold per monitor.

## Incidents

`meridian_store::incidents` owns the ledger. An incident has a lifecycle
(`open` → `acknowledged` → `resolved`), a severity (from the monitor, or mapped
from the contract mode: block = high, quarantine = medium, warn = low), the
**owner** captured at open time from the table's `owner` property (never
inferred — an unowned table opens an unowned incident, honestly), and the
**downstream blast radius** (a JSON array of affected assets + their owners) from
`meridian_lineage::impact::impact_of` at open time.

### De-duplication

A flapping table must not open thousands of incidents. Every incident carries a
stable `dedup_key = {source}:{table_id}:{kind}`. A partial unique index —
`incidents_live_dedup_idx ON (workspace_id, dedup_key) WHERE status <>
'resolved'` — enforces **at most one live incident per condition**. Opening is a
single insert-or-touch statement: on the partial-unique conflict it bumps
`occurrence_count` + `last_seen_at` instead of duplicating. Once the incident is
resolved it leaves the index, so the same condition recurring later opens a
genuinely new incident. This is race-safe: two workers racing the same condition
serialize on the index, and the loser's insert becomes the touch.

Only a *new* incident emits the `quality.incident.opened` event (and audits) — a
re-touch is deliberately quiet, so notifications do not storm.

### Status roll-up

`table_status` reduces a table's live incidents to a traffic light: red if any
live high-severity incident, yellow if any live incident, else green.
`table_status_history` reconstructs the open/resolve timeline from the ledger.

## The trust score (E-F6)

`meridian_store::quality_score` computes a composite `0..=100` score as a **pure**
weighted combination of five `[0,1]` component sub-scores, mirroring the
maintenance health score's discipline so it is deterministic and explainable
(the API returns the components alongside the number):

| Component | Weight | Sub-score |
| --- | --- | --- |
| monitors | 30 | 1.0 monitored + no live incidents; decays with live incidents; 0.4 unmonitored |
| contract | 25 | block 1.0 / quarantine 0.85 / warn 0.6 / none 0.0 |
| ownership | 15 | 1.0 if an `owner` property is set |
| docs | 15 | table comment (0.5) + column-doc proxy (0.5) |
| freshness | 15 | 0.0 if a live staleness incident, else 1.0 |

The inputs are all index/property reads — no scan — so the score is cheap enough
to compute on demand and to fold onto search results (a bounded per-result read;
each table hit carries a `quality_score`). It is the number agents will read to
decide whether to trust a table.

## Invariant preservation

The worker is a **read-side consumer**: it adds no pointer-mutation path and does
not touch the commit transaction, the CAS, lock order, idempotency, or
multi-table atomicity. The monitor-result write and the incident it may open
share their *own* transaction, disjoint from any commit. The existing commit
property suite passes unchanged.
