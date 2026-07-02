# 004. Object-storage IO through Apache OpenDAL

## Status

Accepted — 2026-07-02

## Context

Meridian materializes Iceberg metadata (`metadata.json`, and later manifests
and job artifacts) in the *customer's* object storage. That storage is a
client relationship, not a service dependency: Postgres remains the only
required runtime dependency (ADR 002), and the storage layer must never grow
into an operational component of its own.

The commit protocol (`docs/design/commit-protocol.md`) puts one hard
requirement on this layer: **staged metadata files are written once and never
overwritten.** A commit attempt stages `metadata/NNNNN-<uuid>.metadata.json`
under a unique name; the safety argument in §3/§7 of the design leans on
object PUTs being atomic and on a conditional "create only if absent" write
so that no code path — including a buggy or duplicated one — can replace a
published metadata file in place. On S3 this maps to `PUT` with
`If-None-Match: *` (natively supported by AWS S3 since 2024, and by MinIO,
R2, and friends); on a local filesystem it maps to `open(..., O_CREAT|O_EXCL)`.

M1 needs two backends: local filesystem (`file://` — dev, tests, the
single-binary story) and S3-compatible stores (`s3://` — AWS plus
MinIO-style endpoint overrides with path-style addressing). GCS and Azure
are on the horizon but not in scope yet.

The needs are narrow — read, unconditional write, conditional write, exists,
delete, batched prefix delete, and a recursive listing with size/mtime — so
the realistic options were:

1. **Apache OpenDAL** (`opendal` 0.57): one API over many backends, with
   first-class `write_with(...).if_not_exists(true)` on both fs (O_EXCL) and
   S3 (`If-None-Match: *`), a built-in retry layer, and batched recursive
   deletes.
2. **Hand-rolled fs + official `aws-sdk-s3`**: maximum control, two
   implementations of every operation, and the conditional-write, retry, and
   pagination plumbing written and maintained by us twice.
3. **`object_store`** (the Arrow/DataFusion crate): good fit too, but its
   conditional-write ("put opts") and credential-chain coverage is narrower
   than OpenDAL's, and OpenDAL's per-service capability model maps more
   directly onto the backends we plan to add (GCS, Azure, HDFS).

## Decision

**We use Apache OpenDAL as the storage IO backend**, wrapped behind a
Meridian-owned seam in the new `meridian-storage` crate. Nothing outside
that crate sees an OpenDAL type.

- `StorageProfile` parses a warehouse root URI (`s3://bucket/prefix`,
  `file:///path`, bare or relative fs paths) plus a string options map —
  region, endpoint, path-style, anonymous, explicit credentials, retry
  tuning. Unknown option keys are rejected, not ignored: a typo in a
  durability-relevant option must fail loudly. Credentials fall back to the
  standard AWS environment/config chain when not given explicitly, and are
  redacted from `Debug` output.
- `Storage` is an async, object-safe trait: `read`, `write`,
  `write_if_absent`, `exists`, `delete`, `delete_prefix` (batched),
  streaming recursive `list`, all addressed by absolute location URIs (the
  form Iceberg metadata records) or root-relative paths. Locations that do
  not resolve under the profile root — including `..` traversal — are
  rejected; a handle can never touch storage outside its warehouse.
- Errors are mapped once, at the seam, into semantic variants the commit
  path branches on: `NotFound`, `AlreadyExists`, `PermissionDenied`,
  `Transient { retryable }`, plus configuration/location/metadata errors.
  OpenDAL's `ConditionNotMatch` maps to `AlreadyExists` because the only
  conditional operation we issue is `write_if_absent`.
- Transient failures are retried inside the handle by OpenDAL's `RetryLayer`
  — bounded exponential backoff with jitter (defaults: 3 retries, 100 ms
  first delay, 10 s ceiling; tunable per profile via `retry.*` options).
- Metadata-file helpers pin the conventions: `new_metadata_location`
  produces `<table>/metadata/<version, zero-padded to 5>-<uuid>.metadata.json`;
  `write_table_metadata` always goes through `write_if_absent`;
  `read_table_metadata` parses via `meridian_iceberg::spec::TableMetadata`
  (unknown fields preserved) and checks the declared format version.

Why not option 2: for exactly two backends it was defensible, but we would
own retry/jitter policy, S3 pagination, batched deletes, and the
conditional-write matrix ourselves, and every future backend doubles that
surface. Why not option 3: capability coverage, above. The escape hatch is
real either way — the `Storage` trait is the contract, its conformance
suite (`crates/meridian-storage/tests/storage_backends.rs`) runs identically
against every backend, and swapping OpenDAL out later is invisible to
callers.

One deliberate constraint: **OpenDAL is compiled with exactly the
`services-fs`, `services-s3`, and `layers-retry` features.** New backends
arrive by widening this list consciously (with an ADR update), not by
inheriting a default feature set.

## Consequences

- **Easier:** adding GCS/Azure later is a feature flag plus profile parsing
  plus running the existing conformance suite — no new IO code. Fault
  injection for the chaos suite can be layered at the seam without touching
  callers.
- **Easier:** `metadata.json` immutability is enforced by a storage
  primitive, not by discipline: there is no overwrite API on the metadata
  helpers at all.
- **Harder / risk:** OpenDAL 0.x moves quickly (0.57 split the crate into
  per-service crates); upgrades may churn the wrapper internals. The seam
  confines that churn to one crate, and the conformance suite is the
  upgrade gate.
- **Risk accepted:** conditional writes require backend support
  (`If-None-Match: *`). AWS S3, MinIO, and R2 support it; for a hypothetical
  S3-compatible store that does not, `write_if_absent` fails with a clear
  backend error rather than degrading to check-then-write, which would
  silently reintroduce the race the commit protocol excludes. Supporting
  such stores would need an explicit, opt-in fallback and its own ADR.
- **Neutral:** the crate treats object storage strictly as a client library;
  no daemon, cache, or background process comes with it, keeping the
  "Postgres is the only required dependency" invariant intact.
