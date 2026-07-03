//! Catalog federation for Meridian: the **inbound mirror sync engine**
//! (spec Pillar B, B-F1 read federation).
//!
//! A *mirror* is an external catalog Meridian syncs FROM as read-only foreign
//! assets. The mirror *config* and the sprawl/registration surface live in
//! [`meridian_store::federation`] and the server's `routes::federation`
//! (Pillar B-F5, landed separately); this crate is the engine that a sync run
//! actually executes: connect to the source catalog, list its namespaces and
//! tables, load each table's metadata, and **materialize it as a foreign
//! (read-only) asset** in Meridian's native `tables` / `namespaces` tables so
//! that every read-side feature (search, health, later lineage) works on it
//! immediately (see [`meridian_store::foreign`] and ADR 008).
//!
//! # Layout
//!
//! - [`client`] — a minimal, read-only HTTP **IRC client** (`GET /v1/config`,
//!   list namespaces, list tables, `loadTable`) with `none` / static-bearer /
//!   OAuth2-client-credentials auth. This is deliberately small and speaks
//!   only the read subset a mirror needs; when Glue/HMS source types land they
//!   add sibling clients behind the same [`sync`] engine.
//! - [`sync`] — the **sync engine**: given a mirror config, walk the source,
//!   upsert foreign assets incrementally (skip tables whose
//!   `metadata_location` is unchanged), remove assets that vanished from the
//!   source, and record the run + counts.
//! - [`worker`] — the **background sync loop** (`meridian serve` spawns it,
//!   exactly like the maintenance/events workers) plus [`worker::sync_mirror_now`]
//!   for the manual "sync now" trigger.
//!
//! # Read-only guarantee
//!
//! Foreign assets never accept writes. This crate only ever *upserts* foreign
//! rows through [`meridian_store::foreign`], attributing them to a
//! `federation:sync:<mirror>` principal. The hard guarantee that a foreign
//! table cannot be committed to lives at the server's commit boundary
//! (`commit_table` / `commit_transaction` reject any table whose row carries a
//! `mirror_id`); this crate is the reason such rows exist, not the enforcer.

pub mod client;
pub mod sync;
pub mod worker;

pub use client::{IrcClient, IrcClientError, MirrorAuth};
pub use sync::{SyncEngineError, SyncStats, sync_mirror};
pub use worker::{run_worker, sync_mirror_now};
