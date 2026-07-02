//! Iceberg domain logic: the table-metadata model (v1-read/v2/v3), the
//! view-metadata model (format version 1), the REST update/requirement
//! vocabulary and the validating metadata builders, the commit-protocol
//! contract (see `docs/design/commit-protocol.md`; the store-backed
//! implementation lands in M1), and (in M2) scan planning.
//!
//! Design rule for this crate: **never destroy metadata we do not model.**
//! Every serde struct carries a flattened `extra` map that preserves unknown
//! fields byte-for-byte through a parse/serialize round trip, so acting as
//! the catalog of record for files written by other tools is safe even where
//! our typed model is incomplete.

pub mod commit;
pub mod planning;
pub mod spec;
