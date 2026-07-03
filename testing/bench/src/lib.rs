//! Library side of `meridian-bench`: the synthetic-table fixture
//! generator, shared by the benchmark binary (`plan` scenario setup) and
//! by `meridian-server`'s planning integration/perf tests.
//!
//! The binary's HTTP harness lives in `src/main.rs` and stays
//! binary-private; only fixture generation is a library concern.

pub mod fixture;
