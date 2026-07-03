# ---------------------------------------------------------------------------
# Build stage
# ---------------------------------------------------------------------------
FROM rust:1.96-bookworm AS builder

WORKDIR /build

# Build the whole workspace context. Migrations are embedded into the binary
# at compile time (sqlx::migrate!), so the store crate's migrations directory
# must be present.
# TODO(M1): layer-cache dependencies (e.g. cargo-chef) once CI build times
# justify the extra Dockerfile complexity.
COPY Cargo.toml Cargo.lock rustfmt.toml ./
COPY crates ./crates
# Workspace members outside crates/: cargo needs every member manifest
# present even when building only meridian-cli.
COPY testing/bench ./testing/bench

RUN cargo build --release --locked -p meridian-cli

# ---------------------------------------------------------------------------
# Runtime stage
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system meridian \
    && useradd --system --gid meridian --home-dir /nonexistent --shell /usr/sbin/nologin meridian

COPY --from=builder /build/target/release/meridian /usr/local/bin/meridian

USER meridian:meridian

EXPOSE 8181

ENTRYPOINT ["meridian"]
CMD ["serve"]
