# Architecture Decision Records

This directory holds Meridian's Architecture Decision Records (ADRs): short,
numbered documents that capture a significant design decision, the context in
which it was made, and its consequences. ADRs exist so that future contributors
can understand *why* the system is the way it is, not just *how* it works.

## When an ADR is required

Open an ADR (before or alongside the implementation PR) for any change that:

- **adds a new dependency** — a new crate, Python package, npm package, or
  external service that the core, sidecar, or console will depend on;
- **extends a protocol** — any addition or deviation beyond the standard
  Iceberg REST Catalog specification, or changes to Meridian's own public APIs
  (including the MCP gateway surface);
- **changes a schema or the commit path** — PostgreSQL schema changes,
  migrations, or anything that alters how table commits are validated,
  sequenced, or persisted;
- **changes the security model** — authentication, authorization, access
  governance semantics, token handling, or trust boundaries.

When in doubt, write one. A short ADR is cheap; rediscovering a lost rationale
is not.

## File naming

ADRs are named `NNN-short-title.md` with a zero-padded, monotonically
increasing number, e.g. `001-postgres-as-sole-required-dependency.md`.

Public ADRs start at **001**. **000 is reserved** for the founding product and
architecture specification, which is maintained privately until a public
revision of it is published.

Numbers are never reused. A superseded ADR keeps its file; its Status is
updated to `Superseded by NNN`.

## Template

Use this MADR-style template:

```markdown
# NNN. Short title in imperative or noun form

## Status

Proposed | Accepted | Deprecated | Superseded by NNN

## Context

What problem are we solving? What forces, constraints, and requirements are
in play? Include enough background that a reader without this conversation's
context can follow the reasoning.

## Decision

The decision, stated plainly ("We will ..."). Mention the main alternatives
that were considered and why they were not chosen.

## Consequences

What becomes easier, what becomes harder, and what follow-up work or risks
this creates — including positive, negative, and neutral outcomes.
```

## Process

1. Copy the template into `docs/adr/NNN-short-title.md` using the next free
   number.
2. Open a PR with Status `Proposed`; discussion happens on the PR.
3. On merge, set Status to `Accepted`. Later ADRs may deprecate or supersede
   it, but the record itself is immutable history — do not rewrite accepted
   ADRs, write a new one.
