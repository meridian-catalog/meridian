# Contributing to Meridian

Thank you for your interest in Meridian.

## Project status

Meridian is **pre-1.0** and currently in **early private development**. APIs, schemas,
and internal interfaces may change without notice, and we are not yet accepting
external contributions. External contributions (issues, pull requests, and design
proposals) will open when the repository is made public. This document describes the
workflow we already follow internally and the one external contributors will be asked
to follow at that point.

## Development workflow

1. **Fork** the repository (or create a branch if you have write access).
2. Create a topic branch from `main`, named for the change, e.g.
   `fix/rest-catalog-etag-handling` or `feat/table-maintenance-scheduler`.
3. Keep pull requests **small and focused**. One logical change per PR. Large or
   cross-cutting changes should be split into a series of reviewable PRs, and
   significant design work should start with an ADR (see below) before code.
4. Make sure the test suite and lints pass locally before opening the PR.
5. Open the PR against `main` with a clear description of what changes and why.

## Commit conventions

We use [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<optional scope>): <short imperative summary>
```

Accepted types:

- `feat` — a new feature or user-visible capability
- `fix` — a bug fix
- `docs` — documentation-only changes
- `refactor` — code change that neither fixes a bug nor adds a feature
- `test` — adding or correcting tests
- `chore` — build, tooling, CI, or dependency housekeeping

Write the summary in the **imperative mood** ("add snapshot expiry policy", not
"added" or "adds"). Keep the summary under ~72 characters; use the body for detail.

## Developer Certificate of Origin (DCO)

All commits must be signed off:

```
git commit -s
```

The sign-off adds a `Signed-off-by: Your Name <email>` trailer certifying that you
wrote the change or otherwise have the right to submit it under the project's
Apache-2.0 license, as described in the [Developer Certificate of Origin](https://developercertificate.org/).
It is a legal attestation of provenance, not a cryptographic signature, and commits
without it will not be merged.

## Code standards

- **Rust** — code must be formatted with `rustfmt` (`cargo fmt --check`) and pass
  `cargo clippy` with no warnings.
- **TypeScript** (console) — formatted with `prettier`.
- **Python** (transpiler sidecar) — linted and formatted with `ruff`.
- **Tests are required for behavior changes.** Any PR that changes observable
  behavior must include or update tests that would fail without the change.
  Pure refactors should be covered by existing tests.

## Architecture Decision Records (ADRs)

Significant design decisions — for example a new dependency, a protocol extension,
a schema or commit-path change, or a change to the security model — require an ADR
before or alongside the implementation. See [docs/adr/README.md](docs/adr/README.md)
for when an ADR is required, the numbering scheme, and the template.

## Reporting bugs

Once the repository is public, report bugs through GitHub issues. Please include:

- what you did, what you expected, and what happened;
- Meridian version/commit, PostgreSQL version, and the query engine(s) involved;
- relevant logs or a minimal reproduction where possible.

## Reporting security issues

**Never report security vulnerabilities through public GitHub issues, discussions,
or pull requests.** Follow the process in [SECURITY.md](SECURITY.md) instead, which
uses GitHub private vulnerability reporting until a dedicated security contact is
published.

## License

Meridian is licensed under [Apache-2.0](LICENSE). By contributing, you agree that
your contributions are licensed under the same terms, as certified by your DCO
sign-off.
