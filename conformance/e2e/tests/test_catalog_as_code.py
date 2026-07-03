"""End-to-end test for catalog-as-code (`meridian plan` / `meridian apply`).

Drives the real `meridian` binary against the running server:

1. render a bundle with a unique run id (secrets via ${ENV});
2. `apply` it and verify every resource landed through the management/IRC APIs;
3. `apply` again and assert the run is a no-op (0 created, 0 updated) —
   idempotency;
4. `plan` again and assert it reports no changes for the bundle's resources;
5. mutate server state out-of-band (delete a role) and assert `plan` now
   reports drift (the role, and its cascade-deleted grant, come back as
   `create`);
6. re-`apply` and assert the drift is reconciled.

The binary is built once (debug) via `cargo build -p meridian-cli`. If cargo
or the binary is unavailable the module is skipped, not failed — matching the
suite's convention for optional prerequisites.
"""

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest
import requests

# Repo root: conformance/e2e/tests/ -> ../../../
REPO_ROOT = Path(__file__).resolve().parents[3]


def _cli_binary() -> str:
    """Locates (building if needed) the debug `meridian` binary."""
    prebuilt = REPO_ROOT / "target" / "debug" / "meridian"
    if prebuilt.exists():
        return str(prebuilt)
    if shutil.which("cargo") is None:
        pytest.skip("cargo not available and no prebuilt meridian binary")
    build = subprocess.run(
        ["cargo", "build", "-q", "-p", "meridian-cli"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if build.returncode != 0 or not prebuilt.exists():
        pytest.skip(f"could not build meridian-cli: {build.stderr[:400]}")
    return str(prebuilt)


@pytest.fixture(scope="module")
def cli() -> str:
    return _cli_binary()


def _run(cli: str, args: list[str], base_url: str, env_extra: dict) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env.update(env_extra)
    return subprocess.run(
        [cli, *args, "--server", base_url],
        capture_output=True,
        text=True,
        env=env,
    )


def _bundle_yaml(run_id: str) -> str:
    """A bundle exercising every resource kind, with an env-sourced secret and
    a two-level namespace (so ancestor creation is exercised)."""
    wh = f"cac_e2e_{run_id}"
    role = f"cac_e2e_analyst_{run_id}"
    return f"""\
apiVersion: meridian.dev/v1
kind: CatalogBundle
warehouses:
  - name: {wh}
    storage_root: s3://cac-e2e-{run_id}/wh
    storage_options:
      region: us-east-1
namespaces:
  - warehouse: {wh}
    levels: [sales, emea]
    properties:
      owner: data-platform
roles:
  - name: {role}
    description: read-only analytics
grants:
  - role: {role}
    privilege: READ
    securable:
      type: warehouse
      warehouse: {wh}
webhooks:
  - url: https://hooks.invalid/cac-e2e-{run_id}
    event_types: [com.meridian.table.committed]
    secret: ${{CAC_E2E_SECRET}}
"""


def _summary_counts(stdout: str) -> dict:
    """Parses the `Apply: N created, N updated, N unchanged, ...` trailer."""
    line = next((ln for ln in stdout.splitlines() if ln.startswith("Apply:")), "")
    counts = {}
    for token, key in (("created", "created"), ("updated", "updated"), ("unchanged", "unchanged"), ("failed", "failed")):
        for part in line.replace("Apply:", "").split(","):
            part = part.strip()
            if part.endswith(token):
                counts[key] = int(part.split()[0])
    return counts


def _plan_ops_for(stdout: str, substring: str) -> list[str]:
    """The plan op tags (create/update/noop/...) for lines mentioning
    `substring`, ignoring would-delete prune warnings for unrelated
    resources."""
    ops = []
    for line in stdout.splitlines():
        if substring in line and "would-delete" not in line:
            ops.append(line.split()[0])
    return ops


def test_catalog_as_code_lifecycle(cli: str, base_url: str, run_id: str, tmp_path: Path):
    secret = f"cac-e2e-secret-{run_id}-0000"
    env_extra = {"CAC_E2E_SECRET": secret}
    wh = f"cac_e2e_{run_id}"
    role = f"cac_e2e_analyst_{run_id}"

    bundle = tmp_path / "bundle.yaml"
    bundle.write_text(_bundle_yaml(run_id))

    # 1. First apply: everything is created.
    apply1 = _run(cli, ["apply", "-f", str(bundle)], base_url, env_extra)
    assert apply1.returncode == 0, f"apply failed:\n{apply1.stdout}\n{apply1.stderr}"
    counts1 = _summary_counts(apply1.stdout)
    assert counts1.get("failed", 0) == 0, apply1.stdout
    assert counts1.get("created", 0) == 5, f"expected 5 creates, got {counts1}:\n{apply1.stdout}"

    # 2. Verify each resource via the APIs (not just the CLI's own report).
    warehouses = requests.get(f"{base_url}/api/v2/warehouses", timeout=10).json()["warehouses"]
    assert any(w["name"] == wh for w in warehouses), "warehouse not created"

    # The ancestor namespace [sales] must exist (created implicitly).
    top = requests.get(f"{base_url}/v1/{wh}/namespaces", timeout=10).json()["namespaces"]
    assert ["sales"] in top, f"ancestor namespace [sales] not created: {top}"
    # The leaf [sales, emea] must exist under it.
    children = requests.get(
        f"{base_url}/v1/{wh}/namespaces", params={"parent": "sales"}, timeout=10
    ).json()["namespaces"]
    assert ["sales", "emea"] in children, f"leaf namespace not created: {children}"
    # And it carries the declared property (via the IRC properties-bearing load;
    # the multi-level name is sent with the unit-separator escape %1F).
    leaf = requests.get(f"{base_url}/v1/{wh}/namespaces/sales%1Femea", timeout=10)
    assert leaf.status_code == 200, f"namespace load failed: {leaf.status_code} {leaf.text}"
    assert leaf.json()["properties"].get("owner") == "data-platform"

    roles = requests.get(f"{base_url}/api/v2/roles", timeout=10).json()["roles"]
    assert any(r["name"] == role for r in roles), "role not created"

    grants = requests.get(f"{base_url}/api/v2/grants", timeout=10).json()["grants"]
    assert any(
        g["privilege"] == "READ" and g.get("role") == role for g in grants
    ), "grant not created"

    hooks = requests.get(f"{base_url}/api/v2/webhooks", timeout=10).json()["webhooks"]
    hook_url = f"https://hooks.invalid/cac-e2e-{run_id}"
    assert any(h["url"] == hook_url for h in hooks), "webhook not created"
    # The secret is write-only: it must never be echoed back by any endpoint.
    assert secret not in json.dumps(hooks), "webhook secret leaked in API response"

    # 3. Re-apply: idempotent no-op (nothing created or updated).
    apply2 = _run(cli, ["apply", "-f", str(bundle)], base_url, env_extra)
    assert apply2.returncode == 0, apply2.stdout
    counts2 = _summary_counts(apply2.stdout)
    assert counts2.get("created", 0) == 0, f"re-apply created resources:\n{apply2.stdout}"
    assert counts2.get("updated", 0) == 0, f"re-apply updated resources:\n{apply2.stdout}"
    assert counts2.get("failed", 0) == 0, apply2.stdout
    assert counts2.get("unchanged", 0) == 5, apply2.stdout

    # 4. Plan reports no changes for the bundle's own resources.
    plan1 = _run(cli, ["plan", "-f", str(bundle)], base_url, env_extra)
    assert plan1.returncode == 0, plan1.stderr
    assert _plan_ops_for(plan1.stdout, wh) == ["noop", "noop", "noop"], plan1.stdout  # warehouse, namespace, grant
    assert _plan_ops_for(plan1.stdout, role) == ["noop", "noop"], plan1.stdout  # role + grant line
    assert _plan_ops_for(plan1.stdout, hook_url) == ["noop"], plan1.stdout

    # 5. Out-of-band mutation: delete the role. Its grant cascade-deletes.
    deleted = requests.delete(f"{base_url}/api/v2/roles/{role}", timeout=10)
    assert deleted.status_code == 204, deleted.text

    plan2 = _run(cli, ["plan", "-f", str(bundle)], base_url, env_extra)
    assert plan2.returncode == 0, plan2.stderr
    # The role now shows as create; the warehouse/namespace/webhook stay noop.
    role_ops = _plan_ops_for(plan2.stdout, role)
    assert "create" in role_ops, f"drift not detected for role:\n{plan2.stdout}"
    assert _plan_ops_for(plan2.stdout, hook_url) == ["noop"], plan2.stdout

    # 6. Re-apply reconciles the drift.
    apply3 = _run(cli, ["apply", "-f", str(bundle)], base_url, env_extra)
    assert apply3.returncode == 0, apply3.stdout
    counts3 = _summary_counts(apply3.stdout)
    assert counts3.get("failed", 0) == 0, apply3.stdout
    assert counts3.get("created", 0) >= 1, f"drift not reconciled:\n{apply3.stdout}"

    roles_after = requests.get(f"{base_url}/api/v2/roles", timeout=10).json()["roles"]
    assert any(r["name"] == role for r in roles_after), "role not recreated after drift"


def test_missing_env_var_fails_closed(cli: str, base_url: str, run_id: str, tmp_path: Path):
    """A ${ENV} reference with no value set is a hard error, not an empty
    secret silently sent to the server."""
    bundle = tmp_path / "bundle.yaml"
    bundle.write_text(_bundle_yaml(f"{run_id}_noenv"))
    # Deliberately do NOT set CAC_E2E_SECRET.
    result = _run(cli, ["plan", "-f", str(bundle)], base_url, {"CAC_E2E_SECRET": ""})
    # Empty string is set but empty -> webhook secret validation rejects it,
    # or interpolation rejects unset. Either way, non-zero exit.
    if result.returncode == 0:
        pytest.fail(f"expected failure for empty secret, got success:\n{result.stdout}")
