"use client";

import { useState } from "react";
import { ShieldAlert, Tag as TagIcon, ScanSearch, AlertTriangle } from "lucide-react";
import { api, ApiError } from "@/lib/api";
import { fmtTime } from "@/lib/utils";
import { PageHeader } from "@/components/page-header";
import { Async, useAsync, type AsyncState } from "@/components/states";
import { useToast } from "@/components/toast";
import {
  Badge,
  Button,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  Input,
  Label,
  Select,
} from "@/components/ui/primitives";
import type {
  GovPolicyKind,
  ListGovPoliciesResponse,
  ListTagsResponse,
} from "@/lib/types";

// The Pillar D control plane: classification tags, versioned row-filter /
// column-mask / ABAC policies, their bindings, an effective-policy lookup, and
// drift alerts. Everything here is management-gated on the server; a non-admin
// sees 403s surfaced as toasts.
export default function PoliciesPage() {
  const tags = useAsync(() => api.govListTags(), []);
  const policies = useAsync(() => api.govListPolicies(), []);

  return (
    <div>
      <PageHeader
        title="Policies"
        description="Cross-engine access governance (Pillar D): tags, row/column/ABAC policies, and enforcement analytics."
      />

      <div className="space-y-4">
        <TagsCard tags={tags} />
        <PoliciesCard policies={policies} tagNames={tagOptions(tags)} />
        <EffectivePolicyCard />
        <DriftCard />
      </div>
    </div>
  );
}

function tagOptions(tags: AsyncState<ListTagsResponse>) {
  return (tags.data?.tags ?? []).map((t) => ({ id: t.id, label: t.rendered }));
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

function TagsCard({ tags }: { tags: AsyncState<ListTagsResponse> }) {
  const toast = useToast();
  const [key, setKey] = useState("");
  const [value, setValue] = useState("");
  const [busy, setBusy] = useState(false);

  async function create() {
    if (!key.trim() || !value.trim()) return;
    setBusy(true);
    try {
      await api.govCreateTag({ key: key.trim(), value: value.trim() });
      setKey("");
      setValue("");
      toast.success("Tag created");
      tags.reload();
    } catch (e) {
      toast.error(e instanceof ApiError ? e.message : "Failed to create tag");
    } finally {
      setBusy(false);
    }
  }

  async function remove(id: string) {
    try {
      await api.govDeleteTag(id);
      toast.success("Tag deleted");
      tags.reload();
    } catch (e) {
      toast.error(e instanceof ApiError ? e.message : "Failed to delete tag");
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <TagIcon className="h-4 w-4 text-muted-foreground" /> Classification tags
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-6">
        <div className="flex flex-wrap items-end gap-3">
          <div>
            <Label htmlFor="tag-key">Key</Label>
            <Input id="tag-key" placeholder="pii" value={key} onChange={(e) => setKey(e.target.value)} />
          </div>
          <div>
            <Label htmlFor="tag-value">Value</Label>
            <Input id="tag-value" placeholder="email" value={value} onChange={(e) => setValue(e.target.value)} />
          </div>
          <Button onClick={create} disabled={busy || !key.trim() || !value.trim()}>
            Create tag
          </Button>
        </div>

        <Async state={tags} loadingLabel="Loading tags…">
          {(data) =>
            data.tags.length === 0 ? (
              <p className="py-4 text-sm text-muted-foreground">No tags yet.</p>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-muted-foreground">
                      <th className="py-2 pr-4 font-medium">Tag</th>
                      <th className="py-2 pr-4 font-medium">Description</th>
                      <th className="py-2 pr-4 font-medium">Created</th>
                      <th className="py-2 font-medium" />
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border">
                    {data.tags.map((t) => (
                      <tr key={t.id}>
                        <td className="py-2 pr-4">
                          <Badge variant="secondary">{t.rendered}</Badge>
                        </td>
                        <td className="py-2 pr-4 text-muted-foreground">{t.description ?? "—"}</td>
                        <td className="py-2 pr-4 text-xs text-muted-foreground">{fmtTime(t.created_at)}</td>
                        <td className="py-2 text-right">
                          <Button variant="ghost" size="sm" onClick={() => remove(t.id)}>
                            Delete
                          </Button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )
          }
        </Async>
      </CardContent>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Policies (create with a kind-aware definition builder) + bind
// ---------------------------------------------------------------------------

const MASK_KINDS = ["null", "hash", "drop"] as const;

function PoliciesCard({
  policies,
  tagNames,
}: {
  policies: AsyncState<ListGovPoliciesResponse>;
  tagNames: { id: string; label: string }[];
}) {
  const toast = useToast();
  const [name, setName] = useState("");
  const [kind, setKind] = useState<GovPolicyKind>("column_mask");
  const [tag, setTag] = useState("");
  const [maskKind, setMaskKind] = useState<(typeof MASK_KINDS)[number]>("hash");
  const [column, setColumn] = useState("");
  const [value, setValue] = useState("");
  const [purpose, setPurpose] = useState("");
  const [busy, setBusy] = useState(false);

  // Builds the AbacRule the server expects, from the kind-specific fields.
  function buildDefinition(): unknown {
    if (kind === "column_mask") {
      return { type: "tag_column_mask", tag, exempt_groups: [], mask: { kind: maskKind } };
    }
    if (kind === "row_filter") {
      return {
        type: "tag_row_filter",
        tag,
        exempt_groups: [],
        predicate: { op: "eq", column, value },
      };
    }
    // abac: a deny-unless-purpose rule.
    return {
      type: "tag_deny_unless_purpose",
      tag,
      actions: ["read"],
      unless_purpose: purpose.trim() ? [purpose.trim()] : [],
    };
  }

  async function create() {
    if (!name.trim() || !tag.trim()) return;
    setBusy(true);
    try {
      await api.govCreatePolicy({ name: name.trim(), kind, definition: buildDefinition() });
      setName("");
      toast.success("Policy created");
      policies.reload();
    } catch (e) {
      toast.error(e instanceof ApiError ? e.message : "Failed to create policy");
    } finally {
      setBusy(false);
    }
  }

  async function bindToTag(id: string) {
    // Bind to the tag the policy already targets (the common case), resolved
    // by rendered label to its id.
    const match = tagNames.find((t) => t.label === tag);
    if (!match) {
      toast.error("Pick a tag that exists to bind by tag");
      return;
    }
    try {
      await api.govBindPolicy(id, { target_type: "tag", tag_id: match.id });
      toast.success("Policy bound to tag");
    } catch (e) {
      toast.error(e instanceof ApiError ? e.message : "Failed to bind");
    }
  }

  async function remove(id: string) {
    try {
      await api.govDeletePolicy(id);
      toast.success("Policy deleted");
      policies.reload();
    } catch (e) {
      toast.error(e instanceof ApiError ? e.message : "Failed to delete");
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <ShieldAlert className="h-4 w-4 text-muted-foreground" /> Policies
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-6">
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
          <div>
            <Label htmlFor="p-name">Name</Label>
            <Input id="p-name" placeholder="mask-email" value={name} onChange={(e) => setName(e.target.value)} />
          </div>
          <div>
            <Label htmlFor="p-kind">Kind</Label>
            <Select id="p-kind" value={kind} onChange={(e) => setKind(e.target.value as GovPolicyKind)}>
              <option value="column_mask">column mask</option>
              <option value="row_filter">row filter</option>
              <option value="abac">ABAC (deny unless purpose)</option>
            </Select>
          </div>
          <div>
            <Label htmlFor="p-tag">Tag (key:value)</Label>
            <Input id="p-tag" placeholder="pii:email" value={tag} onChange={(e) => setTag(e.target.value)} list="tag-list" />
            <datalist id="tag-list">
              {tagNames.map((t) => (
                <option key={t.id} value={t.label} />
              ))}
            </datalist>
          </div>

          {kind === "column_mask" && (
            <div>
              <Label htmlFor="p-mask">Mask</Label>
              <Select id="p-mask" value={maskKind} onChange={(e) => setMaskKind(e.target.value as (typeof MASK_KINDS)[number])}>
                {MASK_KINDS.map((m) => (
                  <option key={m} value={m}>
                    {m}
                  </option>
                ))}
              </Select>
            </div>
          )}
          {kind === "row_filter" && (
            <>
              <div>
                <Label htmlFor="p-col">Column</Label>
                <Input id="p-col" placeholder="region" value={column} onChange={(e) => setColumn(e.target.value)} />
              </div>
              <div>
                <Label htmlFor="p-val">Equals value</Label>
                <Input id="p-val" placeholder="eu" value={value} onChange={(e) => setValue(e.target.value)} />
              </div>
            </>
          )}
          {kind === "abac" && (
            <div>
              <Label htmlFor="p-purpose">Unless purpose</Label>
              <Input id="p-purpose" placeholder="fraud_investigation" value={purpose} onChange={(e) => setPurpose(e.target.value)} />
            </div>
          )}
        </div>
        <Button onClick={create} disabled={busy || !name.trim() || !tag.trim()}>
          Create policy
        </Button>

        <Async state={policies} loadingLabel="Loading policies…">
          {(data) =>
            data.policies.length === 0 ? (
              <p className="py-4 text-sm text-muted-foreground">No policies yet.</p>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-muted-foreground">
                      <th className="py-2 pr-4 font-medium">Name</th>
                      <th className="py-2 pr-4 font-medium">Kind</th>
                      <th className="py-2 pr-4 font-medium">Version</th>
                      <th className="py-2 pr-4 font-medium">Enabled</th>
                      <th className="py-2 font-medium" />
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border">
                    {data.policies.map((p) => (
                      <tr key={p.id}>
                        <td className="py-2 pr-4 font-mono text-xs">{p.name}</td>
                        <td className="py-2 pr-4">
                          <Badge variant="outline">{p.kind}</Badge>
                        </td>
                        <td className="py-2 pr-4 text-xs text-muted-foreground">v{p.version}</td>
                        <td className="py-2 pr-4">
                          <Badge variant={p.enabled ? "default" : "secondary"}>
                            {p.enabled ? "enabled" : "disabled"}
                          </Badge>
                        </td>
                        <td className="py-2 text-right">
                          <Button variant="ghost" size="sm" onClick={() => bindToTag(p.id)}>
                            Bind to tag
                          </Button>
                          <Button variant="ghost" size="sm" onClick={() => remove(p.id)}>
                            Delete
                          </Button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )
          }
        </Async>
      </CardContent>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Effective-policy lookup
// ---------------------------------------------------------------------------

function EffectivePolicyCard() {
  const toast = useToast();
  const [principal, setPrincipal] = useState("");
  const [warehouse, setWarehouse] = useState("");
  const [namespace, setNamespace] = useState("");
  const [table, setTable] = useState("");
  const [purpose, setPurpose] = useState("");
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<null | {
    denied: boolean;
    reason: string;
    masked_columns: string[];
    row_filter: unknown | null;
    applied_policies: string[];
  }>(null);

  async function lookup() {
    if (!principal.trim() || !warehouse.trim() || !namespace.trim() || !table.trim()) return;
    setBusy(true);
    try {
      const r = await api.govEffectivePolicy({
        principal: principal.trim(),
        warehouse: warehouse.trim(),
        namespace: namespace.trim(),
        table: table.trim(),
        purpose: purpose.trim() || undefined,
      });
      setResult(r);
    } catch (e) {
      toast.error(e instanceof ApiError ? e.message : "Lookup failed");
      setResult(null);
    } finally {
      setBusy(false);
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <ScanSearch className="h-4 w-4 text-muted-foreground" /> Effective policy
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-sm text-muted-foreground">
          What a principal actually sees on a table: the resolved row filter, masked columns, and
          the allow/deny decision — the auditor&apos;s answer.
        </p>
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
          <Input placeholder="user:alice@example.com" value={principal} onChange={(e) => setPrincipal(e.target.value)} />
          <Input placeholder="warehouse" value={warehouse} onChange={(e) => setWarehouse(e.target.value)} />
          <Input placeholder="namespace" value={namespace} onChange={(e) => setNamespace(e.target.value)} />
          <Input placeholder="table" value={table} onChange={(e) => setTable(e.target.value)} />
          <Input placeholder="purpose (optional)" value={purpose} onChange={(e) => setPurpose(e.target.value)} />
        </div>
        <Button onClick={lookup} disabled={busy}>
          Resolve
        </Button>

        {result && (
          <div className="rounded-md border border-border bg-muted/30 p-4 text-sm">
            <div className="flex items-center gap-2">
              <span className="font-medium">Decision:</span>
              <Badge variant={result.denied ? "secondary" : "default"}>
                {result.denied ? "DENIED" : "allowed"}
              </Badge>
            </div>
            <p className="mt-1 text-muted-foreground">{result.reason}</p>
            <p className="mt-2">
              <span className="font-medium">Masked columns:</span>{" "}
              {result.masked_columns.length ? result.masked_columns.join(", ") : "none"}
            </p>
            <p className="mt-1">
              <span className="font-medium">Row filter:</span>{" "}
              {result.row_filter ? (
                <code className="font-mono text-xs">{JSON.stringify(result.row_filter)}</code>
              ) : (
                "none"
              )}
            </p>
            <p className="mt-1 text-xs text-muted-foreground">
              Applied policies: {result.applied_policies.length ? result.applied_policies.join(", ") : "none"}
            </p>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Drift
// ---------------------------------------------------------------------------

function DriftCard() {
  const toast = useToast();
  const [warehouse, setWarehouse] = useState("");
  const [busy, setBusy] = useState(false);
  const [alerts, setAlerts] = useState<null | { table_id: string; column: string; tag: string }[]>(null);

  async function scan() {
    if (!warehouse.trim()) return;
    setBusy(true);
    try {
      const r = await api.govDrift(warehouse.trim());
      setAlerts(r.alerts);
    } catch (e) {
      toast.error(e instanceof ApiError ? e.message : "Drift scan failed");
      setAlerts(null);
    } finally {
      setBusy(false);
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <AlertTriangle className="h-4 w-4 text-muted-foreground" /> Policy drift
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-sm text-muted-foreground">
          Classified-but-unmasked columns: a column tagged <span className="font-mono">pii*</span> with no
          column-mask policy bound to its tag — an auditor would flag it.
        </p>
        <div className="flex items-end gap-3">
          <Input placeholder="warehouse" value={warehouse} onChange={(e) => setWarehouse(e.target.value)} />
          <Button onClick={scan} disabled={busy}>
            Scan
          </Button>
        </div>

        {alerts !== null &&
          (alerts.length === 0 ? (
            <p className="text-sm text-muted-foreground">No drift — every classified column is covered.</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-muted-foreground">
                    <th className="py-2 pr-4 font-medium">Table</th>
                    <th className="py-2 pr-4 font-medium">Column</th>
                    <th className="py-2 font-medium">Tag</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border">
                  {alerts.map((a, i) => (
                    <tr key={`${a.table_id}-${a.column}-${i}`}>
                      <td className="py-2 pr-4 font-mono text-xs">{a.table_id}</td>
                      <td className="py-2 pr-4">{a.column}</td>
                      <td className="py-2">
                        <Badge variant="secondary">{a.tag}</Badge>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ))}
      </CardContent>
    </Card>
  );
}
