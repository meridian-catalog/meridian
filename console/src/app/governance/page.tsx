"use client";

import { useState } from "react";
import { ShieldCheck, KeyRound, UserSearch } from "lucide-react";
import { api, ApiError } from "@/lib/api";
import { fmtTime } from "@/lib/utils";
import { PageHeader } from "@/components/page-header";
import { Async, useAsync } from "@/components/states";
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
import {
  PRIVILEGES,
  SECURABLE_TYPES,
  type CreateGrantRequest,
  type PermissionsResponse,
  type SecurableSelector,
} from "@/lib/types";

export default function GovernancePage() {
  const roles = useAsync(() => api.listRoles(), []);
  const grants = useAsync(() => api.listGrants(), []);
  const principals = useAsync(() => api.listPrincipals(), []);

  return (
    <div>
      <PageHeader
        title="Governance"
        description="Roles, grants, and effective permissions."
      />

      <div className="space-y-4">
        {/* Roles */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <ShieldCheck className="h-4 w-4 text-muted-foreground" /> Roles
            </CardTitle>
          </CardHeader>
          <CardContent>
            <Async state={roles} loadingLabel="Loading roles…">
              {(data) =>
                data.roles.length === 0 ? (
                  <p className="py-4 text-sm text-muted-foreground">
                    No roles defined.
                  </p>
                ) : (
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-muted-foreground">
                          <th className="py-2 pr-4 font-medium">Name</th>
                          <th className="py-2 pr-4 font-medium">Description</th>
                          <th className="py-2 pr-4 font-medium">Kind</th>
                          <th className="py-2 font-medium">Created</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-border">
                        {data.roles.map((r) => (
                          <tr key={r.id}>
                            <td className="py-2 pr-4 font-mono text-xs">
                              {r.name}
                            </td>
                            <td className="py-2 pr-4 text-muted-foreground">
                              {r.description ?? "—"}
                            </td>
                            <td className="py-2 pr-4">
                              <Badge
                                variant={r.built_in ? "secondary" : "outline"}
                              >
                                {r.built_in ? "built-in" : "custom"}
                              </Badge>
                            </td>
                            <td className="py-2 text-xs text-muted-foreground">
                              {fmtTime(r.created_at)}
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

        {/* Grants + create form */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <KeyRound className="h-4 w-4 text-muted-foreground" /> Grants
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-6">
            <CreateGrantForm
              roleNames={(roles.data?.roles ?? []).map((r) => r.name)}
              onCreated={() => grants.reload()}
            />
            <Async state={grants} loadingLabel="Loading grants…">
              {(data) =>
                data.grants.length === 0 ? (
                  <p className="py-4 text-sm text-muted-foreground">
                    No grants yet.
                  </p>
                ) : (
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-muted-foreground">
                          <th className="py-2 pr-4 font-medium">Grantee</th>
                          <th className="py-2 pr-4 font-medium">Privilege</th>
                          <th className="py-2 font-medium">Securable</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-border">
                        {data.grants.map((g) => (
                          <tr key={g.id}>
                            <td className="py-2 pr-4">
                              {g.role ? (
                                <Badge variant="default">role:{g.role}</Badge>
                              ) : (
                                <span className="font-mono text-xs">
                                  {g.principal_id}
                                </span>
                              )}
                            </td>
                            <td className="py-2 pr-4">
                              <Badge variant="outline">{g.privilege}</Badge>
                            </td>
                            <td className="py-2 font-mono text-xs text-muted-foreground">
                              {g.securable_type}:{g.securable_id}
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

        {/* Effective permissions lookup */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <UserSearch className="h-4 w-4 text-muted-foreground" /> Effective
              permissions
            </CardTitle>
          </CardHeader>
          <CardContent>
            <PermissionsLookup
              principals={(principals.data?.principals ?? []).map((p) => ({
                id: p.id,
                label: p.display_name ?? p.subject,
              }))}
            />
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function CreateGrantForm({
  roleNames,
  onCreated,
}: {
  roleNames: string[];
  onCreated: () => void;
}) {
  const toast = useToast();
  const [securableType, setSecurableType] =
    useState<SecurableSelector["type"]>("warehouse");
  const [warehouse, setWarehouse] = useState("");
  const [namespace, setNamespace] = useState("");
  const [target, setTarget] = useState(""); // table or view name
  const [privilege, setPrivilege] = useState<string>(PRIVILEGES[0]);
  const [granteeKind, setGranteeKind] = useState<"role" | "principal">("role");
  const [grantee, setGrantee] = useState("");
  const [submitting, setSubmitting] = useState(false);

  const needsNamespace = securableType !== "warehouse";
  const needsTarget = securableType === "table" || securableType === "view";

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    if (!warehouse.trim()) {
      toast.error("Warehouse required");
      return;
    }
    if (!grantee.trim()) {
      toast.error("Grantee required");
      return;
    }
    const nsLevels = namespace.trim()
      ? namespace.split(".").map((s) => s.trim()).filter(Boolean)
      : undefined;

    const securable: SecurableSelector = {
      type: securableType,
      warehouse: warehouse.trim(),
      ...(needsNamespace ? { namespace: nsLevels } : {}),
      ...(securableType === "table" ? { table: target.trim() } : {}),
      ...(securableType === "view" ? { view: target.trim() } : {}),
    };
    const body: CreateGrantRequest = {
      privilege,
      securable,
      ...(granteeKind === "role"
        ? { role: grantee.trim() }
        : { principal_id: grantee.trim() }),
    };

    setSubmitting(true);
    try {
      await api.createGrant(body);
      toast.success("Grant created", `${privilege} on ${securableType}`);
      setTarget("");
      onCreated();
    } catch (err) {
      const e2 = err instanceof Error ? err : new Error(String(err));
      toast.error("Grant failed", e2.message);
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <form
      onSubmit={submit}
      className="rounded-md border border-border bg-muted/20 p-4"
    >
      <p className="mb-3 text-sm font-medium">Create grant</p>
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
        <div>
          <Label className="mb-1 block text-xs">Securable type</Label>
          <Select
            value={securableType}
            onChange={(e) =>
              setSecurableType(e.target.value as SecurableSelector["type"])
            }
          >
            {SECURABLE_TYPES.map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </Select>
        </div>
        <div>
          <Label className="mb-1 block text-xs">Warehouse</Label>
          <Input
            value={warehouse}
            onChange={(e) => setWarehouse(e.target.value)}
            placeholder="warehouse name"
          />
        </div>
        {needsNamespace && (
          <div>
            <Label className="mb-1 block text-xs">Namespace (dotted)</Label>
            <Input
              value={namespace}
              onChange={(e) => setNamespace(e.target.value)}
              placeholder="analytics.reporting"
            />
          </div>
        )}
        {needsTarget && (
          <div>
            <Label className="mb-1 block text-xs">
              {securableType === "table" ? "Table" : "View"} name
            </Label>
            <Input
              value={target}
              onChange={(e) => setTarget(e.target.value)}
              placeholder={`${securableType} name`}
            />
          </div>
        )}
        <div>
          <Label className="mb-1 block text-xs">Privilege</Label>
          <Select
            value={privilege}
            onChange={(e) => setPrivilege(e.target.value)}
          >
            {PRIVILEGES.map((p) => (
              <option key={p} value={p}>
                {p}
              </option>
            ))}
          </Select>
        </div>
        <div>
          <Label className="mb-1 block text-xs">Grantee kind</Label>
          <Select
            value={granteeKind}
            onChange={(e) => {
              setGranteeKind(e.target.value as "role" | "principal");
              setGrantee("");
            }}
          >
            <option value="role">role</option>
            <option value="principal">principal</option>
          </Select>
        </div>
        <div>
          <Label className="mb-1 block text-xs">
            {granteeKind === "role" ? "Role" : "Principal id"}
          </Label>
          {granteeKind === "role" && roleNames.length > 0 ? (
            <Select
              value={grantee}
              onChange={(e) => setGrantee(e.target.value)}
            >
              <option value="">select a role…</option>
              {roleNames.map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </Select>
          ) : (
            <Input
              value={grantee}
              onChange={(e) => setGrantee(e.target.value)}
              placeholder={
                granteeKind === "role" ? "role name" : "principal ULID"
              }
            />
          )}
        </div>
      </div>
      <div className="mt-4 flex justify-end">
        <Button type="submit" disabled={submitting}>
          {submitting ? "Creating…" : "Create grant"}
        </Button>
      </div>
    </form>
  );
}

function PermissionsLookup({
  principals,
}: {
  principals: { id: string; label: string }[];
}) {
  const toast = useToast();
  const [principal, setPrincipal] = useState("");
  const [result, setResult] = useState<PermissionsResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<ApiError | Error | null>(null);

  async function lookup(e: React.FormEvent) {
    e.preventDefault();
    const id = principal.trim();
    if (!id) return;
    setLoading(true);
    setError(null);
    try {
      const res = await api.permissions(id);
      setResult(res);
    } catch (err) {
      const e2 = err instanceof Error ? err : new Error(String(err));
      setError(e2);
      setResult(null);
      toast.error("Lookup failed", e2.message);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div>
      <form onSubmit={lookup} className="flex flex-wrap gap-2">
        {principals.length > 0 ? (
          <Select
            value={principal}
            onChange={(e) => setPrincipal(e.target.value)}
            className="max-w-xs"
          >
            <option value="">select a principal…</option>
            {principals.map((p) => (
              <option key={p.id} value={p.id}>
                {p.label} ({p.id})
              </option>
            ))}
          </Select>
        ) : (
          <Input
            value={principal}
            onChange={(e) => setPrincipal(e.target.value)}
            placeholder="principal ULID"
            className="max-w-xs"
          />
        )}
        <Button type="submit" disabled={loading || !principal.trim()}>
          {loading ? "Looking up…" : "Look up"}
        </Button>
      </form>

      {error && (
        <p className="mt-3 text-sm text-destructive">{error.message}</p>
      )}

      {result && (
        <div className="mt-4 space-y-3">
          <div>
            <p className="text-xs uppercase tracking-wide text-muted-foreground">
              Roles
            </p>
            {result.roles.length === 0 ? (
              <p className="text-sm text-muted-foreground">No roles.</p>
            ) : (
              <div className="mt-1 flex flex-wrap gap-1">
                {result.roles.map((r) => (
                  <Badge key={r} variant="default">
                    {r}
                  </Badge>
                ))}
              </div>
            )}
          </div>
          <div>
            <p className="text-xs uppercase tracking-wide text-muted-foreground">
              Permissions
            </p>
            {result.permissions.length === 0 ? (
              <p className="text-sm text-muted-foreground">
                No explicit permission rows (built-in roles may still grant
                blanket access).
              </p>
            ) : (
              <div className="mt-1 overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-muted-foreground">
                      <th className="py-2 pr-4 font-medium">Privilege</th>
                      <th className="py-2 pr-4 font-medium">Securable</th>
                      <th className="py-2 font-medium">Via</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border">
                    {result.permissions.map((p, i) => (
                      <tr key={i}>
                        <td className="py-2 pr-4">
                          <Badge variant="outline">{p.privilege}</Badge>
                        </td>
                        <td className="py-2 pr-4 font-mono text-xs text-muted-foreground">
                          {p.securable_type}:{p.securable_id}
                        </td>
                        <td className="py-2 font-mono text-xs">{p.via}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
