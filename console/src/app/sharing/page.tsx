"use client";

// Sharing (Pillar J): cross-org data shares (J-F1) and the internal data
// marketplace (J-F2). Real data — every panel talks to the /api/v2/shares and
// /api/v2/marketplace endpoints. The recipient-facing IRC endpoint itself is
// token-authenticated and lives outside the console (an external org connects
// its own engine to /share/{token}/v1); this page is the workspace-side control
// plane: create/grant/revoke shares, browse certified products, and run the
// request-access flow.

import { useState } from "react";
import { Boxes, ShieldCheck, Store, Trash2, Ban } from "lucide-react";
import { api } from "@/lib/api";
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
} from "@/components/ui/primitives";
import type { Share, ShareDetail, DataProduct, AccessRequest } from "@/lib/types";

export default function SharingPage() {
  return (
    <div>
      <PageHeader
        title="Sharing"
        description="Cross-org data shares and the internal data marketplace. A share is a scoped, read-only projection of assets to an external recipient — served over a per-share Iceberg REST endpoint with vended read-only credentials. The marketplace is the certified-product gallery with a request-access flow."
      />
      <div className="space-y-4">
        <SharesCard />
        <MarketplaceCard />
        <RequestsCard />
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Cross-org shares (J-F1)
// ---------------------------------------------------------------------------

function SharesCard() {
  const toast = useToast();
  const shares = useAsync(() => api.listShares(), []);
  const [name, setName] = useState("");
  const [recipient, setRecipient] = useState("");
  const [terms, setTerms] = useState("");
  const [busy, setBusy] = useState(false);
  const [selected, setSelected] = useState<string | null>(null);
  const [newToken, setNewToken] = useState<string | null>(null);

  async function create() {
    if (!name.trim() || !recipient.trim()) {
      toast.error("Missing fields", "A share needs a name and a recipient.");
      return;
    }
    setBusy(true);
    try {
      const created = await api.createShare({
        name: name.trim(),
        recipient: recipient.trim(),
        terms: terms.trim() || undefined,
      });
      setNewToken(created.token ?? null);
      setName("");
      setRecipient("");
      setTerms("");
      shares.reload();
      toast.success("Share created", "Copy the token now — it is shown once.");
    } catch (err) {
      toast.error("Create failed", (err as Error).message);
    } finally {
      setBusy(false);
    }
  }

  async function revoke(id: string) {
    try {
      await api.revokeShare(id);
      shares.reload();
      toast.success("Share revoked", "The recipient is denied immediately.");
    } catch (err) {
      toast.error("Revoke failed", (err as Error).message);
    }
  }

  async function remove(id: string) {
    try {
      await api.deleteShare(id);
      if (selected === id) setSelected(null);
      shares.reload();
      toast.success("Share deleted");
    } catch (err) {
      toast.error("Delete failed", (err as Error).message);
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <ShieldCheck className="h-4 w-4 text-muted-foreground" /> Cross-org
          shares
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-sm text-muted-foreground">
          A share serves only its granted assets, read-only, over{" "}
          <span className="font-mono">/share/&#123;token&#125;/v1</span>. Revocation
          is instant — the recipient only ever holds short-lived vended
          credentials.
        </p>

        <div className="grid gap-2 sm:grid-cols-3">
          <div>
            <Label htmlFor="share-name">Name</Label>
            <Input
              id="share-name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="acme-q3-share"
            />
          </div>
          <div>
            <Label htmlFor="share-recipient">Recipient</Label>
            <Input
              id="share-recipient"
              value={recipient}
              onChange={(e) => setRecipient(e.target.value)}
              placeholder="org:acme"
            />
          </div>
          <div>
            <Label htmlFor="share-terms">Terms (optional)</Label>
            <Input
              id="share-terms"
              value={terms}
              onChange={(e) => setTerms(e.target.value)}
              placeholder="Read-only. No redistribution."
            />
          </div>
        </div>
        <Button onClick={create} disabled={busy}>
          Create share
        </Button>

        {newToken && (
          <div className="rounded-md border border-border bg-muted/40 p-3 text-sm">
            <p className="font-medium">Share token (shown once)</p>
            <p className="mt-1 break-all font-mono text-xs">{newToken}</p>
            <p className="mt-1 text-muted-foreground">
              Deliver it to the recipient over a secure channel.
            </p>
          </div>
        )}

        <Async state={shares} loadingLabel="Loading shares…">
          {(data) =>
            data.shares.length === 0 ? (
              <p className="text-sm text-muted-foreground">No shares yet.</p>
            ) : (
              <div className="space-y-1">
                {data.shares.map((s) => (
                  <ShareRow
                    key={s.id}
                    share={s}
                    expanded={selected === s.id}
                    onToggle={() =>
                      setSelected(selected === s.id ? null : s.id)
                    }
                    onRevoke={() => revoke(s.id)}
                    onDelete={() => remove(s.id)}
                    onChanged={shares.reload}
                  />
                ))}
              </div>
            )
          }
        </Async>
      </CardContent>
    </Card>
  );
}

function ShareRow({
  share,
  expanded,
  onToggle,
  onRevoke,
  onDelete,
  onChanged,
}: {
  share: Share;
  expanded: boolean;
  onToggle: () => void;
  onRevoke: () => void;
  onDelete: () => void;
  onChanged: () => void;
}) {
  return (
    <div className="rounded-md border border-border">
      <div className="flex items-center justify-between px-3 py-2">
        <button className="flex items-center gap-2 text-left" onClick={onToggle}>
          <span className="font-medium">{share.name}</span>
          <span className="text-sm text-muted-foreground">
            → {share.recipient}
          </span>
          {share.revoked ? (
            <Badge variant="danger">revoked</Badge>
          ) : (
            <Badge variant="success">active</Badge>
          )}
          {share.has_terms && (
            <Badge variant={share.terms_accepted ? "secondary" : "warning"}>
              {share.terms_accepted ? "terms accepted" : "terms pending"}
            </Badge>
          )}
        </button>
        <div className="flex items-center gap-1">
          {!share.revoked && (
            <Button variant="ghost" size="sm" onClick={onRevoke}>
              <Ban className="h-4 w-4" /> Revoke
            </Button>
          )}
          <Button variant="ghost" size="sm" onClick={onDelete}>
            <Trash2 className="h-4 w-4" />
          </Button>
        </div>
      </div>
      {expanded && <ShareGrants shareId={share.id} onChanged={onChanged} />}
    </div>
  );
}

function ShareGrants({
  shareId,
  onChanged,
}: {
  shareId: string;
  onChanged: () => void;
}) {
  const toast = useToast();
  const detail = useAsync<ShareDetail>(() => api.getShare(shareId), [shareId]);
  const [ref, setRef] = useState("");
  const [rowFilter, setRowFilter] = useState("");
  const [mask, setMask] = useState("");
  const [busy, setBusy] = useState(false);

  async function addGrant() {
    if (!ref.trim()) {
      toast.error("Missing reference", "Enter a table:<id> reference.");
      return;
    }
    setBusy(true);
    try {
      await api.addShareGrant(shareId, {
        securable_kind: "table",
        securable_ref: ref.trim(),
        row_filter: rowFilter.trim() || undefined,
        column_mask: mask.trim()
          ? mask.split(",").map((c) => c.trim()).filter(Boolean)
          : undefined,
      });
      setRef("");
      setRowFilter("");
      setMask("");
      detail.reload();
      onChanged();
      toast.success("Grant added");
    } catch (err) {
      toast.error("Add grant failed", (err as Error).message);
    } finally {
      setBusy(false);
    }
  }

  async function removeGrant(grantId: string) {
    try {
      await api.removeShareGrant(grantId);
      detail.reload();
      onChanged();
    } catch (err) {
      toast.error("Remove failed", (err as Error).message);
    }
  }

  return (
    <div className="border-t border-border bg-muted/20 px-3 py-3">
      <Async state={detail} loadingLabel="Loading grants…">
        {(data) => (
          <div className="space-y-2">
            {data.grants.length === 0 ? (
              <p className="text-sm text-muted-foreground">
                No grants — this share serves nothing yet.
              </p>
            ) : (
              data.grants.map((g) => (
                <div
                  key={g.id}
                  className="flex items-center justify-between text-sm"
                >
                  <span className="font-mono">
                    {g.securable_kind} {g.securable_ref}
                    {g.row_filter ? ` · filter[${g.row_filter}]` : ""}
                    {g.column_mask && g.column_mask.length > 0
                      ? ` · mask[${g.column_mask.join(", ")}]`
                      : ""}
                  </span>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => removeGrant(g.id)}
                  >
                    <Trash2 className="h-4 w-4" />
                  </Button>
                </div>
              ))
            )}
            <div className="grid gap-2 sm:grid-cols-3">
              <Input
                value={ref}
                onChange={(e) => setRef(e.target.value)}
                placeholder="table:<id>"
              />
              <Input
                value={rowFilter}
                onChange={(e) => setRowFilter(e.target.value)}
                placeholder="row filter (advisory)"
              />
              <Input
                value={mask}
                onChange={(e) => setMask(e.target.value)}
                placeholder="masked cols (comma-sep)"
              />
            </div>
            <Button size="sm" onClick={addGrant} disabled={busy}>
              Add grant
            </Button>
          </div>
        )}
      </Async>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Internal marketplace (J-F2)
// ---------------------------------------------------------------------------

function MarketplaceCard() {
  const products = useAsync(() => api.marketplaceProducts(), []);

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Store className="h-4 w-4 text-muted-foreground" /> Data marketplace
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <p className="text-sm text-muted-foreground">
          The certified-data-product gallery — the shopping catalog for internal
          consumers. Certified products are listed first. External/public
          marketplace and clean-room compute are out of scope.
        </p>
        <Async state={products} loadingLabel="Loading products…">
          {(data) =>
            data.products.length === 0 ? (
              <p className="text-sm text-muted-foreground">
                No data products yet.
              </p>
            ) : (
              <div className="space-y-1">
                {data.products.map((p) => (
                  <ProductRow key={p.id} product={p} />
                ))}
              </div>
            )
          }
        </Async>
      </CardContent>
    </Card>
  );
}

function ProductRow({ product }: { product: DataProduct }) {
  const toast = useToast();
  const [open, setOpen] = useState(false);
  const [ref, setRef] = useState("");
  const [purpose, setPurpose] = useState("");
  const [busy, setBusy] = useState(false);

  async function request() {
    if (!ref.trim() || !purpose.trim()) {
      toast.error("Missing fields", "Enter a table reference and a purpose.");
      return;
    }
    setBusy(true);
    try {
      await api.requestAccess({
        securable_type: "table",
        securable_id: ref.trim(),
        privilege: "READ",
        purpose: purpose.trim(),
      });
      setRef("");
      setPurpose("");
      setOpen(false);
      toast.success("Access requested", "An approver will decide.");
    } catch (err) {
      toast.error("Request failed", (err as Error).message);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="rounded-md border border-border px-3 py-2">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Boxes className="h-4 w-4 text-muted-foreground" />
          <span className="font-medium">
            {product.display_name || product.name}
          </span>
          <Badge
            variant={
              product.certification === "certified"
                ? "success"
                : product.certification === "deprecated"
                  ? "warning"
                  : "secondary"
            }
          >
            {product.certification}
          </Badge>
        </div>
        <Button variant="ghost" size="sm" onClick={() => setOpen(!open)}>
          Request access
        </Button>
      </div>
      {product.description && (
        <p className="mt-1 text-sm text-muted-foreground">
          {product.description}
        </p>
      )}
      {open && (
        <div className="mt-2 grid gap-2 sm:grid-cols-3">
          <Input
            value={ref}
            onChange={(e) => setRef(e.target.value)}
            placeholder="table:<id>"
          />
          <Input
            value={purpose}
            onChange={(e) => setPurpose(e.target.value)}
            placeholder="purpose"
          />
          <Button size="sm" onClick={request} disabled={busy}>
            Submit request
          </Button>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Access-request queue (J-F2 / reuses D-F4)
// ---------------------------------------------------------------------------

function RequestsCard() {
  const toast = useToast();
  const requests = useAsync(() => api.listAccessRequests(), []);

  async function decide(id: string, approve: boolean) {
    try {
      await api.decideAccessRequest(id, approve);
      requests.reload();
      toast.success(approve ? "Approved" : "Denied");
    } catch (err) {
      toast.error("Decision failed", (err as Error).message);
    }
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Access requests</CardTitle>
      </CardHeader>
      <CardContent>
        <Async state={requests} loadingLabel="Loading requests…">
          {(data) =>
            data.requests.length === 0 ? (
              <p className="text-sm text-muted-foreground">
                No access requests.
              </p>
            ) : (
              <div className="space-y-1">
                {data.requests.map((r) => (
                  <RequestRow
                    key={r.id}
                    request={r}
                    onDecide={(approve) => decide(r.id, approve)}
                  />
                ))}
              </div>
            )
          }
        </Async>
      </CardContent>
    </Card>
  );
}

function RequestRow({
  request,
  onDecide,
}: {
  request: AccessRequest;
  onDecide: (approve: boolean) => void;
}) {
  return (
    <div className="flex items-center justify-between rounded-md border border-border px-3 py-2 text-sm">
      <div>
        <span className="font-mono">{request.privilege}</span> on{" "}
        <span className="font-mono">{request.securable_id}</span>
        <span className="text-muted-foreground"> — {request.purpose}</span>
        <div className="text-xs text-muted-foreground">
          by {request.principal}
        </div>
      </div>
      <div className="flex items-center gap-1">
        <Badge
          variant={
            request.state === "approved"
              ? "success"
              : request.state === "denied"
                ? "danger"
                : request.state === "expired"
                  ? "secondary"
                  : "warning"
          }
        >
          {request.state}
        </Badge>
        {request.state === "pending" && (
          <>
            <Button variant="ghost" size="sm" onClick={() => onDecide(true)}>
              Approve
            </Button>
            <Button variant="ghost" size="sm" onClick={() => onDecide(false)}>
              Deny
            </Button>
          </>
        )}
      </div>
    </div>
  );
}
