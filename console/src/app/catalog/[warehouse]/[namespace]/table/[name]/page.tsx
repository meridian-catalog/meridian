"use client";

import { use } from "react";
import Link from "next/link";
import { ChevronLeft, Table2 } from "lucide-react";
import { api } from "@/lib/api";
import { decodeNsParam, nsPath, fmtEpochMs } from "@/lib/utils";
import { PageHeader } from "@/components/page-header";
import { Async, useAsync } from "@/components/states";
import { SchemaTree } from "@/components/schema-tree";
import {
  Badge,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/primitives";
import type { IcebergSnapshot, TableMetadata } from "@/lib/types";

export default function TableDetailPage({
  params,
}: {
  params: Promise<{ warehouse: string; namespace: string; name: string }>;
}) {
  const { warehouse, namespace, name } = use(params);
  const wh = decodeURIComponent(warehouse);
  const levels = decodeNsParam(namespace);
  const table = decodeURIComponent(name);

  const state = useAsync(
    () => api.loadTable(wh, levels, table),
    [wh, levels.join("\x1f"), table],
  );

  return (
    <div>
      <PageHeader
        title={table}
        description={`${wh} · ${nsPath(levels)}`}
        actions={
          <Link
            href="/catalog"
            className="inline-flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
          >
            <ChevronLeft className="h-4 w-4" /> Catalog
          </Link>
        }
      />

      <Async state={state} loadingLabel="Loading table…">
        {(result) => {
          const md = result.metadata;
          const schema =
            md.schemas?.find(
              (s) => s["schema-id"] === md["current-schema-id"],
            ) ?? md.schemas?.[0];
          const snapshots = [...(md.snapshots ?? [])].sort(
            (a, b) => b["timestamp-ms"] - a["timestamp-ms"],
          );
          const props = md.properties ?? {};

          return (
            <div className="space-y-4">
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="outline" className="gap-1">
                  <Table2 className="h-3.5 w-3.5 text-sky-400/80" /> table
                </Badge>
                <Badge variant="secondary">
                  format v{md["format-version"]}
                </Badge>
                {md["table-uuid"] && (
                  <Badge variant="outline" className="font-mono">
                    {md["table-uuid"]}
                  </Badge>
                )}
              </div>

              {/* Schema */}
              <Card>
                <CardHeader>
                  <CardTitle>Schema</CardTitle>
                </CardHeader>
                <CardContent>
                  {schema && schema.fields.length > 0 ? (
                    <div className="overflow-x-auto">
                      <SchemaTree
                        fields={schema.fields}
                        identifierFieldIds={schema["identifier-field-ids"]}
                      />
                    </div>
                  ) : (
                    <p className="py-4 text-sm text-muted-foreground">
                      This table has no schema fields.
                    </p>
                  )}
                </CardContent>
              </Card>

              {/* Snapshots */}
              <Card>
                <CardHeader>
                  <CardTitle>Snapshots</CardTitle>
                </CardHeader>
                <CardContent>
                  {snapshots.length === 0 ? (
                    <p className="py-4 text-sm text-muted-foreground">
                      No snapshots yet.
                    </p>
                  ) : (
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="border-b border-border text-left text-xs uppercase tracking-wide text-muted-foreground">
                            <th className="py-2 pr-4 font-medium">Snapshot</th>
                            <th className="py-2 pr-4 font-medium">Operation</th>
                            <th className="py-2 pr-4 font-medium">Timestamp</th>
                            <th className="py-2 font-medium">Summary</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-border">
                          {snapshots.map((s) => (
                            <SnapshotRow
                              key={s["snapshot-id"]}
                              snapshot={s}
                              current={
                                s["snapshot-id"] === md["current-snapshot-id"]
                              }
                            />
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </CardContent>
              </Card>

              {/* Properties */}
              <Card>
                <CardHeader>
                  <CardTitle>Properties</CardTitle>
                </CardHeader>
                <CardContent>
                  <KeyValueTable data={props} empty="No table properties." />
                </CardContent>
              </Card>

              {/* Metadata */}
              <Card>
                <CardHeader>
                  <CardTitle>Metadata</CardTitle>
                </CardHeader>
                <CardContent>
                  <MetadataTable
                    metadata={md}
                    metadataLocation={result["metadata-location"]}
                  />
                </CardContent>
              </Card>
            </div>
          );
        }}
      </Async>
    </div>
  );
}

function SnapshotRow({
  snapshot,
  current,
}: {
  snapshot: IcebergSnapshot;
  current: boolean;
}) {
  const summary = snapshot.summary ?? {};
  const operation = summary["operation"] ?? "—";
  const counts = Object.entries(summary).filter(([k]) => k !== "operation");
  return (
    <tr className="align-top">
      <td className="py-2 pr-4 font-mono text-xs">
        <div className="flex items-center gap-2">
          {snapshot["snapshot-id"]}
          {current && <Badge variant="success">current</Badge>}
        </div>
      </td>
      <td className="py-2 pr-4">
        <Badge variant="outline">{operation}</Badge>
      </td>
      <td className="py-2 pr-4 text-muted-foreground">
        {fmtEpochMs(snapshot["timestamp-ms"])}
      </td>
      <td className="py-2">
        {counts.length === 0 ? (
          <span className="text-xs text-muted-foreground">—</span>
        ) : (
          <div className="flex flex-wrap gap-1">
            {counts.map(([k, v]) => (
              <span
                key={k}
                className="rounded bg-muted px-1.5 py-0.5 font-mono text-[11px] text-muted-foreground"
              >
                {k}={v}
              </span>
            ))}
          </div>
        )}
      </td>
    </tr>
  );
}

function KeyValueTable({
  data,
  empty,
}: {
  data: Record<string, string>;
  empty: string;
}) {
  const entries = Object.entries(data);
  if (entries.length === 0) {
    return <p className="py-4 text-sm text-muted-foreground">{empty}</p>;
  }
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <tbody className="divide-y divide-border">
          {entries.map(([k, v]) => (
            <tr key={k}>
              <td className="w-1/3 py-2 pr-4 align-top font-mono text-xs text-muted-foreground">
                {k}
              </td>
              <td className="py-2 font-mono text-xs break-all">{v}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function MetadataTable({
  metadata,
  metadataLocation,
}: {
  metadata: TableMetadata;
  metadataLocation?: string;
}) {
  const rows: [string, string][] = [
    ["table-uuid", metadata["table-uuid"] ?? "—"],
    ["format-version", String(metadata["format-version"] ?? "—")],
    ["location", metadata.location ?? "—"],
    ["metadata-location", metadataLocation ?? "—"],
    [
      "last-updated",
      fmtEpochMs(metadata["last-updated-ms"]),
    ],
    [
      "current-schema-id",
      metadata["current-schema-id"] !== undefined
        ? String(metadata["current-schema-id"])
        : "—",
    ],
    [
      "current-snapshot-id",
      metadata["current-snapshot-id"] !== undefined
        ? String(metadata["current-snapshot-id"])
        : "—",
    ],
  ];
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <tbody className="divide-y divide-border">
          {rows.map(([k, v]) => (
            <tr key={k}>
              <td className="w-1/3 py-2 pr-4 align-top font-mono text-xs text-muted-foreground">
                {k}
              </td>
              <td className="py-2 font-mono text-xs break-all">{v}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
