"use client";

import { use } from "react";
import Link from "next/link";
import { ChevronLeft, Eye } from "lucide-react";
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
import type { ViewMetadata } from "@/lib/types";

export default function ViewDetailPage({
  params,
}: {
  params: Promise<{ warehouse: string; namespace: string; name: string }>;
}) {
  const { warehouse, namespace, name } = use(params);
  const wh = decodeURIComponent(warehouse);
  const levels = decodeNsParam(namespace);
  const view = decodeURIComponent(name);

  const state = useAsync(
    () => api.loadView(wh, levels, view),
    [wh, levels.join("\x1f"), view],
  );

  return (
    <div>
      <PageHeader
        title={view}
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

      <Async state={state} loadingLabel="Loading view…">
        {(result) => {
          const md = result.metadata;
          const currentVersion =
            md.versions?.find(
              (v) => v["version-id"] === md["current-version-id"],
            ) ?? md.versions?.[md.versions.length - 1];
          const schema =
            md.schemas?.find(
              (s) => s["schema-id"] === currentVersion?.["schema-id"],
            ) ?? md.schemas?.[0];
          const props = md.properties ?? {};

          return (
            <div className="space-y-4">
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="outline" className="gap-1">
                  <Eye className="h-3.5 w-3.5 text-violet-400/80" /> view
                </Badge>
                {md["format-version"] !== undefined && (
                  <Badge variant="secondary">
                    format v{md["format-version"]}
                  </Badge>
                )}
                {currentVersion && (
                  <Badge variant="outline">
                    version {currentVersion["version-id"]}
                  </Badge>
                )}
                {md["view-uuid"] && (
                  <Badge variant="outline" className="font-mono">
                    {md["view-uuid"]}
                  </Badge>
                )}
              </div>

              {/* SQL representations */}
              <Card>
                <CardHeader>
                  <CardTitle>SQL definition</CardTitle>
                </CardHeader>
                <CardContent>
                  {currentVersion &&
                  currentVersion.representations.length > 0 ? (
                    <div className="space-y-4">
                      {currentVersion["default-namespace"] && (
                        <p className="text-xs text-muted-foreground">
                          Default:{" "}
                          <span className="font-mono">
                            {currentVersion["default-catalog"]
                              ? `${currentVersion["default-catalog"]}.`
                              : ""}
                            {nsPath(currentVersion["default-namespace"])}
                          </span>
                        </p>
                      )}
                      {currentVersion.representations.map((rep, i) => (
                        <div key={i}>
                          <div className="mb-1.5 flex items-center gap-2">
                            <Badge variant="secondary" className="font-mono">
                              {rep.dialect}
                            </Badge>
                            <span className="text-xs text-muted-foreground">
                              {rep.type}
                            </span>
                          </div>
                          <pre className="overflow-x-auto rounded-md border border-border bg-muted/40 p-3 font-mono text-xs leading-relaxed">
                            {rep.sql}
                          </pre>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="py-4 text-sm text-muted-foreground">
                      This view has no SQL representations.
                    </p>
                  )}
                </CardContent>
              </Card>

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
                      This view has no schema fields.
                    </p>
                  )}
                </CardContent>
              </Card>

              {/* Properties */}
              <Card>
                <CardHeader>
                  <CardTitle>Properties</CardTitle>
                </CardHeader>
                <CardContent>
                  <KeyValueTable data={props} empty="No view properties." />
                </CardContent>
              </Card>

              {/* Metadata */}
              <Card>
                <CardHeader>
                  <CardTitle>Metadata</CardTitle>
                </CardHeader>
                <CardContent>
                  <ViewMetaTable
                    metadata={md}
                    metadataLocation={result["metadata-location"]}
                    lastUpdatedMs={currentVersion?.["timestamp-ms"]}
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

function ViewMetaTable({
  metadata,
  metadataLocation,
  lastUpdatedMs,
}: {
  metadata: ViewMetadata;
  metadataLocation?: string;
  lastUpdatedMs?: number;
}) {
  const rows: [string, string][] = [
    ["view-uuid", metadata["view-uuid"] ?? "—"],
    [
      "format-version",
      metadata["format-version"] !== undefined
        ? String(metadata["format-version"])
        : "—",
    ],
    ["location", metadata.location ?? "—"],
    ["metadata-location", metadataLocation ?? "—"],
    [
      "current-version-id",
      metadata["current-version-id"] !== undefined
        ? String(metadata["current-version-id"])
        : "—",
    ],
    ["current-version-timestamp", fmtEpochMs(lastUpdatedMs)],
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
