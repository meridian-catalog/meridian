"use client";

// Typed schema tree from an Iceberg schema's fields. Nested struct/list/map
// types expand inline.

import { useState } from "react";
import { ChevronRight, ChevronDown } from "lucide-react";
import type { IcebergField, IcebergType } from "@/lib/types";
import { typeToString } from "@/lib/schema";
import { Badge } from "./ui/primitives";

export function SchemaTree({
  fields,
  identifierFieldIds,
}: {
  fields: IcebergField[];
  identifierFieldIds?: number[];
}) {
  if (fields.length === 0) {
    return (
      <p className="py-4 text-sm text-muted-foreground">
        This schema has no fields.
      </p>
    );
  }
  const idSet = new Set(identifierFieldIds ?? []);
  return (
    <ul className="text-sm">
      {fields.map((f) => (
        <FieldNode key={f.id} field={f} depth={0} identifierIds={idSet} />
      ))}
    </ul>
  );
}

function childFields(type: IcebergType): IcebergField[] | null {
  if (typeof type === "string") return null;
  if (type.type === "struct") return type.fields ?? [];
  // Represent list/map element/key/value as synthetic child rows so the tree
  // stays uniform.
  if (type.type === "list" && typeof type.element === "object") {
    return [
      {
        id: type["element-id"] ?? -1,
        name: "element",
        required: type["element-required"] ?? false,
        type: type.element,
      },
    ];
  }
  if (type.type === "map") {
    const rows: IcebergField[] = [];
    if (typeof type.key === "object")
      rows.push({
        id: type["key-id"] ?? -1,
        name: "key",
        required: true,
        type: type.key,
      });
    if (typeof type.value === "object")
      rows.push({
        id: type["value-id"] ?? -1,
        name: "value",
        required: type["value-required"] ?? false,
        type: type.value,
      });
    return rows.length ? rows : null;
  }
  return null;
}

function FieldNode({
  field,
  depth,
  identifierIds,
}: {
  field: IcebergField;
  depth: number;
  identifierIds: Set<number>;
}) {
  const children = childFields(field.type);
  const [open, setOpen] = useState(depth < 1);
  const isId = identifierIds.has(field.id);

  return (
    <li>
      <div
        className="flex items-center gap-2 rounded px-1.5 py-1 hover:bg-accent/40"
        style={{ paddingLeft: `${depth * 18 + 4}px` }}
      >
        {children ? (
          <button
            onClick={() => setOpen((o) => !o)}
            className="text-muted-foreground"
            aria-label={open ? "Collapse" : "Expand"}
          >
            {open ? (
              <ChevronDown className="h-3.5 w-3.5" />
            ) : (
              <ChevronRight className="h-3.5 w-3.5" />
            )}
          </button>
        ) : (
          <span className="inline-block w-3.5" />
        )}
        <span className="font-mono text-xs text-muted-foreground">
          {field.id}
        </span>
        <span className="font-medium">{field.name}</span>
        <span className="font-mono text-xs text-sky-400">
          {typeToString(field.type)}
        </span>
        {field.required && (
          <Badge variant="outline" className="text-[10px]">
            required
          </Badge>
        )}
        {isId && (
          <Badge variant="default" className="text-[10px]">
            identifier
          </Badge>
        )}
        {field.doc && (
          <span className="truncate text-xs text-muted-foreground">
            — {field.doc}
          </span>
        )}
      </div>
      {children && open && (
        <ul>
          {children.map((c, i) => (
            <FieldNode
              key={`${c.id}-${i}`}
              field={c}
              depth={depth + 1}
              identifierIds={identifierIds}
            />
          ))}
        </ul>
      )}
    </li>
  );
}
