// Renders an Iceberg type into a compact human string, recursing into
// struct/list/map. Kept separate so the catalog schema tree stays declarative.

import type { IcebergType } from "./types";

export function typeToString(t: IcebergType | undefined): string {
  if (t === undefined) return "?";
  if (typeof t === "string") return t;
  switch (t.type) {
    case "struct": {
      const inner = (t.fields ?? [])
        .map((f) => `${f.name}: ${typeToString(f.type)}`)
        .join(", ");
      return `struct<${inner}>`;
    }
    case "list":
      return `list<${typeToString(t.element)}>`;
    case "map":
      return `map<${typeToString(t.key)}, ${typeToString(t.value)}>`;
    default:
      return "?";
  }
}

/** True when a type has expandable children (for the schema tree). */
export function hasChildren(t: IcebergType | undefined): boolean {
  if (t === undefined || typeof t === "string") return false;
  if (t.type === "struct") return (t.fields ?? []).length > 0;
  if (t.type === "list") return typeof t.element === "object";
  if (t.type === "map")
    return typeof t.key === "object" || typeof t.value === "object";
  return false;
}
