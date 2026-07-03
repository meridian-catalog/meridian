import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]): string {
  return twMerge(clsx(inputs));
}

/** Formats an RFC 3339 / ISO timestamp for display, or returns "—". */
export function fmtTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString(undefined, {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

/** Formats a millisecond epoch (Iceberg timestamp-ms) for display. */
export function fmtEpochMs(ms: number | null | undefined): string {
  if (ms === null || ms === undefined) return "—";
  const d = new Date(ms);
  if (Number.isNaN(d.getTime())) return String(ms);
  return d.toLocaleString(undefined, {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

/** Relative "time ago" for short-lived feeds. */
export function timeAgo(iso: string | null | undefined): string {
  if (!iso) return "—";
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return iso;
  const secs = Math.round((Date.now() - then) / 1000);
  if (secs < 0) return "just now";
  if (secs < 60) return `${secs}s ago`;
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

/** Joins namespace levels for display. */
export function nsPath(levels: string[]): string {
  return levels.join(".");
}

// Namespace levels can contain dots, so a `.`-joined URL segment would be
// ambiguous. Encode the level array into a single opaque, round-trippable URL
// token by joining with the 0x1F unit separator (which levels may never
// contain) and URI-encoding.
const URL_NS_SEP = "\x1f";

export function encodeNsParam(levels: string[]): string {
  return encodeURIComponent(levels.join(URL_NS_SEP));
}

export function decodeNsParam(param: string): string[] {
  const raw = decodeURIComponent(param);
  return raw.length ? raw.split(URL_NS_SEP) : [];
}
