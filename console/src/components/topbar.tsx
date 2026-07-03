"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useState } from "react";
import { KeyRound, Check } from "lucide-react";
import { cn } from "@/lib/utils";
import { baseUrl } from "@/lib/api";
import { useAuth } from "@/lib/auth";
import { Button, Input } from "./ui/primitives";

const NAV = [
  { href: "/", label: "Overview" },
  { href: "/catalog", label: "Catalog" },
  { href: "/search", label: "Search" },
  { href: "/ops", label: "Ops" },
  { href: "/federation", label: "Federation" },
  { href: "/governance", label: "Governance" },
  { href: "/policies", label: "Policies" },
  { href: "/audit", label: "Audit" },
  { href: "/events", label: "Events" },
];

export function TopBar() {
  const pathname = usePathname();
  const { token, setToken } = useAuth();
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState("");

  const isActive = (href: string) =>
    href === "/" ? pathname === "/" : pathname.startsWith(href);

  return (
    <header className="sticky top-0 z-40 border-b border-border bg-card/80 backdrop-blur">
      <div className="mx-auto flex h-14 max-w-7xl items-center gap-6 px-4">
        <Link href="/" className="flex items-center gap-2 font-semibold">
          <span className="inline-block h-5 w-5 rounded bg-primary" />
          Meridian
          <span className="text-muted-foreground">Console</span>
        </Link>
        <nav className="flex items-center gap-1 text-sm">
          {NAV.map((item) => (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "rounded-md px-3 py-1.5 transition-colors",
                isActive(item.href)
                  ? "bg-accent text-accent-foreground"
                  : "text-muted-foreground hover:text-foreground",
              )}
            >
              {item.label}
            </Link>
          ))}
        </nav>
        <div className="ml-auto flex items-center gap-3">
          <span className="hidden font-mono text-xs text-muted-foreground sm:inline">
            {baseUrl()}
          </span>
          <div className="relative">
            <Button
              variant={token ? "secondary" : "outline"}
              size="sm"
              onClick={() => {
                setDraft(token ?? "");
                setOpen((o) => !o);
              }}
            >
              {token ? (
                <>
                  <Check className="h-3.5 w-3.5" /> Token set
                </>
              ) : (
                <>
                  <KeyRound className="h-3.5 w-3.5" /> Bearer token
                </>
              )}
            </Button>
            {open && (
              <div className="absolute right-0 top-10 w-80 rounded-md border border-border bg-card p-3 shadow-lg">
                <p className="mb-2 text-xs text-muted-foreground">
                  For servers in <span className="font-mono">oidc</span> mode.
                  Held in session memory only — never persisted to disk or
                  cookies.
                </p>
                <Input
                  type="password"
                  placeholder="Paste bearer token"
                  value={draft}
                  onChange={(e) => setDraft(e.target.value)}
                  autoFocus
                />
                <div className="mt-2 flex justify-end gap-2">
                  {token && (
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => {
                        setToken(null);
                        setDraft("");
                        setOpen(false);
                      }}
                    >
                      Clear
                    </Button>
                  )}
                  <Button
                    size="sm"
                    onClick={() => {
                      setToken(draft.trim() ? draft.trim() : null);
                      setOpen(false);
                    }}
                  >
                    Save
                  </Button>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </header>
  );
}
