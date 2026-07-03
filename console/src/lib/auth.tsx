"use client";

// Session-memory bearer token store. The token lives in React state (and
// sessionStorage so it survives a tab reload but not a browser restart); it is
// NEVER written to a cookie or sent anywhere but the configured Meridian base
// URL via the Authorization header in the API client.

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";
import { setTokenProvider } from "./api";

const STORAGE_KEY = "meridian_console_token";

interface AuthContextValue {
  token: string | null;
  setToken: (token: string | null) => void;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [token, setTokenState] = useState<string | null>(null);

  // Rehydrate from sessionStorage on mount (client only).
  useEffect(() => {
    try {
      const stored = sessionStorage.getItem(STORAGE_KEY);
      if (stored) setTokenState(stored);
    } catch {
      // sessionStorage unavailable (private mode); token stays in memory only.
    }
  }, []);

  const setToken = useCallback((next: string | null) => {
    setTokenState(next);
    try {
      if (next) sessionStorage.setItem(STORAGE_KEY, next);
      else sessionStorage.removeItem(STORAGE_KEY);
    } catch {
      // Ignore; in-memory value still applies for this session.
    }
  }, []);

  // Keep the plain API client's token provider in sync with context.
  useEffect(() => {
    setTokenProvider(() => token);
  }, [token]);

  const value = useMemo(() => ({ token, setToken }), [token, setToken]);
  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within AuthProvider");
  return ctx;
}
