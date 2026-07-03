import type { Metadata } from "next";
import "./globals.css";
import { AuthProvider } from "@/lib/auth";
import { ToastProvider } from "@/components/toast";
import { TopBar } from "@/components/topbar";

export const metadata: Metadata = {
  title: "Meridian Console",
  description: "Browse and govern a Meridian Iceberg catalog.",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark">
      <body>
        <AuthProvider>
          <ToastProvider>
            <TopBar />
            <main className="mx-auto max-w-7xl px-4 py-6">{children}</main>
          </ToastProvider>
        </AuthProvider>
      </body>
    </html>
  );
}
