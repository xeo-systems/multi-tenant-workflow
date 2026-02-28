"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import {
  apiFetch,
  getApiErrorKind,
  getStoredOrgId,
  ORG_CONTEXT_UPDATED_EVENT,
  toApiError,
} from "@/lib/api";
import { OrgSwitcher } from "@/components/org-switcher";
import { UserMenu } from "@/components/user-menu";

const isDemoMode = process.env["NEXT_PUBLIC_DEMO_MODE"] === "true";

const navItems = [
  { label: "Dashboard", href: "/app" },
  { label: "Members", href: "/app/members" },
  { label: "API Keys", href: "/app/api-keys" },
  { label: "Usage", href: "/app/usage" },
  { label: "Billing", href: "/app/billing" },
  { label: "Settings", href: "/app/settings" },
];

type OrgSummary = {
  org: { id: string; name: string; plan: string };
};

export function AppShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const [open, setOpen] = useState(false);
  const [orgSummary, setOrgSummary] = useState<OrgSummary | null>(null);
  const [localOrgId, setLocalOrgId] = useState("");
  const [orgSummaryHint, setOrgSummaryHint] = useState<string | null>(null);
  const [orgReloadNonce, setOrgReloadNonce] = useState(0);

  useEffect(() => {
    setLocalOrgId(getStoredOrgId() || "");
    let mounted = true;
    async function loadOrgSummary() {
      try {
        const data = await apiFetch<OrgSummary>("/org");
        if (mounted) {
          setOrgSummary(data);
          setOrgSummaryHint(null);
        }
      } catch (err) {
        if (mounted) {
          const apiErr = toApiError(err, "Unable to load organization");
          const kind = getApiErrorKind(apiErr);
          setOrgSummary(null);
          if (!getStoredOrgId()) {
            setOrgSummaryHint("Set an org ID in settings");
          } else if (kind === "forbidden") {
            setOrgSummaryHint("Selected org ID is not accessible");
          } else if (kind === "network") {
            setOrgSummaryHint("API unavailable");
          } else {
            setOrgSummaryHint("Unable to load organization");
          }
        }
      }
    }
    loadOrgSummary();

    function handleOrgUpdate() {
      setLocalOrgId(getStoredOrgId() || "");
      setOrgReloadNonce((prev) => prev + 1);
    }

    if (typeof window !== "undefined") {
      window.addEventListener("storage", handleOrgUpdate);
      window.addEventListener(ORG_CONTEXT_UPDATED_EVENT, handleOrgUpdate);
    }

    return () => {
      mounted = false;
      if (typeof window !== "undefined") {
        window.removeEventListener("storage", handleOrgUpdate);
        window.removeEventListener(ORG_CONTEXT_UPDATED_EVENT, handleOrgUpdate);
      }
    };
  }, [pathname, orgReloadNonce]);

  useEffect(() => {
    let active = true;

    async function verifySession() {
      try {
        await apiFetch("/auth/me", { skipOrgHeader: true });
      } catch {
        // apiFetch handles 401 by redirecting to /login.
      }
    }

    function onPageShow() {
      if (!active) {
        return;
      }
      verifySession();
    }

    if (typeof window !== "undefined") {
      window.addEventListener("pageshow", onPageShow);
      document.addEventListener("visibilitychange", onPageShow);
    }

    verifySession();

    return () => {
      active = false;
      if (typeof window !== "undefined") {
        window.removeEventListener("pageshow", onPageShow);
        document.removeEventListener("visibilitychange", onPageShow);
      }
    };
  }, [pathname]);

  return (
    <div className="min-h-screen bg-background">
      {isDemoMode && (
        <div className="border-b border-amber-200 bg-amber-50 text-amber-900">
          <div className="container-page py-2 text-xs font-semibold tracking-wide">Demo mode (limited)</div>
        </div>
      )}
      <header className="sticky top-0 z-40 border-b border-border bg-card shadow-sm">
        <div className="container-page flex min-h-[72px] items-center justify-between gap-4 py-4">
          <div className="flex items-center gap-4">
            <Button
              variant="ghost"
              size="icon"
              className="md:hidden"
              aria-label="Toggle navigation"
              aria-expanded={open}
              onClick={() => setOpen((prev) => !prev)}
            >
              ☰
            </Button>
            <Link href="/app" className="text-lg font-semibold">
              Control Center
            </Link>
            <div className="hidden min-w-0 md:flex flex-col">
              <p className="text-sm font-semibold">{orgSummary?.org.name || "Organization"}</p>
              <p className="truncate text-xs text-muted-foreground">
                {orgSummary?.org.id || localOrgId || orgSummaryHint || "Set an org ID in settings"}
              </p>
              {orgSummaryHint && localOrgId && !orgSummary?.org.id && (
                <p className="truncate text-xs text-error">{orgSummaryHint}</p>
              )}
            </div>
            <Badge className="hidden md:inline-flex capitalize" variant="secondary">
              {orgSummary?.org.plan || "Free"}
            </Badge>
          </div>
          <div className="hidden md:flex items-center gap-4">
            <OrgSwitcher />
            <UserMenu />
          </div>
        </div>
      </header>
      <div className="container-page flex gap-6 py-8">
        {open && (
          <button
            className="fixed inset-0 z-20 bg-black/30 md:hidden"
            aria-label="Close navigation"
            onClick={() => setOpen(false)}
          />
        )}
        <aside
          className={cn(
            "fixed bottom-0 left-0 top-[72px] z-30 w-[85vw] max-w-[320px] border-r border-border bg-card p-4 transition-transform md:static md:inset-y-0 md:w-64 md:max-w-none md:translate-x-0 md:rounded-lg md:border md:border-border md:pt-4",
            open ? "translate-x-0" : "-translate-x-full"
          )}
        >
          <nav className="flex flex-col gap-2" aria-label="Primary">
            {navItems.map((item) => {
              const active = item.href === "/app" ? pathname === item.href : pathname.startsWith(item.href);
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  onClick={() => setOpen(false)}
                  className={cn(
                    "rounded-[8px] px-4 py-2.5 text-sm font-medium text-foreground focus:outline-none focus:ring-2 focus:ring-ring",
                    active ? "bg-primary/10 text-primary" : "hover:bg-muted"
                  )}
                >
                  {item.label}
                </Link>
              );
            })}
          </nav>
        </aside>
        <main className="min-w-0 flex-1 pt-2 md:pt-0" role="main">
          <div className="flex items-center justify-between gap-2 md:hidden">
            <OrgSwitcher />
            <UserMenu />
          </div>
          {children}
        </main>
      </div>
    </div>
  );
}
