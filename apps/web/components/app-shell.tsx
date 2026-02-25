"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { apiFetch } from "@/lib/api";
import { OrgSwitcher } from "@/components/org-switcher";
import { UserMenu } from "@/components/user-menu";

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

  useEffect(() => {
    if (typeof window !== "undefined") {
      setLocalOrgId(window.localStorage.getItem("orgId") || "");
    }
    let mounted = true;
    async function loadOrgSummary() {
      try {
        const data = await apiFetch<OrgSummary>("/org");
        if (mounted) {
          setOrgSummary(data);
        }
      } catch {
        if (mounted) {
          setOrgSummary(null);
        }
      }
    }
    loadOrgSummary();
    return () => {
      mounted = false;
    };
  }, [pathname]);

  return (
    <div className="min-h-screen bg-background">
      <header className="sticky top-0 z-30 border-b bg-card/95 backdrop-blur">
        <div className="container-page flex min-h-[72px] items-center justify-between gap-4 py-3">
          <div className="flex items-center gap-3">
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
                {orgSummary?.org.id || localOrgId || "Set an org ID in settings"}
              </p>
            </div>
            <Badge className="hidden md:inline-flex capitalize" variant="secondary">
              {orgSummary?.org.plan || "Free"}
            </Badge>
          </div>
          <div className="hidden md:flex items-center gap-3">
            <OrgSwitcher />
            <UserMenu />
          </div>
        </div>
      </header>
      <div className="container-page flex gap-6 py-6">
        {open && (
          <button
            className="fixed inset-0 z-30 bg-black/30 md:hidden"
            aria-label="Close navigation"
            onClick={() => setOpen(false)}
          />
        )}
        <aside
          className={cn(
            "fixed inset-y-0 left-0 z-40 w-[85vw] max-w-[320px] border-r bg-card p-4 pt-24 transition-transform md:static md:inset-y-0 md:w-64 md:max-w-none md:translate-x-0 md:rounded-lg md:border md:pt-4",
            open ? "translate-x-0" : "-translate-x-full"
          )}
        >
          <nav className="flex flex-col gap-1" aria-label="Primary">
            {navItems.map((item) => {
              const active = item.href === "/app" ? pathname === item.href : pathname.startsWith(item.href);
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  onClick={() => setOpen(false)}
                  className={cn(
                    "rounded-md px-3 py-2 text-sm font-medium focus:outline-none focus:ring-2 focus:ring-ring",
                    active ? "bg-muted" : "hover:bg-muted"
                  )}
                >
                  {item.label}
                </Link>
              );
            })}
          </nav>
        </aside>
        <main className="min-w-0 flex-1 page-shell" role="main">
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
