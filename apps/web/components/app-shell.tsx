"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useState } from "react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
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

export function AppShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const [open, setOpen] = useState(false);

  return (
    <div className="min-h-screen bg-background">
      <header className="border-b bg-card">
        <div className="container-page flex h-16 items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <Button
              variant="ghost"
              size="icon"
              className="md:hidden"
              aria-label="Toggle navigation"
              onClick={() => setOpen((prev) => !prev)}
            >
              ☰
            </Button>
            <Link href="/app" className="text-lg font-semibold">
              Control Center
            </Link>
          </div>
          <div className="hidden md:flex items-center gap-3">
            <OrgSwitcher />
            <UserMenu />
          </div>
        </div>
      </header>
      <div className="container-page flex gap-6 py-6">
        <aside
          className={cn(
            "fixed inset-y-16 left-0 z-40 w-64 border-r bg-card p-4 transition-transform md:static md:translate-x-0",
            open ? "translate-x-0" : "-translate-x-full"
          )}
        >
          <nav className="flex flex-col gap-1" aria-label="Primary">
            {navItems.map((item) => {
              const active = pathname === item.href;
              return (
                <Link
                  key={item.href}
                  href={item.href}
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
        <main className="flex-1 space-y-6 md:ml-0" role="main">
          <div className="md:hidden flex items-center justify-between gap-2">
            <OrgSwitcher />
            <UserMenu />
          </div>
          {children}
        </main>
      </div>
    </div>
  );
}
