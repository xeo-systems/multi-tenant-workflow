"use client";

import { useState } from "react";
import { Button } from "@/components/ui/button";
import { apiFetch, clearStoredOrgId } from "@/lib/api";
import { useRouter } from "next/navigation";

export function UserMenu() {
  const [open, setOpen] = useState(false);
  const router = useRouter();

  async function logout() {
    try {
      await apiFetch("/auth/logout", { method: "POST", skipOrgHeader: true });
    } catch {
      // Best effort: continue local sign-out even if API logout fails.
    } finally {
      clearStoredOrgId();
      router.replace("/login");
    }
  }

  return (
    <div className="relative">
      <Button variant="outline" size="sm" onClick={() => setOpen((prev) => !prev)} aria-label="User menu">
        Account
      </Button>
      {open && (
        <div className="absolute right-0 mt-2 w-40 rounded-md border bg-card p-2 shadow-lg">
          <button
            className="w-full rounded-md px-4 py-2 text-left text-sm hover:bg-muted"
            onClick={() => router.push("/app/settings")}
          >
            Settings
          </button>
          <button
            className="w-full rounded-md px-4 py-2 text-left text-sm text-red-600 hover:bg-muted"
            onClick={logout}
          >
            Log out
          </button>
        </div>
      )}
    </div>
  );
}
