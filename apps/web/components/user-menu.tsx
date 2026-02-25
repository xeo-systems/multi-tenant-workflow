"use client";

import { useState } from "react";
import { Button } from "@/components/ui/button";
import { apiFetch } from "@/lib/api";
import { useRouter } from "next/navigation";
import { useToast } from "@/lib/toast";

export function UserMenu() {
  const [open, setOpen] = useState(false);
  const router = useRouter();
  const { push } = useToast();

  async function logout() {
    try {
      await apiFetch("/auth/logout", { method: "POST" });
      router.push("/login");
    } catch (err) {
      push({ title: "Logout failed", description: "Please try again", variant: "destructive" });
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
            className="w-full rounded-md px-3 py-2 text-left text-sm hover:bg-muted"
            onClick={() => router.push("/app/settings")}
          >
            Settings
          </button>
          <button
            className="w-full rounded-md px-3 py-2 text-left text-sm text-red-600 hover:bg-muted"
            onClick={logout}
          >
            Log out
          </button>
        </div>
      )}
    </div>
  );
}
