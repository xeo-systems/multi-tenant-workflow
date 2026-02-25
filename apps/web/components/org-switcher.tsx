"use client";

import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { getStoredOrgId, setStoredOrgId } from "@/lib/api";

export function OrgSwitcher() {
  const [orgId, setOrgId] = useState("");
  const [editing, setEditing] = useState(false);

  useEffect(() => {
    setOrgId(getStoredOrgId() || "");
  }, []);

  function save() {
    setStoredOrgId(orgId.trim());
    setEditing(false);
  }

  return (
    <div className="flex min-w-0 items-center gap-2">
      {editing ? (
        <>
          <Input
            aria-label="Organization ID"
            value={orgId}
            onChange={(e) => {
              const value = e.currentTarget.value;
              setOrgId(value);
            }}
            className="h-10 w-48 sm:w-56"
          />
          <Button size="sm" onClick={save} aria-label="Save organization">
            Save
          </Button>
        </>
      ) : (
        <Button variant="outline" size="sm" onClick={() => setEditing(true)} aria-label="Switch organization">
          {orgId ? `Org: ${orgId.slice(0, 6)}...` : "Set Org"}
        </Button>
      )}
    </div>
  );
}
