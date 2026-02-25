"use client";

import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

export function OrgSwitcher() {
  const [orgId, setOrgId] = useState("");
  const [editing, setEditing] = useState(false);

  useEffect(() => {
    if (typeof window !== "undefined") {
      setOrgId(window.localStorage.getItem("orgId") || "");
    }
  }, []);

  function save() {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("orgId", orgId.trim());
    }
    setEditing(false);
  }

  return (
    <div className="flex items-center gap-2">
      {editing ? (
        <>
          <Input
            aria-label="Organization ID"
            value={orgId}
            onChange={(e) => setOrgId(e.currentTarget.value)}
            className="h-8 w-48"
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
