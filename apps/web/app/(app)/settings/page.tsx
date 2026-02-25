"use client";

import { useEffect, useState } from "react";
import { apiFetch, ApiError } from "@/lib/api";
import { useToast } from "@/lib/toast";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";

type OrgResponse = {
  org: { id: string; name: string; plan: string; planLimit: number };
  role: string;
};

export default function SettingsPage() {
  const { push } = useToast();
  const [org, setOrg] = useState<OrgResponse | null>(null);
  const [orgId, setOrgId] = useState("");

  useEffect(() => {
    if (typeof window !== "undefined") {
      setOrgId(window.localStorage.getItem("orgId") || "");
    }
  }, []);

  useEffect(() => {
    async function load() {
      try {
        const data = await apiFetch<OrgResponse>("/org");
        setOrg(data);
      } catch (err) {
        const apiErr = err as ApiError;
        push({ title: "Failed to load org", description: apiErr.message, variant: "destructive" });
      }
    }
    load();
  }, [push]);

  function saveOrgId() {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("orgId", orgId.trim());
      push({ title: "Org updated", description: "Org ID saved to browser." });
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold">Settings</h1>
        <p className="text-sm text-muted-foreground">Profile and organization preferences.</p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Organization</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {org ? (
            <div className="space-y-1 text-sm">
              <p className="font-semibold">{org.org.name}</p>
              <p className="text-muted-foreground">Org ID: {org.org.id}</p>
              <p className="text-muted-foreground">Role: {org.role}</p>
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">No org loaded.</p>
          )}
          <div className="space-y-2">
            <Label htmlFor="org-id">Org ID (used for API requests)</Label>
            <Input id="org-id" value={orgId} onChange={(e) => setOrgId(e.currentTarget.value)} />
            <Button onClick={saveOrgId}>Save Org ID</Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
