"use client";

import { useEffect, useState } from "react";
import { apiFetch, ApiError } from "@/lib/api";
import { useToast } from "@/lib/toast";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import { OrgSwitcher } from "@/components/org-switcher";
import { Skeleton } from "@/components/ui/skeleton";

type OrgResponse = {
  org: { id: string; name: string; plan: string; planLimit: number };
  role: string;
};

type MeResponse = {
  user: { id: string; email: string };
};

export default function SettingsPage() {
  const { push } = useToast();
  const [org, setOrg] = useState<OrgResponse | null>(null);
  const [me, setMe] = useState<MeResponse | null>(null);
  const [orgId, setOrgId] = useState("");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (typeof window !== "undefined") {
      setOrgId(window.localStorage.getItem("orgId") || "");
    }
  }, []);

  useEffect(() => {
    async function load() {
      setLoading(true);
      try {
        const [orgData, meData] = await Promise.all([
          apiFetch<OrgResponse>("/org"),
          apiFetch<MeResponse>("/auth/me", { skipOrgHeader: true }),
        ]);
        setOrg(orgData);
        setMe(meData);
      } catch (err) {
        const apiErr = err as ApiError;
        push({ title: "Failed to load settings", description: apiErr.message, variant: "destructive" });
      } finally {
        setLoading(false);
      }
    }
    load();
  }, [push]);

  function saveOrgId() {
    if (typeof window !== "undefined") {
      const nextOrgId = orgId.trim();
      window.localStorage.setItem("orgId", nextOrgId);
      setOrgId(nextOrgId);
      push({ title: "Org updated", description: "X-Org-Id value saved for future API requests." });
    }
  }

  async function copyOrgId() {
    if (!org?.org.id || typeof navigator === "undefined") {
      return;
    }
    await navigator.clipboard.writeText(org.org.id);
    push({ title: "Copied", description: "Organization ID copied." });
  }

  return (
    <div className="page-shell">
      <div className="page-header">
        <h1 className="page-title">Settings</h1>
        <p className="page-description">Profile and organization preferences.</p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Profile</CardTitle>
        </CardHeader>
        <CardContent>
          {loading ? (
            <Skeleton className="h-10 w-full" />
          ) : (
            <div className="space-y-2 text-sm">
              <p className="font-semibold">Email</p>
              <p className="text-muted-foreground">{me?.user.email || "Unavailable"}</p>
              <p className="text-xs text-muted-foreground">Password changes are not available in this build.</p>
            </div>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Organization</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {loading ? (
            <Skeleton className="h-16 w-full" />
          ) : org ? (
            <div className="space-y-2 text-sm">
              <p className="font-semibold">{org.org.name}</p>
              <p className="text-muted-foreground">Org ID: {org.org.id}</p>
              <p className="text-muted-foreground">Role: {org.role}</p>
              <p className="text-muted-foreground capitalize">Plan: {org.org.plan}</p>
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">No org loaded.</p>
          )}

          <div className="flex flex-wrap items-center gap-3">
            <Button onClick={copyOrgId} variant="outline" disabled={!org?.org.id}>
              Copy Org ID
            </Button>
            <OrgSwitcher />
          </div>

          <div className="space-y-2">
            <Label htmlFor="org-id">X-Org-Id for API requests</Label>
            <Input
              id="org-id"
              value={orgId}
              onChange={(e) => {
                const value = e.currentTarget.value;
                setOrgId(value);
              }}
            />
            <Button onClick={saveOrgId}>Save Org ID</Button>
            <p className="text-xs text-muted-foreground">
              This value is stored in your browser and attached as the <code>X-Org-Id</code> header by the web API client.
            </p>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
