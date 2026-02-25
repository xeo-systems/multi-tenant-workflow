"use client";

import { useCallback, useEffect, useState } from "react";
import {
  apiFetch,
  getApiErrorKind,
  getStoredOrgId,
  ORG_ID_STORAGE_KEY,
  setStoredOrgId,
  toApiError,
} from "@/lib/api";
import { useToast } from "@/lib/toast";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import { OrgSwitcher } from "@/components/org-switcher";
import { Skeleton } from "@/components/ui/skeleton";
import { EmptyState } from "@/components/empty-state";
import { applyTheme, getStoredTheme, setTheme, THEME_STORAGE_KEY, ThemeMode } from "@/lib/theme";

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
  const [forbidden, setForbidden] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [orgIdError, setOrgIdError] = useState<string | null>(null);
  const [theme, setThemeState] = useState<ThemeMode>("light");

  const load = useCallback(async () => {
    setLoading(true);
    setForbidden(false);
    setErrorMessage(null);
    try {
      const [orgData, meData] = await Promise.all([
        apiFetch<OrgResponse>("/org"),
        apiFetch<MeResponse>("/auth/me", { skipOrgHeader: true }),
      ]);
      setOrg(orgData);
      setMe(meData);
    } catch (err) {
      const apiErr = toApiError(err, "Failed to load settings");
      const kind = getApiErrorKind(apiErr);
      if (kind === "forbidden") {
        setForbidden(true);
      } else {
        setErrorMessage(apiErr.message);
      }
      if (kind === "network") {
        push({ title: "Network error", description: "Could not reach API. Try again.", variant: "destructive" });
      } else {
        push({ title: "Failed to load settings", description: apiErr.message, variant: "destructive" });
      }
    } finally {
      setLoading(false);
    }
  }, [push]);

  useEffect(() => {
    setOrgId(getStoredOrgId() || "");
    const storedTheme = getStoredTheme();
    const initialTheme: ThemeMode = storedTheme === "dark" ? "dark" : "light";
    setThemeState(initialTheme);
    applyTheme(initialTheme);
    load();
  }, [load]);

  async function saveOrgId() {
    const nextOrgId = orgId.trim();
    if (!nextOrgId) {
      setOrgIdError("Organization ID is required");
      return;
    }
    setOrgIdError(null);
    setStoredOrgId(nextOrgId);
    setOrgId(nextOrgId);
    push({ title: "Org updated", description: "X-Org-Id saved. Refreshing tenant data..." });
    await load();
  }

  async function copyText(value: string, label: string) {
    if (!value || typeof navigator === "undefined") {
      return;
    }
    await navigator.clipboard.writeText(value);
    push({ title: "Copied", description: `${label} copied.` });
  }

  function handleThemeChange(nextTheme: ThemeMode) {
    setTheme(nextTheme);
    setThemeState(nextTheme);
    push({ title: "Theme updated", description: `${nextTheme === "dark" ? "Dark" : "Light"} mode enabled.` });
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
              <div className="flex flex-wrap items-center gap-2">
                <p className="text-muted-foreground">{me?.user.email || "Unavailable"}</p>
                <Button size="sm" variant="outline" onClick={() => copyText(me?.user.email || "", "Email")}>
                  Copy
                </Button>
              </div>
              <p className="text-xs text-muted-foreground">Password changes are not available in this build.</p>
            </div>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Appearance</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="space-y-2">
            <Label>Theme</Label>
            <div className="flex flex-wrap gap-2">
              <Button
                type="button"
                variant={theme === "light" ? "default" : "outline"}
                onClick={() => handleThemeChange("light")}
                aria-pressed={theme === "light"}
              >
                Light
              </Button>
              <Button
                type="button"
                variant={theme === "dark" ? "default" : "outline"}
                onClick={() => handleThemeChange("dark")}
                aria-pressed={theme === "dark"}
              >
                Dark
              </Button>
            </div>
            <p className="text-xs text-muted-foreground">
              Stored in browser local storage key <code>{THEME_STORAGE_KEY}</code>.
            </p>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Organization</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {loading ? (
            <Skeleton className="h-16 w-full" />
          ) : forbidden ? (
            <EmptyState
              title="Access denied for current org ID"
              description="You are not a member of this org or the ID is invalid. Update X-Org-Id below and refresh."
            />
          ) : org ? (
            <div className="space-y-2 text-sm">
              <p className="font-semibold">{org.org.name}</p>
              <div className="flex flex-wrap items-center gap-2">
                <p className="text-muted-foreground">Org ID: {org.org.id}</p>
                <Button size="sm" variant="outline" onClick={() => copyText(org.org.id, "Organization ID")}>
                  Copy Org ID
                </Button>
              </div>
              <p className="text-muted-foreground">Role: {org.role}</p>
              <p className="text-muted-foreground capitalize">Plan: {org.org.plan}</p>
            </div>
          ) : (
            <EmptyState
              title="No org loaded"
              description={errorMessage || "Set or fix orgId below, then refresh."}
            />
          )}

          <div className="flex flex-wrap items-center gap-4">
            <OrgSwitcher />
            <Button variant="outline" onClick={load}>
              Refresh data
            </Button>
          </div>

          <div className="space-y-2">
            <Label htmlFor="org-id">X-Org-Id for API requests</Label>
            <Input
              id="org-id"
              value={orgId}
              onChange={(e) => {
                const value = e.currentTarget.value;
                setOrgId(value);
                if (orgIdError) {
                  setOrgIdError(null);
                }
              }}
              placeholder="org_xxx"
            />
            {orgIdError && <p className="text-xs text-red-600">{orgIdError}</p>}
            <div className="flex flex-wrap gap-2">
              <Button onClick={saveOrgId}>Save Org ID</Button>
              <Button variant="outline" onClick={() => copyText(orgId, "Current X-Org-Id")}>
                Copy current value
              </Button>
            </div>
            <p className="text-xs text-muted-foreground">
              Stored in browser local storage key <code>{ORG_ID_STORAGE_KEY}</code> and attached to API requests as{" "}
              <code>X-Org-Id</code>.
            </p>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
