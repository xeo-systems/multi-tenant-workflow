"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import {
  apiFetch,
  DASHBOARD_CHECKLIST_DISMISSED_KEY,
  getApiErrorKind,
  getStoredOrgId,
  toApiError,
} from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Button } from "@/components/ui/button";
import { useToast } from "@/lib/toast";

type OrgResponse = {
  org: { id: string; name: string; plan: string; planLimit: number };
  role: string;
};

type UsageDaily = {
  id: string;
  metric: string;
  quantity: number;
};

type ApiKey = {
  id: string;
  revokedAt?: string | null;
};

type Member = {
  id: string;
};

type ChecklistSnapshot = {
  hasApiKey: boolean;
  usageTotal: number;
  hasTeammate: boolean;
};

const EMPTY_SNAPSHOT: ChecklistSnapshot = {
  hasApiKey: false,
  usageTotal: 0,
  hasTeammate: false,
};

const USAGE_VERIFIED_KEY = "activationUsageVerified";

type ChecklistItem = {
  label: string;
  done: boolean;
  href: string;
  optional?: boolean;
};

export default function DashboardPage() {
  const [data, setData] = useState<OrgResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [storedOrgId, setStoredOrgId] = useState("");
  const [snapshot, setSnapshot] = useState<ChecklistSnapshot>(EMPTY_SNAPSHOT);
  const [forbiddenMessage, setForbiddenMessage] = useState<string | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [dismissedChecklist, setDismissedChecklist] = useState(false);
  const [retryNonce, setRetryNonce] = useState(0);
  const { push } = useToast();
  const router = useRouter();

  useEffect(() => {
    const orgId = getStoredOrgId() || "";
    setStoredOrgId(orgId);
    if (typeof window !== "undefined") {
      const dismissed = window.localStorage.getItem(DASHBOARD_CHECKLIST_DISMISSED_KEY) === "1";
      setDismissedChecklist(dismissed);
    }
  }, []);

  useEffect(() => {
    let mounted = true;

    async function load() {
      const orgId = getStoredOrgId() || "";
      if (!orgId) {
        if (mounted) {
          setStoredOrgId("");
          setData(null);
          setSnapshot(EMPTY_SNAPSHOT);
          setForbiddenMessage(null);
          setLoadError(null);
          setLoading(false);
        }
        return;
      }

      if (mounted) {
        setStoredOrgId(orgId);
        setLoading(true);
        setForbiddenMessage(null);
        setLoadError(null);
      }

      try {
        const orgData = await apiFetch<OrgResponse>("/org");

        const [keysResult, usageResult, membersResult] = await Promise.allSettled([
          apiFetch<ApiKey[]>("/api-keys"),
          apiFetch<UsageDaily[]>("/usage?days=30"),
          apiFetch<Member[]>("/org/members"),
        ]);

        const activeKeys =
          keysResult.status === "fulfilled" ? keysResult.value.filter((key) => !key.revokedAt).length : 0;
        const usageTotal =
          usageResult.status === "fulfilled"
            ? usageResult.value.filter((row) => row.metric === "api_requests").reduce((sum, row) => sum + row.quantity, 0)
            : 0;
        const teammateCount = membersResult.status === "fulfilled" ? membersResult.value.length : 0;
        if (mounted) {
          setData(orgData);
          setSnapshot({
            hasApiKey: activeKeys > 0,
            usageTotal,
            hasTeammate: teammateCount > 1,
          });
        }
      } catch (err) {
        const apiErr = toApiError(err, "Failed to load dashboard");
        const kind = getApiErrorKind(apiErr);
        if (kind === "forbidden") {
          if (mounted) {
            setForbiddenMessage(
              `${apiErr.message} This org ID is invalid for your account or you are no longer a member.`
            );
            setData(null);
          }
          return;
        }

        if (mounted) {
          setLoadError(apiErr.message);
        }

        if (kind === "network") {
          push({ title: "Network error", description: "Could not reach the API. You can retry below.", variant: "destructive" });
        } else {
          push({ title: "Unable to load dashboard", description: apiErr.message, variant: "destructive" });
        }
      } finally {
        if (mounted) {
          setLoading(false);
        }
      }
    }

    load();
    return () => {
      mounted = false;
    };
  }, [push, router, retryNonce]);

  const checklistItems = useMemo<ChecklistItem[]>(
    () => [
      {
        label: "Confirm org context",
        done: Boolean(storedOrgId),
        href: "/app/settings",
      },
      {
        label: "Create API key",
        done: snapshot.hasApiKey,
        href: "/app/api-keys",
      },
      {
        label: "Make first API call",
        done: snapshot.usageTotal > 0,
        href: "/app/api-keys",
      },
      {
        label: "Verify usage appears",
        done:
          typeof window !== "undefined" &&
          window.localStorage.getItem(USAGE_VERIFIED_KEY) === "1" &&
          snapshot.usageTotal > 0,
        href: "/app/usage",
      },
      {
        label: "Invite teammate (optional)",
        done: snapshot.hasTeammate,
        href: "/app/members",
        optional: true,
      },
    ],
    [snapshot.hasApiKey, snapshot.hasTeammate, snapshot.usageTotal, storedOrgId]
  );

  const activationComplete = checklistItems
    .filter((item) => !item.optional)
    .every((item) => item.done);

  function dismissChecklist() {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(DASHBOARD_CHECKLIST_DISMISSED_KEY, "1");
    }
    setDismissedChecklist(true);
  }

  function retryLoad() {
    setRetryNonce((prev) => prev + 1);
  }

  if (loading) {
    return (
      <div className="grid gap-6 md:grid-cols-3">
        <Skeleton className="h-28" />
        <Skeleton className="h-28" />
        <Skeleton className="h-28" />
      </div>
    );
  }

  return (
    <div className="page-shell">
      <div className="page-header">
        <h1 className="page-title">Welcome back</h1>
        <p className="page-description">Organization overview and plan status.</p>
      </div>

      {!storedOrgId && (
        <Card>
          <CardHeader>
            <CardTitle>Organization context required</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <p className="text-sm text-muted-foreground">
              No organization is selected. Set an <code>X-Org-Id</code> first to load tenant data.
            </p>
            <Button onClick={() => router.push("/app/settings")}>Go to Settings</Button>
          </CardContent>
        </Card>
      )}

      {forbiddenMessage && (
        <Card>
          <CardHeader>
            <CardTitle>Access denied for selected org</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <p className="text-sm text-muted-foreground">{forbiddenMessage}</p>
            <Button onClick={() => router.push("/app/settings")} variant="outline">
              Change org ID in Settings
            </Button>
          </CardContent>
        </Card>
      )}

      {loadError && (
        <Card>
          <CardHeader>
            <CardTitle>Dashboard unavailable</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <p className="text-sm text-muted-foreground">{loadError}</p>
            <Button onClick={retryLoad} variant="outline">
              Retry
            </Button>
          </CardContent>
        </Card>
      )}

      {!dismissedChecklist && !activationComplete && (
        <Card>
          <CardHeader className="flex-row items-center justify-between gap-4">
            <CardTitle>Activation checklist</CardTitle>
            <Button size="sm" variant="ghost" onClick={dismissChecklist}>
              Dismiss
            </Button>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-4">
              {checklistItems.map((item) => (
                <div key={item.label} className="flex items-start justify-between gap-4 rounded-md border p-4">
                  <div className="space-y-2">
                    <p className="text-sm font-medium">{item.label}</p>
                    <p className="text-xs text-muted-foreground">
                      {item.done ? "Complete" : item.optional ? "Optional" : "Pending"}
                    </p>
                  </div>
                  <Link href={item.href}>
                    <Button size="sm" variant={item.done ? "outline" : "default"}>
                      {item.done ? "View" : "Open"}
                    </Button>
                  </Link>
                </div>
              ))}
            </div>
            <div className="rounded-md border bg-muted/40 p-4 text-xs">
              <p className="font-semibold">First API call snippet</p>
              <pre className="mt-2 overflow-x-auto whitespace-pre-wrap text-[11px] text-muted-foreground">
{`curl -X GET ${process.env["NEXT_PUBLIC_API_BASE_URL"] || "http://localhost:4000"}/data \\
  -H "X-Org-Id: ${storedOrgId || "<your-org-id>"}" \\
  -H "Authorization: Bearer <paste-key-here>"`}
              </pre>
            </div>
          </CardContent>
        </Card>
      )}

      {activationComplete && (
        <Card>
          <CardHeader>
            <CardTitle>Activation complete</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm text-muted-foreground">
              First API call succeeded and usage is visible. Your org is now fully activated.
            </p>
          </CardContent>
        </Card>
      )}

      {data && !forbiddenMessage && !loadError && (
        <div className="section-grid md:grid-cols-3">
          <Card>
            <CardHeader>
              <CardTitle>Organization</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-base font-semibold">{data.org.name || "Unnamed organization"}</p>
              <p className="text-sm text-muted-foreground">ID: {data.org.id || "No org selected"}</p>
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>Plan</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-base font-semibold capitalize">{data.org.plan || "free"}</p>
              <p className="text-sm text-muted-foreground">
                Limit: {typeof data.org.planLimit === "number" ? data.org.planLimit : 1000} requests
              </p>
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>Role</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-base font-semibold">{data.role || "MEMBER"}</p>
              <p className="text-sm text-muted-foreground">Permissions scoped to your role.</p>
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
}
