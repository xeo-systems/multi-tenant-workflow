"use client";

import { useEffect, useMemo, useState } from "react";
import { apiFetch, getApiErrorKind, toApiError } from "@/lib/api";
import { useToast } from "@/lib/toast";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { EmptyState } from "@/components/empty-state";

const METRIC = "api_requests";

type UsageDaily = {
  id: string;
  orgId: string;
  metric: string;
  date: string;
  quantity: number;
};

type OrgResponse = {
  org: { id: string; name: string; plan: string; planLimit: number };
  role: string;
};

const USAGE_VERIFIED_KEY = "activationUsageVerified";

export default function UsagePage() {
  const { push } = useToast();
  const [usage, setUsage] = useState<UsageDaily[]>([]);
  const [loading, setLoading] = useState(true);
  const [plan, setPlan] = useState<{ plan: string; planLimit: number } | null>(null);
  const [forbidden, setForbidden] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [retryNonce, setRetryNonce] = useState(0);

  useEffect(() => {
    async function load() {
      setLoading(true);
      setForbidden(false);
      setErrorMessage(null);
      try {
        const orgData = await apiFetch<OrgResponse>("/org");
        setPlan({ plan: orgData.org.plan, planLimit: orgData.org.planLimit });

        const data = await apiFetch<UsageDaily[]>("/usage?days=30");
        setUsage(data.filter((entry) => entry.metric === METRIC));
      } catch (err) {
        const apiErr = toApiError(err, "Failed to load usage");
        const kind = getApiErrorKind(apiErr);
        if (kind === "forbidden") {
          setForbidden(true);
        } else {
          setErrorMessage(apiErr.message);
        }
        if (kind === "network") {
          push({ title: "Network error", description: "Could not reach API. Retry below.", variant: "destructive" });
        } else {
          push({ title: "Failed to load usage", description: apiErr.message, variant: "destructive" });
        }
      } finally {
        setLoading(false);
      }
    }
    load();
  }, [push, retryNonce]);

  const used = useMemo(() => usage.reduce((sum, entry) => sum + entry.quantity, 0), [usage]);
  const limit = plan?.planLimit ?? 1000;
  const ratio = limit > 0 ? used / limit : 0;
  const progressWidth = Math.min(Math.max(ratio * 100, 0), 100);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    if (used > 0) {
      window.localStorage.setItem(USAGE_VERIFIED_KEY, "1");
    }
  }, [used]);

  function usageMessage() {
    if (ratio >= 1) {
      return "Limit exceeded for this period. Requests may be blocked.";
    }
    if (ratio >= 0.8) {
      return "Approaching plan limit. Consider upgrading your plan.";
    }
    return "Usage is within plan limits.";
  }

  return (
    <div className="page-shell">
      <div className="page-header">
        <h1 className="page-title">Usage</h1>
        <p className="page-description">Track API activity and plan limits.</p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Current period</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {loading ? (
            <Skeleton className="h-16 w-full" />
          ) : (
            <>
              <p className="text-3xl font-semibold tracking-tight">
                {used} <span className="text-base font-normal text-muted-foreground">/ {limit} requests</span>
              </p>
              <div className="h-2 overflow-hidden rounded-full bg-muted" role="progressbar" aria-valuemin={0} aria-valuemax={limit} aria-valuenow={used}>
                <div
                  className="h-full rounded-full bg-primary transition-all"
                  style={{ width: `${progressWidth}%` }}
                />
              </div>
              <p className="text-sm text-muted-foreground capitalize">
                Plan: {plan?.plan || "free"} • {usageMessage()}
              </p>
              {used > 0 && (
                <div className="rounded-md border border-dashed p-4 text-sm">
                  <p className="font-medium">Activation success</p>
                  <p className="text-muted-foreground">
                    Usage has incremented from your API calls. Return to dashboard to see checklist completion.
                  </p>
                </div>
              )}
            </>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Last 30 days</CardTitle>
        </CardHeader>
        <CardContent>
          {loading ? (
            <div className="space-y-2">
              <Skeleton className="h-10 w-full" />
              <Skeleton className="h-10 w-full" />
              <Skeleton className="h-10 w-full" />
            </div>
          ) : forbidden ? (
            <EmptyState
              title="Permissions required"
              description="Only OWNER, ADMIN, and BILLING roles can view usage details."
            />
          ) : errorMessage ? (
            <EmptyState title="Unable to load usage" description={errorMessage} actionLabel="Retry" onAction={() => setRetryNonce((v) => v + 1)} />
          ) : usage.length === 0 ? (
            <EmptyState
              title="No usage yet"
              description="Create an API key, then call GET /data with X-Org-Id and Authorization: Bearer <api_key> to generate usage."
            />
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Date</TableHead>
                  <TableHead>Requests</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {usage.map((entry) => (
                  <TableRow key={entry.id}>
                    <TableCell>{new Date(entry.date).toLocaleDateString()}</TableCell>
                    <TableCell>{entry.quantity}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      {!loading && !forbidden && (
        <div className="flex justify-end">
          <Button variant="outline" onClick={() => setRetryNonce((v) => v + 1)}>
            Refresh usage
          </Button>
        </div>
      )}
    </div>
  );
}
