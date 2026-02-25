"use client";

import { useEffect, useMemo, useState } from "react";
import { apiFetch, ApiError } from "@/lib/api";
import { useToast } from "@/lib/toast";
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

export default function UsagePage() {
  const { push } = useToast();
  const [usage, setUsage] = useState<UsageDaily[]>([]);
  const [loading, setLoading] = useState(true);
  const [plan, setPlan] = useState<{ plan: string; planLimit: number } | null>(null);
  const [forbidden, setForbidden] = useState(false);

  useEffect(() => {
    async function load() {
      setLoading(true);
      setForbidden(false);
      try {
        const orgData = await apiFetch<OrgResponse>("/org");
        setPlan({ plan: orgData.org.plan, planLimit: orgData.org.planLimit });

        const data = await apiFetch<UsageDaily[]>("/usage?days=30");
        setUsage(data.filter((entry) => entry.metric === METRIC));
      } catch (err) {
        const apiErr = err as ApiError;
        if (apiErr.status === 403) {
          setForbidden(true);
        } else {
          push({ title: "Failed to load usage", description: apiErr.message, variant: "destructive" });
        }
      } finally {
        setLoading(false);
      }
    }
    load();
  }, [push]);

  const used = useMemo(() => usage.reduce((sum, entry) => sum + entry.quantity, 0), [usage]);
  const limit = plan?.planLimit ?? 1000;
  const ratio = limit > 0 ? used / limit : 0;

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
        <CardContent className="space-y-3">
          {loading ? (
            <Skeleton className="h-16 w-full" />
          ) : (
            <>
              <p className="text-3xl font-semibold tracking-tight">
                {used} <span className="text-base font-normal text-muted-foreground">/ {limit} requests</span>
              </p>
              <p className="text-sm text-muted-foreground capitalize">
                Plan: {plan?.plan || "free"} • {usageMessage()}
              </p>
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
          ) : usage.length === 0 ? (
            <EmptyState title="No usage yet" description="Start using the API to generate usage data." />
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
    </div>
  );
}
