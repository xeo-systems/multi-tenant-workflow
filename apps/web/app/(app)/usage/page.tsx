"use client";

import { useEffect, useMemo, useState } from "react";
import { apiFetch, ApiError } from "@/lib/api";
import { useToast } from "@/lib/toast";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
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

export default function UsagePage() {
  const { push } = useToast();
  const [usage, setUsage] = useState<UsageDaily[]>([]);
  const [loading, setLoading] = useState(true);
  const [plan, setPlan] = useState<{ plan: string; planLimit: number } | null>(null);

  useEffect(() => {
    async function load() {
      setLoading(true);
      try {
        const data = await apiFetch<UsageDaily[]>("/usage?days=30");
        setUsage(data.filter((entry) => entry.metric === METRIC));
        try {
          const planData = await apiFetch<{ plan: string; planLimit: number }>("/admin/plan");
          setPlan(planData);
        } catch {
          setPlan(null);
        }
      } catch (err) {
        const apiErr = err as ApiError;
        push({ title: "Failed to load usage", description: apiErr.message, variant: "destructive" });
      } finally {
        setLoading(false);
      }
    }
    load();
  }, [push]);

  const max = useMemo(() => Math.max(...usage.map((u) => u.quantity), 1), [usage]);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold">Usage</h1>
        <p className="text-sm text-muted-foreground">Track daily API activity and plan limits.</p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Last 30 days</CardTitle>
        </CardHeader>
        <CardContent>
          {plan && (
            <p className="mb-4 text-sm text-muted-foreground">
              Plan {plan.plan} • Limit {plan.planLimit} requests per cycle
            </p>
          )}
          {loading ? (
            <p className="text-sm text-muted-foreground">Loading usage...</p>
          ) : usage.length === 0 ? (
            <EmptyState
              title="No usage yet"
              description="Start using the API to generate usage data."
            />
          ) : (
            <div className="space-y-4">
              <div className="grid gap-2">
                {usage.slice(0, 10).map((entry) => (
                  <div key={entry.id} className="flex items-center gap-3">
                    <span className="w-24 text-xs text-muted-foreground">
                      {new Date(entry.date).toLocaleDateString()}
                    </span>
                    <div className="h-2 flex-1 rounded bg-muted">
                      <div
                        className="h-2 rounded bg-primary"
                        style={{ width: `${(entry.quantity / max) * 100}%` }}
                      />
                    </div>
                    <span className="w-12 text-xs text-muted-foreground text-right">{entry.quantity}</span>
                  </div>
                ))}
              </div>
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
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
