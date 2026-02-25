"use client";

import { useEffect, useState } from "react";
import { apiFetch, ApiError } from "@/lib/api";
import { useToast } from "@/lib/toast";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export default function BillingPage() {
  const { push } = useToast();
  const [plan, setPlan] = useState<{ plan: string; planLimit: number } | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      setLoading(true);
      try {
        const data = await apiFetch<{ plan: string; planLimit: number }>("/admin/plan");
        setPlan(data);
      } catch (err) {
        const apiErr = err as ApiError;
        push({ title: "Failed to load plan", description: apiErr.message, variant: "destructive" });
      } finally {
        setLoading(false);
      }
    }
    load();
  }, [push]);

  async function startCheckout() {
    try {
      const data = await apiFetch<{ url: string }>("/billing/checkout", { method: "POST" });
      if (typeof window !== "undefined") {
        window.location.href = data.url;
      }
    } catch (err) {
      const apiErr = err as ApiError;
      push({ title: "Checkout failed", description: apiErr.message, variant: "destructive" });
    }
  }

  async function openPortal() {
    try {
      const data = await apiFetch<{ url: string }>("/billing/portal", { method: "POST" });
      if (typeof window !== "undefined") {
        window.location.href = data.url;
      }
    } catch (err) {
      const apiErr = err as ApiError;
      push({ title: "Portal failed", description: apiErr.message, variant: "destructive" });
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold">Billing</h1>
        <p className="text-sm text-muted-foreground">Manage your subscription and payment method.</p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Plan status</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          {loading ? (
            <p className="text-sm text-muted-foreground">Loading plan...</p>
          ) : plan ? (
            <div className="space-y-1">
              <p className="text-sm font-semibold">{plan.plan}</p>
              <p className="text-xs text-muted-foreground">Limit: {plan.planLimit} requests</p>
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">Plan data unavailable.</p>
          )}
          <div className="flex flex-wrap gap-3">
            <Button onClick={startCheckout}>Start checkout</Button>
            <Button variant="outline" onClick={openPortal}>
              Open billing portal
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
