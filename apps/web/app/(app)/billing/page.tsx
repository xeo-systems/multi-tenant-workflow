"use client";

import { useEffect, useState } from "react";
import { apiFetch, ApiError } from "@/lib/api";
import { useToast } from "@/lib/toast";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";

type BillingStatus = {
  billingConfigured: boolean;
  plan: string;
  planLimit: number;
  subscription: {
    status: string;
    currentPeriodStart: string | null;
    currentPeriodEnd: string | null;
  } | null;
};

export default function BillingPage() {
  const { push } = useToast();
  const [status, setStatus] = useState<BillingStatus | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      setLoading(true);
      try {
        const data = await apiFetch<BillingStatus>("/billing/status");
        setStatus(data);
      } catch (err) {
        const apiErr = err as ApiError;
        push({ title: "Failed to load billing", description: apiErr.message, variant: "destructive" });
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
      push({ title: "Manage billing failed", description: apiErr.message, variant: "destructive" });
    }
  }

  return (
    <div className="page-shell">
      <div className="page-header">
        <h1 className="page-title">Billing</h1>
        <p className="page-description">Manage your subscription and payment method.</p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Subscription</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {loading ? (
            <Skeleton className="h-20 w-full" />
          ) : status ? (
            <>
              <div className="space-y-2">
                <p className="text-base font-semibold capitalize">Plan: {status.plan}</p>
                <p className="text-sm text-muted-foreground">Limit: {status.planLimit} requests</p>
                <p className="text-sm text-muted-foreground capitalize">
                  Subscription status: {status.subscription?.status || "none"}
                </p>
                <p className="text-sm text-muted-foreground">
                  Renewal date: {status.subscription?.currentPeriodEnd ? new Date(status.subscription.currentPeriodEnd).toLocaleDateString() : "Not available"}
                </p>
              </div>

              {!status.billingConfigured && (
                <p className="text-sm text-muted-foreground">
                  Billing not configured. Set Stripe environment variables on the API service.
                </p>
              )}

              <div className="flex flex-wrap gap-3">
                <Button onClick={openPortal} variant="outline" disabled={!status.billingConfigured}>
                  Manage billing
                </Button>
                <Button onClick={startCheckout} disabled={!status.billingConfigured}>
                  Upgrade
                </Button>
              </div>
            </>
          ) : (
            <p className="text-sm text-muted-foreground">Billing data unavailable.</p>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
