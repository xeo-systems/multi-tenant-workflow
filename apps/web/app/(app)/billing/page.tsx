"use client";

import { useEffect, useState } from "react";
import { apiFetch, getApiErrorKind, toApiError } from "@/lib/api";
import { useToast } from "@/lib/toast";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { EmptyState } from "@/components/empty-state";

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
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [forbidden, setForbidden] = useState(false);
  const [retryNonce, setRetryNonce] = useState(0);

  useEffect(() => {
    async function load() {
      setLoading(true);
      setErrorMessage(null);
      setForbidden(false);
      try {
        const data = await apiFetch<BillingStatus>("/billing/status");
        setStatus(data);
      } catch (err) {
        const apiErr = toApiError(err, "Failed to load billing");
        const kind = getApiErrorKind(apiErr);
        if (kind === "forbidden") {
          setForbidden(true);
        } else {
          setErrorMessage(apiErr.message);
        }
        if (kind === "network") {
          push({ title: "Network error", description: "Could not reach billing API. Retry below.", variant: "destructive" });
        } else if (kind === "forbidden") {
          push({ title: "Permission denied", description: "You do not have access to billing for this organization.", variant: "destructive" });
        } else {
          push({ title: "Failed to load billing", description: apiErr.message, variant: "destructive" });
        }
      } finally {
        setLoading(false);
      }
    }
    load();
  }, [push, retryNonce]);

  async function startCheckout() {
    try {
      const data = await apiFetch<{ url: string }>("/billing/checkout", { method: "POST" });
      if (typeof window !== "undefined") {
        window.location.href = data.url;
      }
    } catch (err) {
      const apiErr = toApiError(err, "Checkout failed");
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
      const apiErr = toApiError(err, "Manage billing failed");
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
          ) : forbidden ? (
            <EmptyState
              title="Permissions required"
              description="Only OWNER, ADMIN, and BILLING roles can access billing."
            />
          ) : errorMessage ? (
            <EmptyState title="Billing unavailable" description={errorMessage} actionLabel="Retry" onAction={() => setRetryNonce((v) => v + 1)} />
          ) : status ? (
            <>
              <div className="space-y-2">
                <p className="text-base font-semibold capitalize">Plan: {status.plan}</p>
                <p className="text-sm text-muted-foreground">Limit: {status.planLimit} requests</p>
                <p className="text-sm text-muted-foreground capitalize">
                  Subscription status: {status.subscription?.status || "none"}
                </p>
                <p className="text-sm text-muted-foreground">
                  Renewal date:{" "}
                  {status.subscription?.currentPeriodEnd
                    ? new Date(status.subscription.currentPeriodEnd).toLocaleDateString()
                    : "Not available"}
                </p>
              </div>

              {!status.billingConfigured && (
                <div className="rounded-md border border-dashed p-4">
                  <p className="text-sm font-medium">Billing not configured</p>
                  <p className="mt-1 text-xs text-muted-foreground">
                    Stripe actions are disabled. Set <code>STRIPE_SECRET_KEY</code>, <code>STRIPE_WEBHOOK_SECRET</code>,{" "}
                    <code>STRIPE_PRICE_ID</code>, <code>STRIPE_SUCCESS_URL</code>, <code>STRIPE_CANCEL_URL</code>, and{" "}
                    <code>STRIPE_PORTAL_RETURN_URL</code> in the API environment.
                  </p>
                </div>
              )}

              <div className="flex flex-wrap gap-4">
                <Button onClick={openPortal} variant="outline" disabled={!status.billingConfigured}>
                  Manage billing
                </Button>
                <Button onClick={startCheckout} disabled={!status.billingConfigured}>
                  Upgrade
                </Button>
                <Button variant="ghost" onClick={() => setRetryNonce((v) => v + 1)}>
                  Refresh
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
