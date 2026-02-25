"use client";

import { useEffect, useState } from "react";
import { apiFetch, ApiError } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { useToast } from "@/lib/toast";
import { useRouter } from "next/navigation";

type OrgResponse = {
  org: { id: string; name: string; plan: string; planLimit: number };
  role: string;
};

export default function DashboardPage() {
  const [data, setData] = useState<OrgResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const { push } = useToast();
  const router = useRouter();

  useEffect(() => {
    let mounted = true;
    async function load() {
      try {
        const response = await apiFetch<OrgResponse>("/org");
        if (mounted) {
          setData(response);
        }
      } catch (err) {
        const apiErr = err as ApiError;
        if (apiErr.status === 401) {
          router.push("/login");
          return;
        }
        if (apiErr.status === 403) {
          push({ title: "Access denied", description: apiErr.message, variant: "destructive" });
          return;
        }
        push({ title: "Unable to load org", description: apiErr.message, variant: "destructive" });
      } finally {
        if (mounted) setLoading(false);
      }
    }
    load();
    return () => {
      mounted = false;
    };
  }, [push, router]);

  if (loading) {
    return (
      <div className="grid gap-4 md:grid-cols-3">
        <Skeleton className="h-28" />
        <Skeleton className="h-28" />
        <Skeleton className="h-28" />
      </div>
    );
  }

  if (!data) {
    return <p className="text-sm text-muted-foreground">Set an org ID to view data.</p>;
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold">Welcome back</h1>
        <p className="text-sm text-muted-foreground">Organization overview and plan status.</p>
      </div>
      <div className="grid gap-4 md:grid-cols-3">
        <Card>
          <CardHeader>
            <CardTitle>Organization</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm font-semibold">{data.org.name}</p>
            <p className="text-xs text-muted-foreground">ID: {data.org.id}</p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Plan</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm font-semibold capitalize">{data.org.plan}</p>
            <p className="text-xs text-muted-foreground">Limit: {data.org.planLimit} requests</p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>Role</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-sm font-semibold">{data.role}</p>
            <p className="text-xs text-muted-foreground">Permissions scoped to your role.</p>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
