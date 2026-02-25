"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { RegisterFormSchema } from "@/lib/validators";
import { apiFetch, ApiError } from "@/lib/api";
import { useToast } from "@/lib/toast";

export default function RegisterPage() {
  const router = useRouter();
  const { push } = useToast();
  const [values, setValues] = useState({ email: "", password: "", orgName: "" });
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(false);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setErrors({});

    const parsed = RegisterFormSchema.safeParse(values);
    if (!parsed.success) {
      const nextErrors: Record<string, string> = {};
      parsed.error.issues.forEach((issue) => {
        const key = issue.path[0] as string;
        nextErrors[key] = issue.message;
      });
      setErrors(nextErrors);
      return;
    }

    setLoading(true);
    try {
      const data = await apiFetch<{ orgId: string; userId: string }>("/auth/register", {
        method: "POST",
        skipOrgHeader: true,
        body: JSON.stringify(values),
      });
      if (typeof window !== "undefined") {
        window.localStorage.setItem("orgId", data.orgId);
      }
      push({ title: "Account created", description: "Welcome to your new org." });
      router.push("/app");
    } catch (err) {
      const apiErr = err as ApiError;
      push({
        title: "Registration failed",
        description: apiErr.message,
        variant: "destructive",
      });
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="min-h-screen bg-background">
      <div className="container-page py-16">
        <Card className="mx-auto max-w-md">
          <CardHeader>
            <CardTitle>Create an account</CardTitle>
          </CardHeader>
          <CardContent>
            <form className="space-y-4" onSubmit={submit} aria-label="Register form">
              <div className="space-y-2">
                <Label htmlFor="orgName">Organization name</Label>
                <Input
                  id="orgName"
                  value={values.orgName}
                  onChange={(e) => {
                    const value = e.currentTarget.value;
                    setValues((prev) => ({ ...prev, orgName: value }));
                  }}
                  aria-invalid={Boolean(errors["orgName"])}
                  aria-describedby={errors["orgName"] ? "org-error" : undefined}
                  required
                />
                {errors["orgName"] && (
                  <p id="org-error" className="text-xs text-red-600">
                    {errors["orgName"]}
                  </p>
                )}
              </div>
              <div className="space-y-2">
                <Label htmlFor="email">Email</Label>
                <Input
                  id="email"
                  type="email"
                  value={values.email}
                  onChange={(e) => {
                    const value = e.currentTarget.value;
                    setValues((prev) => ({ ...prev, email: value }));
                  }}
                  aria-invalid={Boolean(errors["email"])}
                  aria-describedby={errors["email"] ? "email-error" : undefined}
                  required
                />
                {errors["email"] && (
                  <p id="email-error" className="text-xs text-red-600">
                    {errors["email"]}
                  </p>
                )}
              </div>
              <div className="space-y-2">
                <Label htmlFor="password">Password</Label>
                <Input
                  id="password"
                  type="password"
                  value={values.password}
                  onChange={(e) => {
                    const value = e.currentTarget.value;
                    setValues((prev) => ({ ...prev, password: value }));
                  }}
                  aria-invalid={Boolean(errors["password"])}
                  aria-describedby={errors["password"] ? "password-error" : undefined}
                  required
                />
                {errors["password"] && (
                  <p id="password-error" className="text-xs text-red-600">
                    {errors["password"]}
                  </p>
                )}
              </div>
              <Button type="submit" className="w-full" disabled={loading} aria-busy={loading}>
                {loading ? "Creating..." : "Create account"}
              </Button>
              <p className="text-sm text-muted-foreground">
                Already have an account? <Link href="/login">Sign in</Link>
              </p>
            </form>
          </CardContent>
        </Card>
      </div>
    </main>
  );
}
