"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { PasswordInput } from "@/components/ui/password-input";
import { Label } from "@/components/ui/label";
import { LoginFormSchema } from "@/lib/validators";
import { apiFetch, ApiError, setStoredOrgId, setStoredSessionToken } from "@/lib/api";
import { useToast } from "@/lib/toast";

export default function LoginPage() {
  const router = useRouter();
  const { push } = useToast();
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(false);

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setErrors({});

    const formData = new FormData(event.currentTarget);
    const parsed = LoginFormSchema.safeParse({
      email: String(formData.get("email") || ""),
      password: String(formData.get("password") || ""),
    });
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
      const data = await apiFetch<{ userId: string; orgId: string; sessionToken?: string }>("/auth/login", {
        method: "POST",
        skipOrgHeader: true,
        body: JSON.stringify(parsed.data),
      });
      setStoredOrgId(data.orgId);
      if (data.sessionToken) {
        setStoredSessionToken(data.sessionToken);
      }
      push({ title: "Welcome back", description: "Login successful." });
      router.push("/app");
    } catch (err) {
      const apiErr = err as ApiError;
      if (apiErr.field) {
        setErrors((prev) => ({ ...prev, [apiErr.field as string]: apiErr.message }));
      }
      push({
        title: "Login failed",
        description: apiErr.status === 401 ? "Invalid credentials" : apiErr.message,
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
            <CardTitle>Sign in</CardTitle>
          </CardHeader>
          <CardContent>
            <form className="space-y-4" onSubmit={submit} aria-label="Login form" autoComplete="on" method="post">
              <div className="space-y-2">
                <Label htmlFor="email">Email</Label>
                <Input
                  id="email"
                  name="email"
                  type="email"
                  autoComplete="email"
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
                <PasswordInput
                  id="password"
                  name="password"
                  autoComplete="current-password"
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
                {loading ? "Signing in..." : "Sign in"}
              </Button>
              <p className="text-sm text-muted-foreground">
                New here? <Link href="/register">Create an account</Link>
              </p>
            </form>
          </CardContent>
        </Card>
      </div>
    </main>
  );
}
