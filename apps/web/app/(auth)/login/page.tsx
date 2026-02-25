"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { LoginFormSchema } from "@/lib/validators";
import { apiFetch, ApiError } from "@/lib/api";
import { useToast } from "@/lib/toast";

export default function LoginPage() {
  const router = useRouter();
  const { push } = useToast();
  const [values, setValues] = useState({ email: "", password: "" });
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(false);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setErrors({});

    const parsed = LoginFormSchema.safeParse(values);
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
      await apiFetch("/auth/login", { method: "POST", body: JSON.stringify(values) });
      push({ title: "Welcome back", description: "Login successful." });
      router.push("/app");
    } catch (err) {
      const apiErr = err as ApiError;
      push({
        title: "Login failed",
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
            <CardTitle>Sign in</CardTitle>
          </CardHeader>
          <CardContent>
            <form className="space-y-4" onSubmit={submit} aria-label="Login form">
              <div className="space-y-2">
                <Label htmlFor="email">Email</Label>
                <Input
                  id="email"
                  type="email"
                  value={values.email}
                  onChange={(e) => setValues((prev) => ({ ...prev, email: e.currentTarget.value }))}
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
                  onChange={(e) => setValues((prev) => ({ ...prev, password: e.currentTarget.value }))}
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
