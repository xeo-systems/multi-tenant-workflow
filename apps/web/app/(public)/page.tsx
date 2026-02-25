import Link from "next/link";

export const dynamic = "force-dynamic";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export default function LandingPage() {
  return (
    <main className="min-h-screen bg-background">
      <div className="container-page py-16">
        <div className="flex flex-col gap-10">
          <header className="flex flex-col gap-4">
            <p className="text-sm font-semibold uppercase tracking-wide text-muted-foreground">
              Senior Engineer Demo
            </p>
            <h1 className="text-4xl font-semibold leading-tight">
              Control Center for multi-tenant SaaS teams
            </h1>
            <p className="max-w-2xl text-base text-muted-foreground">
              A functional baseline UI that ties into the existing Fastify + Prisma backend. Manage members,
              API keys, usage, and billing with production-safe defaults.
            </p>
            <div className="flex flex-wrap gap-3">
              <Link
                href="/login"
                className="inline-flex items-center justify-center rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
              >
                Sign in
              </Link>
              <Link
                href="/register"
                className="inline-flex items-center justify-center rounded-md border bg-background px-4 py-2 text-sm font-medium hover:bg-muted"
              >
                Create account
              </Link>
            </div>
          </header>
          <section className="grid gap-6 md:grid-cols-3">
            {[
              {
                title: "Tenant-safe",
                body: "Org-scoped routes, API keys, and usage enforcement.",
              },
              {
                title: "Operational",
                body: "Stripe billing, audit logs, and clean error handling.",
              },
              {
                title: "Usable UI",
                body: "A modern app shell with practical workflows.",
              },
            ].map((item) => (
              <Card key={item.title}>
                <CardHeader>
                  <CardTitle>{item.title}</CardTitle>
                </CardHeader>
                <CardContent>
                  <p className="text-sm text-muted-foreground">{item.body}</p>
                </CardContent>
              </Card>
            ))}
          </section>
        </div>
      </div>
    </main>
  );
}
