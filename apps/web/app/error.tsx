"use client";

import { Button } from "@/components/ui/button";

export default function Error({ reset }: { reset: () => void }) {
  return (
    <main className="min-h-screen bg-background">
      <div className="container-page py-16">
        <div className="card space-y-3">
          <h1 className="text-2xl font-semibold">Something went wrong</h1>
          <p className="text-sm text-muted-foreground">Try again or return to the dashboard.</p>
          <div className="flex gap-3">
            <Button onClick={() => reset()}>Try again</Button>
            <Button variant="outline" onClick={() => (window.location.href = "/app")}>Go to dashboard</Button>
          </div>
        </div>
      </div>
    </main>
  );
}
