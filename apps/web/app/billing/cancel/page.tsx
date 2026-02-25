import Link from "next/link";

export default function BillingCancel() {
  return (
    <main className="min-h-screen bg-background">
      <div className="container-page py-16">
        <div className="card space-y-3">
          <h1 className="text-2xl font-semibold">Checkout canceled</h1>
          <p className="text-sm text-muted-foreground">You can retry checkout from billing.</p>
          <Link
            href="/app/billing"
            className="inline-flex items-center justify-center rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
          >
            Back to billing
          </Link>
        </div>
      </div>
    </main>
  );
}
