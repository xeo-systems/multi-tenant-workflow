import { Queue, Worker, JobsOptions } from "bullmq";
import { config as loadDotenv } from "dotenv";
import fs from "fs";
import path from "path";
import { prisma } from "@saas/db";
import Stripe from "stripe";
import { z } from "zod";

function loadEnvFile() {
  let dir = process.cwd();
  for (let i = 0; i < 6; i += 1) {
    const candidate = path.join(dir, ".env");
    if (fs.existsSync(candidate)) {
      loadDotenv({ path: candidate });
      return;
    }
    const parent = path.dirname(dir);
    if (parent === dir) {
      return;
    }
    dir = parent;
  }
}

loadEnvFile();

const EnvSchema = z.object({
  REDIS_URL: z.string().url(),
  STRIPE_SECRET_KEY: z.string().optional().nullable(),
  WORKER_CONCURRENCY: z.string().default("5"),
});

const env = EnvSchema.parse(process.env);

const redisUrl = new URL(env.REDIS_URL);
const connection = {
  host: redisUrl.hostname,
  port: Number(redisUrl.port || "6379"),
  password: redisUrl.password || undefined,
};

const stripeQueue = new Queue("stripe-events", { connection });
const usageQueue = new Queue("usage-rollups", { connection });

const concurrency = Number(env.WORKER_CONCURRENCY || 5);

const stripeWorker = new Worker(
  "stripe-events",
  async (job) => {
    const event = job.data.event as Stripe.Event;
    await handleStripeEvent(event);
  },
  { connection, concurrency }
);

const usageWorker = new Worker(
  "usage-rollups",
  async (job) => {
    const date = job.data.date ? new Date(job.data.date) : new Date(Date.now() - 24 * 60 * 60 * 1000);
    await rollupUsage(date);
  },
  { connection, concurrency: 2 }
);

void ensureRepeatable();

async function ensureRepeatable() {
  const opts: JobsOptions = {
    repeat: { pattern: "0 2 * * *" },
    jobId: "daily-usage-rollup",
  };
  await usageQueue.add("rollup", { }, opts);
}

async function handleStripeEvent(event: Stripe.Event) {
  switch (event.type) {
    case "checkout.session.completed":
      await handleCheckoutCompleted(event);
      return;
    case "customer.subscription.updated":
    case "customer.subscription.deleted":
      await handleSubscriptionUpdated(event);
      return;
    default:
      return;
  }
}

async function handleCheckoutCompleted(event: Stripe.Event) {
  const session = event.data.object as Stripe.Checkout.Session;
  const orgId = session.metadata?.["orgId"];
  const subscriptionId = session.subscription as string | null;
  const customerId = session.customer as string | null;

  if (!orgId || !subscriptionId || !customerId) {
    return;
  }

  await prisma.subscription.upsert({
    where: { stripeSubscriptionId: subscriptionId },
    create: {
      orgId,
      stripeCustomerId: customerId,
      stripeSubscriptionId: subscriptionId,
      status: "active",
      currentPeriodEnd: session.expires_at ? new Date(session.expires_at * 1000) : null,
    },
    update: {
      stripeCustomerId: customerId,
      status: "active",
    },
  });

  await writeSystemAudit(orgId, "billing.webhook.subscription.created", "subscription", subscriptionId, {
    stripeEventId: event.id,
    type: event.type,
  });
}

async function handleSubscriptionUpdated(event: Stripe.Event) {
  const subscription = event.data.object as Stripe.Subscription;
  const subscriptionId = subscription.id;
  const customerId = subscription.customer as string;
  const status = subscription.status as any;
  const currentPeriodStart = subscription.current_period_start
    ? new Date(subscription.current_period_start * 1000)
    : null;
  const currentPeriodEnd = subscription.current_period_end ? new Date(subscription.current_period_end * 1000) : null;

  const existing = await prisma.subscription.findUnique({ where: { stripeSubscriptionId: subscriptionId } });
  if (!existing) {
    return;
  }

  await prisma.subscription.update({
    where: { stripeSubscriptionId: subscriptionId },
    data: {
      stripeCustomerId: customerId,
      status,
      currentPeriodStart,
      currentPeriodEnd,
    },
  });

  const action = event.type === "customer.subscription.deleted"
    ? "billing.webhook.subscription.canceled"
    : "billing.webhook.subscription.updated";
  await writeSystemAudit(existing.orgId, action, "subscription", existing.id, {
    stripeEventId: event.id,
    stripeSubscriptionId: subscriptionId,
    type: event.type,
  });
}

async function rollupUsage(date: Date) {
  const bucket = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));

  const rows = await prisma.$queryRaw<Array<{ orgId: string; metric: string; quantity: number }>>`
    SELECT "orgId", metric, SUM(quantity)::int as quantity
    FROM "UsageEvent"
    WHERE ts >= ${bucket} AND ts < ${new Date(bucket.getTime() + 24 * 60 * 60 * 1000)}
    GROUP BY "orgId", metric
  `;

  for (const row of rows) {
    await prisma.usageDaily.upsert({
      where: { orgId_metric_date: { orgId: row.orgId, metric: row.metric, date: bucket } },
      create: { orgId: row.orgId, metric: row.metric, date: bucket, quantity: row.quantity },
      update: { quantity: row.quantity },
    });
  }
}

async function writeSystemAudit(
  orgId: string,
  action: string,
  targetType: string,
  targetId: string,
  metadata: Record<string, unknown>
) {
  await prisma.auditLog.create({
    data: {
      orgId,
      action,
      targetType,
      targetId,
      metadata: {
        ...metadata,
        actorApiKeyId: "system",
      },
    },
  });
}

const shutdownSignals = ["SIGTERM", "SIGINT"] as const;
shutdownSignals.forEach((signal) => {
  process.on(signal, async () => {
    try {
      await Promise.allSettled([
        stripeWorker.close(),
        usageWorker.close(),
        stripeQueue.close(),
        usageQueue.close(),
      ]);
      await prisma.$disconnect();
      process.exit(0);
    } catch (error) {
      console.error("Worker shutdown failed", error);
      process.exit(1);
    }
  });
});
