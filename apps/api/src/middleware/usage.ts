import { FastifyRequest } from "fastify";
import { prisma } from "../lib/prisma";
import { AppError } from "../lib/errors";
import { Prisma } from "@saas/db";

const METRIC = "api_requests";

export async function enforceUsageLimit(request: FastifyRequest) {
  // Billable usage is measured on API-key authenticated data-plane calls only.
  if (!request.apiKey?.id) {
    return;
  }
  const orgId = request.apiKey.orgId;

  await prisma.$transaction(async (tx) => {
    await tx.$executeRaw`SELECT pg_advisory_xact_lock(hashtext(${orgId}))`;

    const org = await tx.organization.findUnique({ where: { id: orgId } });
    if (!org) {
      throw new AppError("NOT_FOUND", 404, "Organization not found");
    }

    const cycleStart = await getBillingCycleStart(tx, orgId);
    const usedAgg = await tx.usageDaily.aggregate({
      where: { orgId, metric: METRIC, date: { gte: cycleStart } },
      _sum: { quantity: true },
    });
    const used = usedAgg._sum.quantity ?? 0;
    if (used >= org.planLimit) {
      throw new AppError("RATE_LIMIT", 429, "Usage limit exceeded");
    }

    const date = dayBucket(new Date());
    await tx.usageDaily.upsert({
      where: { orgId_metric_date: { orgId, metric: METRIC, date } },
      create: { orgId, metric: METRIC, date, quantity: 1 },
      update: { quantity: { increment: 1 } },
    });
  });

  request.usageReserved = true;
}

export async function recordUsage(request: FastifyRequest, reply: { statusCode: number }, quantity = 1) {
  if (!request.apiKey?.id) {
    return;
  }
  const orgId = request.apiKey.orgId;

  const date = dayBucket(new Date());

  if (request.usageReserved && reply.statusCode >= 500) {
    await prisma.usageDaily.updateMany({
      where: { orgId, metric: METRIC, date },
      data: { quantity: { decrement: quantity } },
    });
    return;
  }

  await prisma.usageEvent.create({
    data: {
      orgId,
      metric: METRIC,
      quantity,
      apiKeyId: request.apiKey.id,
    },
  });

  if (!request.usageReserved) {
    await prisma.usageDaily.upsert({
      where: { orgId_metric_date: { orgId, metric: METRIC, date } },
      create: { orgId, metric: METRIC, date, quantity },
      update: { quantity: { increment: quantity } },
    });
  }

  const keyMetric = `api_key:${request.apiKey.id}`;
  await prisma.usageDaily.upsert({
    where: { orgId_metric_date: { orgId, metric: keyMetric, date } },
    create: { orgId, metric: keyMetric, date, quantity },
    update: { quantity: { increment: quantity } },
  });
}

export function dayBucket(date: Date) {
  const utc = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  return utc;
}

async function getBillingCycleStart(tx: Prisma.TransactionClient, orgId: string) {
  const subscription = await tx.subscription.findFirst({
    where: { orgId },
    orderBy: { createdAt: "desc" },
  });

  if (subscription?.currentPeriodStart) {
    return dayBucket(subscription.currentPeriodStart);
  }

  return dayBucket(new Date());
}

declare module "fastify" {
  interface FastifyRequest {
    usageReserved?: boolean;
  }
}
