import { FastifyInstance } from "fastify";
import { prisma } from "../lib/prisma";
import { requireRole, requireUser } from "../middleware/auth";
import { enforceUsageLimit } from "../middleware/usage";
import { AppError } from "../lib/errors";
import { z } from "zod";

const PlanUpdateSchema = z.object({
  plan: z.string().min(2),
  planLimit: z.number().int().positive(),
});

export async function adminRoutes(app: FastifyInstance) {
  app.get("/plan", { preHandler: [enforceUsageLimit, requireUser, (req) => requireRole(req, ["OWNER", "ADMIN"]) ] }, async (request) => {
    const org = await prisma.organization.findUnique({ where: { id: request.auth!.orgId } });
    if (!org) {
      throw new AppError("NOT_FOUND", 404, "Organization not found");
    }
    return { plan: org.plan, planLimit: org.planLimit };
  });

  app.post("/plan", { preHandler: [enforceUsageLimit, requireUser, (req) => requireRole(req, ["OWNER"]) ] }, async (request) => {
    const input = PlanUpdateSchema.parse(request.body);
    const org = await prisma.organization.update({
      where: { id: request.auth!.orgId },
      data: { plan: input.plan, planLimit: input.planLimit },
    });

    await prisma.auditLog.create({
      data: {
        orgId: request.auth!.orgId,
        actorUserId: request.auth!.userId,
        action: "admin.plan.update",
        targetType: "organization",
        targetId: org.id,
        metadata: { plan: input.plan, planLimit: input.planLimit },
      },
    });

    return { plan: org.plan, planLimit: org.planLimit };
  });

  app.get("/audit", { preHandler: [enforceUsageLimit, requireUser, (req) => requireRole(req, ["OWNER", "ADMIN"]) ] }, async (request) => {
    const logs = await prisma.auditLog.findMany({
      where: { orgId: request.auth!.orgId },
      orderBy: { createdAt: "desc" },
      take: 100,
    });
    return logs;
  });

  app.get("/orgs/:orgId/summary", async (request) => {
    requireInternalAdmin(request.headers.authorization, app.env.INTERNAL_ADMIN_TOKEN);
    const { orgId } = request.params as { orgId: string };

    const org = await prisma.organization.findUnique({
      where: { id: orgId },
      select: { id: true, name: true, plan: true, planLimit: true },
    });
    if (!org) {
      throw new AppError("NOT_FOUND", 404, "Organization not found");
    }

    const memberCount = await prisma.membership.count({ where: { orgId } });
    const subscription = await prisma.subscription.findFirst({
      where: { orgId },
      orderBy: { createdAt: "desc" },
      select: {
        id: true,
        status: true,
        currentPeriodStart: true,
        currentPeriodEnd: true,
      },
    });

    const cycleStart = subscription?.currentPeriodStart || startOfUtcDay(new Date());
    const usage = await prisma.usageDaily.aggregate({
      where: {
        orgId,
        metric: "api_requests",
        date: { gte: startOfUtcDay(cycleStart) },
      },
      _sum: { quantity: true },
    });

    return {
      org,
      memberCount,
      subscription: subscription || null,
      usage: {
        used: usage._sum.quantity ?? 0,
        limit: org.planLimit,
        cycleStart: startOfUtcDay(cycleStart),
      },
    };
  });
}

function requireInternalAdmin(authHeader: string | string[] | undefined, expectedToken?: string | null) {
  if (!expectedToken) {
    throw new AppError("FORBIDDEN", 403, "Internal admin token not configured");
  }

  const header = Array.isArray(authHeader) ? authHeader[0] : authHeader;
  const token = header?.startsWith("Bearer ") ? header.slice("Bearer ".length).trim() : "";
  if (!token || token !== expectedToken) {
    throw new AppError("UNAUTHORIZED", 401, "Unauthorized");
  }
}

function startOfUtcDay(date: Date) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
}
