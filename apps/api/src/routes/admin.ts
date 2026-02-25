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
}
