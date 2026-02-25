import { FastifyInstance } from "fastify";
import { prisma } from "../lib/prisma";
import { requireRole, requireUser } from "../middleware/auth";
import { enforceUsageLimit } from "../middleware/usage";

export async function usageRoutes(app: FastifyInstance) {
  app.get("/", { preHandler: [enforceUsageLimit, requireUser, (req) => requireRole(req, ["OWNER", "ADMIN", "BILLING"]) ] }, async (request) => {
    const days = Number((request.query as { days?: string })?.days ?? 30);
    const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    const daily = await prisma.usageDaily.findMany({
      where: { orgId: request.auth!.orgId, date: { gte: since } },
      orderBy: { date: "desc" },
    });

    return daily;
  });
}
