import { FastifyInstance } from "fastify";
import { prisma } from "../lib/prisma";
import { requireRole, requireUser } from "../middleware/auth";
import { enforceUsageLimit } from "../middleware/usage";
import { z } from "zod";

const AuditQuerySchema = z.object({
  limit: z.coerce.number().int().min(1).max(100).default(100),
  cursor: z.string().optional(),
});

export async function auditRoutes(app: FastifyInstance) {
  app.get("/", { preHandler: [enforceUsageLimit, requireUser, (req) => requireRole(req, ["OWNER", "ADMIN"]) ] }, async (request) => {
    const query = AuditQuerySchema.parse(request.query || {});

    const logs = await prisma.auditLog.findMany({
      where: { orgId: request.auth!.orgId },
      orderBy: { createdAt: "desc" },
      ...(query.cursor ? { cursor: { id: query.cursor }, skip: 1 } : {}),
      take: query.limit + 1,
    });

    const hasMore = logs.length > query.limit;
    const items = hasMore ? logs.slice(0, query.limit) : logs;
    const nextCursor = hasMore ? items[items.length - 1]?.id : null;

    return { items, nextCursor };
  });
}
