import { FastifyInstance } from "fastify";
import { requireApiKey } from "../middleware/apiKeyAuth";
import { enforceUsageLimit } from "../middleware/usage";
import { prisma } from "../lib/prisma";

export async function dataRoutes(app: FastifyInstance) {
  app.get("/", { preHandler: [enforceUsageLimit, requireApiKey] }, async (request) => {
    const org = await prisma.organization.findUnique({ where: { id: request.apiKey!.orgId } });
    return { orgId: org?.id, name: org?.name };
  });
}
