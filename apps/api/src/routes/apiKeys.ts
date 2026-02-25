import { FastifyInstance } from "fastify";
import { requireRole, requireUser } from "../middleware/auth";
import { enforceUsageLimit } from "../middleware/usage";
import { ApiKeyCreateSchema } from "@saas/shared";
import { createApiKey, listApiKeys, revokeApiKey } from "../services/apiKeys";
import { ApiKey } from "@saas/db";

export async function apiKeyRoutes(app: FastifyInstance) {
  app.get("/", { preHandler: [enforceUsageLimit, requireUser, (req) => requireRole(req, ["OWNER", "ADMIN", "BILLING"]) ] }, async (request) => {
    const keys = await listApiKeys({
      orgId: request.auth!.orgId,
      userId: request.auth!.userId,
      role: request.auth!.role,
      requestId: request.id,
      ip: request.ip,
      userAgent: readUserAgent(request.headers["user-agent"]),
    });

    return keys.map((key: ApiKey) => ({
      id: key.id,
      name: key.name,
      prefix: key.prefix,
      revokedAt: key.revokedAt,
      lastUsedAt: key.lastUsedAt,
      createdAt: key.createdAt,
    }));
  });

  app.post("/", { preHandler: [enforceUsageLimit, requireUser, (req) => requireRole(req, ["OWNER", "ADMIN", "BILLING"]) ] }, async (request, reply) => {
    const input = ApiKeyCreateSchema.parse(request.body);
    const { apiKey, secret } = await createApiKey(
      {
        orgId: request.auth!.orgId,
        userId: request.auth!.userId,
        role: request.auth!.role,
        requestId: request.id,
        ip: request.ip,
        userAgent: readUserAgent(request.headers["user-agent"]),
      },
      input.name
    );

    reply.status(201).send({
      id: apiKey.id,
      name: apiKey.name,
      prefix: apiKey.prefix,
      secret,
      createdAt: apiKey.createdAt,
    });
  });

  app.delete("/:id", { preHandler: [enforceUsageLimit, requireUser, (req) => requireRole(req, ["OWNER", "ADMIN", "BILLING"]) ] }, async (request) => {
    const { id } = request.params as { id: string };
    await revokeApiKey(
      {
        orgId: request.auth!.orgId,
        userId: request.auth!.userId,
        role: request.auth!.role,
        requestId: request.id,
        ip: request.ip,
        userAgent: readUserAgent(request.headers["user-agent"]),
      },
      id
    );

    return { ok: true };
  });
}

function readUserAgent(value: string | string[] | undefined) {
  if (Array.isArray(value)) {
    return value[0];
  }
  return value;
}
