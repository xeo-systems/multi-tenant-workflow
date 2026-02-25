import { FastifyRequest } from "fastify";
import { prisma } from "../lib/prisma";
import { prefixFromKey, sha256 } from "../lib/crypto";
import { AppError } from "../lib/errors";
import { getOrgId } from "./auth";
import crypto from "crypto";
import { enforceRateLimit } from "./rateLimit";

export async function requireApiKey(request: FastifyRequest) {
  const auth = request.headers.authorization;
  if (!auth || !auth.startsWith("Bearer ")) {
    request.log.warn({ reason: "missing_api_key" }, "API key auth failed");
    throw new AppError("UNAUTHORIZED", 401, "Missing API key");
  }
  const key = auth.replace("Bearer ", "").trim();
  const prefix = prefixFromKey(key);
  const hash = sha256(key);

  const orgId = getOrgId(request);
  const candidates = await prisma.apiKey.findMany({
    where: { prefix, revokedAt: null, orgId },
  });

  const hashBuf = Buffer.from(hash);
  const apiKey = candidates.find((candidate) => {
    const candidateBuf = Buffer.from(candidate.hash);
    if (candidateBuf.length !== hashBuf.length) {
      return false;
    }
    return crypto.timingSafeEqual(candidateBuf, hashBuf);
  });

  if (!apiKey) {
    request.log.warn({ reason: "invalid_api_key", orgId, prefix }, "API key auth failed");
    throw new AppError("UNAUTHORIZED", 401, "Invalid API key");
  }

  request.apiKey = { id: apiKey.id, orgId: apiKey.orgId };
  request.log = request.log.child({ orgId: apiKey.orgId, apiKeyId: apiKey.id });

  await prisma.apiKey.update({
    where: { id: apiKey.id },
    data: { lastUsedAt: new Date() },
  });

  await enforceApiKeyRateLimit(apiKey.id, apiKey.orgId, request);
}

async function enforceApiKeyRateLimit(apiKeyId: string, orgId: string, request: FastifyRequest) {
  const limit = Number(request.server.env.API_KEY_RATE_LIMIT || 1000);
  const windowSec = Number(request.server.env.API_KEY_RATE_LIMIT_WINDOW_SEC || "60");
  enforceRateLimit(request, {
    scope: "api_key",
    keyParts: [apiKeyId, orgId],
    limit,
    windowSec,
    message: "API key rate limit exceeded",
  });
}

declare module "fastify" {
  interface FastifyRequest {
    apiKey?: { id: string; orgId: string };
  }
}
