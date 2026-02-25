import { prisma } from "../lib/prisma";
import { TenantContext } from "../lib/tenant";
import { Prisma } from "@saas/db";

export async function writeAudit(
  ctx: TenantContext,
  action: string,
  targetType: string,
  targetId?: string,
  metadata?: Prisma.InputJsonValue
) {
  if (!ctx.orgId) {
    return;
  }
  if (!ctx.userId && !ctx.apiKeyId) {
    return;
  }

  const baseMetadata: Prisma.JsonObject = {
    ...(isObject(metadata) ? metadata : {}),
    requestId: ctx.requestId,
    ip: ctx.ip,
    userAgent: ctx.userAgent,
  };
  if (ctx.apiKeyId) {
    baseMetadata["actorApiKeyId"] = ctx.apiKeyId;
  }

  await prisma.auditLog.create({
    data: {
      orgId: ctx.orgId,
      actorUserId: ctx.userId,
      action,
      targetType,
      targetId,
      metadata: Object.keys(baseMetadata).length > 0 ? (baseMetadata as Prisma.InputJsonValue) : undefined,
    },
  });
}

function isObject(value: Prisma.InputJsonValue | undefined): value is Prisma.JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
