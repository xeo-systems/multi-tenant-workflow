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
  await prisma.auditLog.create({
    data: {
      orgId: ctx.orgId,
      actorUserId: ctx.userId,
      action,
      targetType,
      targetId,
      metadata,
    },
  });
}
