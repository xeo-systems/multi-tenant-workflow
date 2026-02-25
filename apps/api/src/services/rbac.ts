import { Role } from "@saas/db";
import { prisma } from "../lib/prisma";
import { AppError } from "../lib/errors";
import { TenantContext } from "../lib/tenant";

export async function requireOrgRole(ctx: TenantContext, allowed: Role[]) {
  const role = await resolveRole(ctx);
  if (!allowed.includes(role)) {
    throw new AppError("FORBIDDEN", 403, "Insufficient role");
  }
}

async function resolveRole(ctx: TenantContext): Promise<Role> {
  if (ctx.role) {
    return ctx.role;
  }
  if (!ctx.userId) {
    throw new AppError("FORBIDDEN", 403, "User context required");
  }

  const membership = await prisma.membership.findUnique({
    where: { orgId_userId: { orgId: ctx.orgId, userId: ctx.userId } },
    select: { role: true },
  });
  if (!membership) {
    throw new AppError("FORBIDDEN", 403, "Not a member of this organization");
  }
  return membership.role;
}
