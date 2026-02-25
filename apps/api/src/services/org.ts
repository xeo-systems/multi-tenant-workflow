import { AppError } from "../lib/errors";
import { TenantContext } from "../lib/tenant";
import { Role } from "@saas/db";
import { writeAudit } from "./audit";
import * as orgRepo from "../repo/orgRepo";
import * as membershipRepo from "../repo/membershipRepo";
import { prisma } from "../lib/prisma";

export async function getOrg(ctx: TenantContext) {
  const org = await orgRepo.getById(ctx.orgId);
  if (!org) {
    throw new AppError("NOT_FOUND", 404, "Organization not found");
  }
  return org;
}

export async function listMembers(ctx: TenantContext) {
  return membershipRepo.listByOrg(ctx.orgId);
}

export async function addMember(ctx: TenantContext, email: string, role: Role) {
  const user = await prisma.user.findUnique({ where: { email } });
  if (!user) {
    throw new AppError("NOT_FOUND", 404, "User not found");
  }

  const membership = await membershipRepo.createMembership(ctx.orgId, user.id, role);

  await writeAudit(ctx, "org.member.add", "membership", membership.id, { email, role });

  return membership;
}

export async function updateMember(ctx: TenantContext, memberId: string, role: Role) {
  const membership = await membershipRepo.findById(ctx.orgId, memberId);
  if (!membership) {
    throw new AppError("NOT_FOUND", 404, "Membership not found");
  }

  const updated = await membershipRepo.updateRole(ctx.orgId, memberId, role);
  if (!updated) {
    throw new AppError("NOT_FOUND", 404, "Membership not found");
  }

  await writeAudit(ctx, "org.member.update", "membership", updated.id, { role });

  return updated;
}

export async function removeMember(ctx: TenantContext, memberId: string) {
  const membership = await membershipRepo.findById(ctx.orgId, memberId);
  if (!membership) {
    throw new AppError("NOT_FOUND", 404, "Membership not found");
  }
  if (membership.role === "OWNER") {
    throw new AppError("BAD_REQUEST", 400, "Cannot remove owner");
  }

  await membershipRepo.remove(ctx.orgId, memberId);
  await writeAudit(ctx, "org.member.remove", "membership", memberId);
}
