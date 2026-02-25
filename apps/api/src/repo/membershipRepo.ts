import { prisma } from "../lib/prisma";
import { Role } from "@saas/db";

export function listByOrg(orgId: string) {
  return prisma.membership.findMany({
    where: { orgId },
    include: { user: true },
    orderBy: { createdAt: "asc" },
  });
}

export function createMembership(orgId: string, userId: string, role: Role) {
  return prisma.membership.create({
    data: { orgId, userId, role },
  });
}

export function findById(orgId: string, id: string) {
  return prisma.membership.findFirst({ where: { id, orgId } });
}

export async function updateRole(orgId: string, id: string, role: Role) {
  await prisma.membership.updateMany({ where: { id, orgId }, data: { role } });
  return prisma.membership.findFirst({ where: { id, orgId } });
}

export function remove(orgId: string, id: string) {
  return prisma.membership.deleteMany({ where: { id, orgId } });
}
