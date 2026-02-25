import { prisma } from "../lib/prisma";

export function listByOrg(orgId: string) {
  return prisma.apiKey.findMany({
    where: { orgId },
    orderBy: { createdAt: "desc" },
  });
}

export function createApiKey(orgId: string, name: string, prefix: string, hash: string) {
  return prisma.apiKey.create({
    data: { orgId, name, prefix, hash },
  });
}

export async function revokeApiKey(orgId: string, id: string) {
  return prisma.apiKey.updateMany({
    where: { id, orgId },
    data: { revokedAt: new Date() },
  });
}

export function findById(orgId: string, id: string) {
  return prisma.apiKey.findFirst({ where: { id, orgId } });
}
