import { prisma } from "../lib/prisma";

export function getById(id: string) {
  return prisma.organization.findUnique({ where: { id } });
}

export function updatePlan(id: string, plan: string, planLimit: number) {
  return prisma.organization.update({
    where: { id },
    data: { plan, planLimit },
  });
}
