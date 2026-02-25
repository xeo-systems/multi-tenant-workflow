import { prisma } from "./prisma";
import { pingRedis } from "./queue";

export async function checkDbReady() {
  await prisma.$queryRaw`SELECT 1`;
  return true;
}

export async function checkRedisReady() {
  return pingRedis();
}
