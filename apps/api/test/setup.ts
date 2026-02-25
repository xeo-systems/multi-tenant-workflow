import { prisma } from "@saas/db";

const TABLES = [
  "AuditLog",
  "StripeEvent",
  "Subscription",
  "UsageDaily",
  "UsageEvent",
  "ApiKey",
  "Session",
  "Membership",
  "Organization",
  "User",
];

export async function resetDb() {
  const joined = TABLES.map((t) => `\"${t}\"`).join(", ");
  await prisma.$executeRawUnsafe(`TRUNCATE TABLE ${joined} CASCADE;`);
}

export async function closeDb() {
  await prisma.$disconnect();
}
