import { prisma } from "./index";
import bcrypt from "bcryptjs";

async function main() {
  const email = "owner@example.com";
  const password = "password123";

  const existing = await prisma.user.findUnique({ where: { email } });
  if (existing) {
    return;
  }

  const passwordHash = await bcrypt.hash(password, 12);
  const user = await prisma.user.create({
    data: {
      email,
      passwordHash,
    },
  });

  const org = await prisma.organization.create({
    data: {
      name: "Acme Inc",
      plan: "free",
      planLimit: 1000,
      memberships: {
        create: {
          userId: user.id,
          role: "OWNER",
        },
      },
    },
  });

  await prisma.auditLog.create({
    data: {
      orgId: org.id,
      actorUserId: user.id,
      action: "seed.create",
      targetType: "organization",
      targetId: org.id,
      metadata: { email },
    },
  });
}

main()
  .then(async () => {
    await prisma.$disconnect();
  })
  .catch(async (err) => {
    console.error(err);
    await prisma.$disconnect();
    process.exit(1);
  });
