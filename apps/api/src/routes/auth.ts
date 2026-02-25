import { FastifyInstance } from "fastify";
import bcrypt from "bcryptjs";
import { prisma } from "../lib/prisma";
import { AppError } from "../lib/errors";
import { randomToken, sha256 } from "../lib/crypto";
import { LoginSchema, RegisterSchema } from "@saas/shared";
import { Prisma } from "@saas/db";

const SESSION_DAYS = 7;

export async function authRoutes(app: FastifyInstance) {
  app.post("/register", async (request, reply) => {
    const input = RegisterSchema.parse(request.body);
    const existing = await prisma.user.findUnique({ where: { email: input.email } });
    if (existing) {
      throw new AppError("CONFLICT", 409, "Email already registered");
    }

    const passwordHash = await bcrypt.hash(input.password, 12);

    const { org, user } = await prisma.$transaction(async (tx: Prisma.TransactionClient) => {
      const org = await tx.organization.create({
        data: { name: input.orgName, plan: "free", planLimit: 1000 },
      });

      const user = await tx.user.create({
        data: { email: input.email, passwordHash },
      });

      await tx.membership.create({
        data: { orgId: org.id, userId: user.id, role: "OWNER" },
      });

      await tx.auditLog.create({
        data: {
          orgId: org.id,
          actorUserId: user.id,
          action: "auth.register",
          targetType: "user",
          targetId: user.id,
        },
      });

      return { org, user };
    });

    const sessionToken = await createSession(user.id);
    setSessionCookie(reply, sessionToken, app);

    reply.status(201).send({ orgId: org.id, userId: user.id });
  });

  app.post("/login", async (request, reply) => {
    const input = LoginSchema.parse(request.body);
    const user = await prisma.user.findUnique({ where: { email: input.email } });
    if (!user) {
      throw new AppError("UNAUTHORIZED", 401, "Invalid credentials");
    }

    const match = await bcrypt.compare(input.password, user.passwordHash);
    if (!match) {
      throw new AppError("UNAUTHORIZED", 401, "Invalid credentials");
    }

    const sessionToken = await createSession(user.id);
    setSessionCookie(reply, sessionToken, app);

    reply.send({ userId: user.id });
  });

  app.post("/logout", async (request, reply) => {
    const sessionToken = request.cookies["sid"] as string | undefined;
    if (sessionToken) {
      const tokenHash = sha256(sessionToken);
      await prisma.session.deleteMany({ where: { tokenHash } });
    }
    reply.clearCookie("sid");
    reply.send({ ok: true });
  });
}

async function createSession(userId: string) {
  const sessionToken = randomToken(32);
  const tokenHash = sha256(sessionToken);
  const expiresAt = new Date(Date.now() + SESSION_DAYS * 24 * 60 * 60 * 1000);

  await prisma.session.create({
    data: {
      userId,
      tokenHash,
      expiresAt,
    },
  });

  return sessionToken;
}

function setSessionCookie(reply: any, token: string, app: FastifyInstance) {
  const secure = app.env.COOKIE_SECURE === "true";
  reply.setCookie("sid", token, {
    path: "/",
    httpOnly: true,
    sameSite: "lax",
    secure,
    domain: app.env.COOKIE_DOMAIN || undefined,
    maxAge: SESSION_DAYS * 24 * 60 * 60,
  });
}
