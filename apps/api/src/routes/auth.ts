import { FastifyInstance } from "fastify";
import bcrypt from "bcryptjs";
import { prisma } from "../lib/prisma";
import { AppError } from "../lib/errors";
import { randomToken, sha256 } from "../lib/crypto";
import { LoginSchema, RegisterSchema } from "@saas/shared";
import { Prisma } from "@saas/db";

const SESSION_DAYS = 7;

export async function authRoutes(app: FastifyInstance) {
  app.get("/me", async (request) => {
    const sessionToken = request.cookies["sid"] as string | undefined;
    if (!sessionToken) {
      throw new AppError("UNAUTHORIZED", 401, "Missing session");
    }

    const tokenHash = sha256(sessionToken);
    const session = await prisma.session.findUnique({
      where: { tokenHash },
      include: { user: true },
    });
    if (!session || session.expiresAt < new Date()) {
      throw new AppError("UNAUTHORIZED", 401, "Invalid session");
    }

    return {
      user: {
        id: session.user.id,
        email: session.user.email,
      },
    };
  });

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

    const membership = await prisma.membership.findFirst({
      where: { userId: user.id },
      orderBy: { createdAt: "asc" },
      select: { orgId: true },
    });
    if (!membership) {
      throw new AppError("FORBIDDEN", 403, "No organization membership found");
    }

    const sessionToken = await createSession(user.id);
    setSessionCookie(reply, sessionToken, app);

    reply.send({ userId: user.id, orgId: membership.orgId });
  });

  app.post("/logout", async (request, reply) => {
    try {
      const sessionToken = request.cookies["sid"] as string | undefined;
      if (sessionToken) {
        try {
          const tokenHash = sha256(sessionToken);
          await prisma.session.deleteMany({ where: { tokenHash } });
        } catch (error) {
          request.log.error({ err: error }, "Failed to revoke session during logout");
        }
      }
    } catch (error) {
      request.log.error({ err: error }, "Logout handler failure");
    } finally {
      try {
        reply.clearCookie("sid", {
          path: "/",
          httpOnly: true,
          sameSite: "lax",
          secure: app.env.COOKIE_SECURE === "true",
          domain: app.env.COOKIE_DOMAIN || undefined,
        });
      } catch (error) {
        request.log.error({ err: error }, "Failed to clear session cookie during logout");
      }
      reply.send({ ok: true });
    }
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
