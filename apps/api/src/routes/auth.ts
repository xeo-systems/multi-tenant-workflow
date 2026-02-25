import { FastifyInstance, FastifyRequest } from "fastify";
import bcrypt from "bcryptjs";
import { prisma } from "../lib/prisma";
import { AppError } from "../lib/errors";
import { randomToken, sha256 } from "../lib/crypto";
import { LoginSchema, RegisterSchema } from "@saas/shared";
import { Prisma } from "@saas/db";
import { enforceRateLimit, normalizeIdentifier } from "../middleware/rateLimit";
import { writeAudit } from "../services/audit";

const SESSION_DAYS = 7;
const PASSWORD_COST = 12;
const DUMMY_BCRYPT_HASH = "$2a$12$Y7Qan/XVUPQQM4YVgjP7eOk7V/Wj34Y6PQxN4U2QjS4fRaI3hxLHS"; // hash for "dummy-password"

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
    ensureTrustedOrigin(request, app);
    enforceRegisterRateLimit(request);

    const input = RegisterSchema.parse(request.body);
    const existing = await prisma.user.findUnique({ where: { email: input.email } });
    if (existing) {
      throw new AppError("CONFLICT", 409, "Email already registered", { field: "email" });
    }

    const passwordHash = await bcrypt.hash(input.password, PASSWORD_COST);

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

      return { org, user };
    });

    await writeAudit(
      {
        orgId: org.id,
        userId: user.id,
        requestId: request.id,
        ip: request.ip,
        userAgent: readUserAgent(request.headers["user-agent"]),
      },
      "auth.register",
      "user",
      user.id
    );

    const sessionToken = await createSession(user.id);
    setSessionCookie(reply, sessionToken, app);

    reply.status(201).send({ orgId: org.id, userId: user.id });
  });

  app.post("/login", async (request, reply) => {
    ensureTrustedOrigin(request, app);
    enforceLoginRateLimit(request);

    const input = LoginSchema.parse(request.body);
    const user = await prisma.user.findUnique({ where: { email: input.email } });
    const passwordHash = user?.passwordHash || DUMMY_BCRYPT_HASH;
    const match = await bcrypt.compare(input.password, passwordHash);
    if (!user || !match) {
      if (user) {
        const membership = await prisma.membership.findFirst({
          where: { userId: user.id },
          orderBy: { createdAt: "asc" },
          select: { orgId: true },
        });
        if (membership) {
          await writeAudit(
            {
              orgId: membership.orgId,
              userId: user.id,
              requestId: request.id,
              ip: request.ip,
              userAgent: readUserAgent(request.headers["user-agent"]),
            },
            "auth.login.failure",
            "user",
            user.id
          );
        }
      }
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

    await writeAudit(
      {
        orgId: membership.orgId,
        userId: user.id,
        requestId: request.id,
        ip: request.ip,
        userAgent: readUserAgent(request.headers["user-agent"]),
      },
      "auth.login.success",
      "user",
      user.id
    );

    reply.send({ userId: user.id, orgId: membership.orgId });
  });

  app.post("/logout", async (request, reply) => {
    try {
      const sessionToken = request.cookies["sid"] as string | undefined;
      if (sessionToken) {
        try {
          const tokenHash = sha256(sessionToken);
          const session = await prisma.session.findUnique({
            where: { tokenHash },
            select: { userId: true },
          });
          await prisma.session.deleteMany({ where: { tokenHash } });
          const orgId = readOrgIdHeader(request);
          if (session?.userId && orgId) {
            const membership = await prisma.membership.findUnique({
              where: {
                orgId_userId: {
                  orgId,
                  userId: session.userId,
                },
              },
              select: { id: true },
            });
            if (membership) {
              await writeAudit(
                {
                  orgId,
                  userId: session.userId,
                  requestId: request.id,
                  ip: request.ip,
                  userAgent: readUserAgent(request.headers["user-agent"]),
                },
                "auth.logout",
                "session"
              );
            }
          }
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
          secure: shouldUseSecureCookie(app),
          domain: app.env.COOKIE_DOMAIN || undefined,
        });
      } catch (error) {
        request.log.error({ err: error }, "Failed to clear session cookie during logout");
      }
      reply.send({ ok: true });
    }
  });
}

function readOrgIdHeader(request: FastifyRequest): string | undefined {
  const header = request.headers["x-org-id"];
  if (Array.isArray(header)) {
    return header[0];
  }
  return header;
}

function readUserAgent(value: string | string[] | undefined) {
  if (Array.isArray(value)) {
    return value[0];
  }
  return value;
}

function enforceRegisterRateLimit(request: FastifyRequest) {
  const ip = request.ip || "unknown";
  const limit = Number(request.server.env.AUTH_REGISTER_IP_RATE_LIMIT || "5");
  const windowSec = Number(request.server.env.AUTH_RATE_LIMIT_WINDOW_SEC || "60");
  enforceRateLimit(request, {
    scope: "auth_register_ip",
    keyParts: [ip],
    limit,
    windowSec,
    message: "Too many registration attempts. Please try again later.",
  });
}

function enforceLoginRateLimit(request: FastifyRequest) {
  const ip = request.ip || "unknown";
  const body = (request.body || {}) as { email?: unknown };
  const identifier = normalizeIdentifier(body.email);
  const windowSec = Number(request.server.env.AUTH_RATE_LIMIT_WINDOW_SEC || "60");

  const ipLimit = Number(request.server.env.AUTH_LOGIN_IP_RATE_LIMIT || "10");
  enforceRateLimit(request, {
    scope: "auth_login_ip",
    keyParts: [ip],
    limit: ipLimit,
    windowSec,
    message: "Too many login attempts. Please try again later.",
  });

  const identifierLimit = Number(request.server.env.AUTH_LOGIN_IDENTIFIER_RATE_LIMIT || "5");
  enforceRateLimit(request, {
    scope: "auth_login_identifier",
    keyParts: [identifier],
    limit: identifierLimit,
    windowSec,
    message: "Too many login attempts. Please try again later.",
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
  const secure = shouldUseSecureCookie(app);
  reply.setCookie("sid", token, {
    path: "/",
    httpOnly: true,
    sameSite: "lax",
    secure,
    domain: app.env.COOKIE_DOMAIN || undefined,
    maxAge: SESSION_DAYS * 24 * 60 * 60,
  });
}

function shouldUseSecureCookie(app: FastifyInstance) {
  return app.env.NODE_ENV === "production" || app.env.COOKIE_SECURE === "true";
}

function ensureTrustedOrigin(request: FastifyRequest, app: FastifyInstance) {
  const originHeader = Array.isArray(request.headers.origin) ? request.headers.origin[0] : request.headers.origin;
  const hostHeader = Array.isArray(request.headers.host) ? request.headers.host[0] : request.headers.host;
  const trusted = new URL(app.env.WEB_BASE_URL);

  if (originHeader) {
    let parsed: URL;
    try {
      parsed = new URL(originHeader);
    } catch {
      throw new AppError("FORBIDDEN", 403, "Invalid request origin");
    }
    if (parsed.origin !== trusted.origin) {
      throw new AppError("FORBIDDEN", 403, "Invalid request origin");
    }
    return;
  }

  if (app.env.NODE_ENV === "production" && (!hostHeader || hostHeader !== trusted.host)) {
    throw new AppError("FORBIDDEN", 403, "Invalid request origin");
  }
}
