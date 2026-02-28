import { FastifyInstance, FastifyRequest } from "fastify";
import { prisma } from "../lib/prisma";
import { AppError } from "../lib/errors";
import { randomToken, sha256 } from "../lib/crypto";
import { LoginSchema, RegisterSchema } from "@saas/shared";
import { enforceRateLimit, normalizeIdentifier } from "../middleware/rateLimit";
import { writeAudit } from "../services/audit";
import { createAuthProvider } from "../services/authProvider";
import { isDemoMode } from "../lib/demo";
import { readSessionToken } from "../middleware/auth";

export async function authRoutes(app: FastifyInstance) {
  const authProvider = createAuthProvider(app.env.AUTH_PROVIDER);
  app.get("/me", async (request) => {
    const sessionToken = readSessionToken(request);
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

    const membership = await prisma.membership.findFirst({
      where: { userId: session.user.id },
      orderBy: { createdAt: "asc" },
      select: { orgId: true },
    });

    return {
      user: {
        id: session.user.id,
        email: session.user.email,
      },
      orgId: membership?.orgId || null,
    };
  });

  app.post("/register", async (request, reply) => {
    ensureTrustedOrigin(request, app);
    enforceRegisterRateLimit(request);

    const input = RegisterSchema.parse(request.body);
    const { orgId, userId } = await authProvider.register(input);

    await writeAudit(
      {
        orgId,
        userId,
        requestId: request.id,
        ip: request.ip,
        userAgent: readUserAgent(request.headers["user-agent"]),
      },
      "auth.register",
      "user",
      userId
    );

    const sessionToken = await createSession(userId, app);
    setSessionCookie(reply, sessionToken, app);

    reply.status(201).send({ orgId, userId, sessionToken });
  });

  app.post("/login", async (request, reply) => {
    ensureTrustedOrigin(request, app);
    enforceLoginRateLimit(request);

    const input = LoginSchema.parse(request.body);
    const user = await prisma.user.findUnique({ where: { email: input.email } });
    try {
      const result = await authProvider.login(input);
      const sessionToken = await createSession(result.userId, app);
      setSessionCookie(reply, sessionToken, app);

      await writeAudit(
        {
          orgId: result.orgId,
          userId: result.userId,
          requestId: request.id,
          ip: request.ip,
          userAgent: readUserAgent(request.headers["user-agent"]),
        },
        "auth.login.success",
        "user",
        result.userId
      );

      reply.send({ userId: result.userId, orgId: result.orgId, sessionToken });
      return;
    } catch (error) {
      if (!(error instanceof AppError) || error.code !== "UNAUTHORIZED") {
        throw error;
      }
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
      throw error;
    }
  });

  app.post("/logout", async (request, reply) => {
    try {
      const sessionToken = readSessionToken(request);
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
          sameSite: sessionSameSite(app),
          secure: shouldUseSecureCookie(app),
          domain: app.env.COOKIE_DOMAIN || undefined,
        });
      } catch (error) {
        request.log.error({ err: error }, "Failed to clear session cookie during logout");
      }
      reply.send({ ok: true });
    }
  });

  app.post("/logout-all", async (request, reply) => {
    const sessionToken = readSessionToken(request);
    if (!sessionToken) {
      throw new AppError("UNAUTHORIZED", 401, "Missing session");
    }

    const tokenHash = sha256(sessionToken);
    const session = await prisma.session.findUnique({
      where: { tokenHash },
      select: { userId: true },
    });
    if (!session) {
      throw new AppError("UNAUTHORIZED", 401, "Invalid session");
    }

    await prisma.session.deleteMany({ where: { userId: session.userId } });
    reply.clearCookie("sid", {
      path: "/",
      httpOnly: true,
      sameSite: sessionSameSite(app),
      secure: shouldUseSecureCookie(app),
      domain: app.env.COOKIE_DOMAIN || undefined,
    });
    reply.send({ ok: true });
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
  const baseLimit = Number(request.server.env.AUTH_REGISTER_IP_RATE_LIMIT || "5");
  const limit = isDemoMode(request.server.env) ? Math.min(baseLimit, 3) : baseLimit;
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

  const baseIpLimit = Number(request.server.env.AUTH_LOGIN_IP_RATE_LIMIT || "10");
  const ipLimit = isDemoMode(request.server.env) ? Math.min(baseIpLimit, 5) : baseIpLimit;
  enforceRateLimit(request, {
    scope: "auth_login_ip",
    keyParts: [ip],
    limit: ipLimit,
    windowSec,
    message: "Too many login attempts. Please try again later.",
  });

  const baseIdentifierLimit = Number(request.server.env.AUTH_LOGIN_IDENTIFIER_RATE_LIMIT || "5");
  const identifierLimit = isDemoMode(request.server.env) ? Math.min(baseIdentifierLimit, 3) : baseIdentifierLimit;
  enforceRateLimit(request, {
    scope: "auth_login_identifier",
    keyParts: [identifier],
    limit: identifierLimit,
    windowSec,
    message: "Too many login attempts. Please try again later.",
  });
}

async function createSession(userId: string, app: FastifyInstance) {
  const sessionToken = randomToken(32);
  const tokenHash = sha256(sessionToken);
  const now = Date.now();
  const absoluteTtlDays = readPositiveNumber(app.env.SESSION_TTL_DAYS, 7);
  const absoluteExpiresAtMs = now + absoluteTtlDays * 24 * 60 * 60 * 1000;
  const idleHours = readPositiveNumber(app.env.SESSION_IDLE_TIMEOUT_HOURS, 24);
  const slidingEnabled = app.env.SESSION_SLIDING_ENABLED !== "false";
  const idleExpiresAtMs = now + idleHours * 60 * 60 * 1000;
  const expiresAt = new Date(slidingEnabled ? Math.min(absoluteExpiresAtMs, idleExpiresAtMs) : absoluteExpiresAtMs);

  await prisma.session.create({
    data: {
      userId,
      tokenHash,
      expiresAt,
    },
  });

  const maxSessions = readPositiveNumber(app.env.SESSION_MAX_PER_USER, 10);
  const sessions = await prisma.session.findMany({
    where: { userId },
    orderBy: { createdAt: "desc" },
    select: { id: true },
  });
  const stale = sessions.slice(maxSessions);
  if (stale.length > 0) {
    await prisma.session.deleteMany({
      where: { id: { in: stale.map((item) => item.id) } },
    });
  }

  return sessionToken;
}

function setSessionCookie(reply: any, token: string, app: FastifyInstance) {
  const secure = shouldUseSecureCookie(app);
  reply.setCookie("sid", token, {
    path: "/",
    httpOnly: true,
    sameSite: sessionSameSite(app),
    secure,
    domain: app.env.COOKIE_DOMAIN || undefined,
    maxAge: readPositiveNumber(app.env.SESSION_TTL_DAYS, 7) * 24 * 60 * 60,
  });
}

function shouldUseSecureCookie(app: FastifyInstance) {
  return app.env.NODE_ENV === "production" || app.env.COOKIE_SECURE === "true";
}

function sessionSameSite(app: FastifyInstance): "lax" | "none" {
  try {
    const apiOrigin = new URL(app.env.API_BASE_URL).origin;
    const webOrigin = new URL(app.env.WEB_BASE_URL).origin;
    return apiOrigin === webOrigin ? "lax" : "none";
  } catch {
    return "lax";
  }
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

function readPositiveNumber(value: string | undefined, fallback: number) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}
