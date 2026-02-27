import Fastify from "fastify";
import cookie from "@fastify/cookie";
import cors from "@fastify/cors";
import rawBody from "fastify-raw-body";
import { loadEnv } from "./config/env";
import { createLogger } from "./lib/logger";
import { captureError, initSentry } from "./lib/sentry";
import { isAppError, AppError } from "./lib/errors";
import { authRoutes } from "./routes/auth";
import { orgRoutes } from "./routes/org";
import { apiKeyRoutes } from "./routes/apiKeys";
import { usageRoutes } from "./routes/usage";
import { billingRoutes } from "./routes/billing";
import { adminRoutes } from "./routes/admin";
import { dataRoutes } from "./routes/data";
import { auditRoutes } from "./routes/audit";
import { scimRoutes } from "./routes/scim";
import { recordUsage } from "./middleware/usage";
import crypto from "crypto";
import { ZodError } from "zod";
import { checkDbReady, checkRedisReady } from "./lib/readiness";
import { prisma } from "./lib/prisma";
import { closeQueues } from "./lib/queue";

export function buildApp() {
  const env = loadEnv();
  const logger = createLogger(env);
  const sentryEnabled = initSentry(env);

  const app = Fastify({
    logger,
    disableRequestLogging: true,
    genReqId: () => crypto.randomBytes(12).toString("hex"),
  });

  app.decorate("env", env);

  app.register(cors, {
    origin: env.WEB_BASE_URL,
    credentials: true,
  });

  app.register(cookie, {
    secret: env.SESSION_SECRET,
    parseOptions: {},
  });

  app.register(rawBody, {
    field: "rawBody",
    global: false,
    encoding: "utf8",
    runFirst: true,
  });

  app.addHook("onRequest", async (request) => {
    request.log = logger.child({ requestId: request.id });
  });

  app.addHook("onSend", async (_request, reply, payload) => {
    reply.header("X-Content-Type-Options", "nosniff");
    reply.header("Referrer-Policy", "strict-origin-when-cross-origin");
    reply.header("X-Frame-Options", "DENY");
    if (env.NODE_ENV === "production") {
      reply.header("Strict-Transport-Security", "max-age=31536000; includeSubDomains");
    }
    return payload;
  });

  app.addHook("onResponse", async (request, reply) => {
    if (reply.statusCode < 500 || request.usageReserved) {
      try {
        await recordUsage(request, reply);
      } catch (error) {
        request.log.error({ err: error }, "Usage recording failed");
      }
    }
  });

  app.addHook("onClose", async () => {
    await Promise.allSettled([prisma.$disconnect(), closeQueues()]);
  });

  app.get("/health", async () => ({ status: "ok" }));
  app.get("/ready", async (request) => {
    try {
      await checkDbReady();
      await checkRedisReady();
      return { status: "ready" };
    } catch (error) {
      request.log.error({ err: error }, "Readiness check failed");
      throw new AppError("INTERNAL", 503, "Service not ready");
    }
  });

  app.register(authRoutes, { prefix: "/auth" });
  app.register(orgRoutes, { prefix: "/org" });
  app.register(apiKeyRoutes, { prefix: "/api-keys" });
  app.register(usageRoutes, { prefix: "/usage" });
  app.register(billingRoutes, { prefix: "/billing" });
  app.register(adminRoutes, { prefix: "/admin" });
  app.register(auditRoutes, { prefix: "/audit" });
  app.register(scimRoutes, { prefix: "/scim/v2" });
  app.register(dataRoutes, { prefix: "/data" });

  app.setErrorHandler(async (error, request, reply) => {
    const zodError = getZodError(error);
    if (zodError) {
      const issue = zodError.issues[0];
      const field = issue?.path?.join(".") || undefined;
      reply.status(400).send({
        error: {
          code: "BAD_REQUEST",
          message: issue?.message || "Invalid request",
          field,
          requestId: request.id,
        },
      });
      return;
    }

    if (isAppError(error)) {
      const details = (error.details || {}) as {
        field?: string;
        retryAfterSec?: number;
        scope?: string;
        limit?: number;
        windowSec?: number;
      };
      if (error.httpStatus === 429 && details.retryAfterSec) {
        reply.header("Retry-After", String(details.retryAfterSec));
      }
      reply.status(error.httpStatus).send({
        error: {
          code: error.code,
          message: error.message,
          field: details.field,
          retryAfterSec: details.retryAfterSec,
          scope: details.scope,
          limit: details.limit,
          windowSec: details.windowSec,
          requestId: request.id,
        },
      });
      return;
    }

    request.log.error({ err: error }, "Unhandled error");
    if (sentryEnabled) {
      captureError(error);
    }
    reply.status(500).send({
      error: {
        code: "INTERNAL",
        message: "Internal server error",
        requestId: request.id,
      },
    });
  });

  app.setNotFoundHandler(async (request, reply) => {
    reply.status(404).send({
      error: {
        code: "NOT_FOUND",
        message: `Route ${request.method} ${request.url} not found`,
        requestId: request.id,
      },
    });
  });

  return app;
}

function getZodError(error: unknown): ZodError | null {
  if (error instanceof ZodError) {
    return error;
  }
  if (!error || typeof error !== "object") {
    return null;
  }
  const maybe = error as { name?: unknown; issues?: unknown };
  if (maybe.name !== "ZodError" || !Array.isArray(maybe.issues)) {
    return null;
  }
  return error as ZodError;
}

declare module "fastify" {
  interface FastifyInstance {
    env: ReturnType<typeof loadEnv>;
  }
}
