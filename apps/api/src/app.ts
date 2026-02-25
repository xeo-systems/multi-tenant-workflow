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
import { recordUsage } from "./middleware/usage";
import crypto from "crypto";

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

  app.addHook("onResponse", async (request, reply) => {
    if (reply.statusCode < 500 || request.usageReserved) {
      try {
        await recordUsage(request, reply);
      } catch (error) {
        request.log.error({ err: error }, "Usage recording failed");
      }
    }
  });

  app.get("/health", async () => ({ status: "ok" }));

  app.register(authRoutes, { prefix: "/auth" });
  app.register(orgRoutes, { prefix: "/org" });
  app.register(apiKeyRoutes, { prefix: "/api-keys" });
  app.register(usageRoutes, { prefix: "/usage" });
  app.register(billingRoutes, { prefix: "/billing" });
  app.register(adminRoutes, { prefix: "/admin" });
  app.register(dataRoutes, { prefix: "/data" });

  app.setErrorHandler(async (error, request, reply) => {
    if (isAppError(error)) {
      reply.status(error.httpStatus).send({
        error: {
          code: error.code,
          message: error.message,
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

declare module "fastify" {
  interface FastifyInstance {
    env: ReturnType<typeof loadEnv>;
  }
}
