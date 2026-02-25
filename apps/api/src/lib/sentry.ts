import * as Sentry from "@sentry/node";
import { Env } from "../config/env";

export function initSentry(env: Env) {
  if (env.SENTRY_ENABLED !== "true" || !env.SENTRY_DSN) {
    return false;
  }
  Sentry.init({
    dsn: env.SENTRY_DSN,
    environment: env.NODE_ENV,
    tracesSampleRate: 0,
  });
  return true;
}

export function captureError(err: unknown) {
  Sentry.captureException(err);
}
