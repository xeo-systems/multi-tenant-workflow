import { config as loadDotenv } from "dotenv";
import fs from "fs";
import path from "path";
import { z } from "zod";

function loadEnvFile() {
  let dir = process.cwd();
  for (let i = 0; i < 6; i += 1) {
    const candidate = path.join(dir, ".env");
    if (fs.existsSync(candidate)) {
      loadDotenv({ path: candidate });
      return;
    }
    const parent = path.dirname(dir);
    if (parent === dir) {
      return;
    }
    dir = parent;
  }
}

loadEnvFile();

const EnvSchema = z.object({
  NODE_ENV: z.string().default("development"),
  API_PORT: z.string().default("4000"),
  API_BASE_URL: z.string().url().default("http://localhost:4000"),
  WEB_BASE_URL: z.string().url().default("http://localhost:3000"),
  AUTH_PROVIDER: z.string().default("local"),
  DATABASE_URL: z.string().url(),
  REDIS_URL: z.string().url(),
  SESSION_SECRET: z.string().min(16),
  SESSION_TTL_DAYS: z.string().default("7"),
  SESSION_IDLE_TIMEOUT_HOURS: z.string().default("24"),
  SESSION_SLIDING_ENABLED: z.string().default("true"),
  SESSION_MAX_PER_USER: z.string().default("10"),
  LOG_LEVEL: z.string().default("info"),
  COOKIE_DOMAIN: z.string().optional().nullable(),
  COOKIE_SECURE: z.string().default("false"),
  SENTRY_DSN: z.string().optional().nullable(),
  SENTRY_ENABLED: z.string().default("false"),
  DEMO_MODE: z.string().default("false"),
  STRIPE_SECRET_KEY: z.string().optional().nullable(),
  STRIPE_WEBHOOK_SECRET: z.string().optional().nullable(),
  STRIPE_PRICE_ID: z.string().optional().nullable(),
  STRIPE_SUCCESS_URL: z.string().optional().nullable(),
  STRIPE_CANCEL_URL: z.string().optional().nullable(),
  STRIPE_PORTAL_RETURN_URL: z.string().optional().nullable(),
  STRIPE_WEBHOOK_TOLERANCE: z.string().default("300"),
  WORKER_CONCURRENCY: z.string().default("5"),
  API_KEY_RATE_LIMIT: z.string().default("1000"),
  API_KEY_RATE_LIMIT_WINDOW_SEC: z.string().default("60"),
  AUTH_RATE_LIMIT_WINDOW_SEC: z.string().default("60"),
  AUTH_LOGIN_IP_RATE_LIMIT: z.string().default("10"),
  AUTH_LOGIN_IDENTIFIER_RATE_LIMIT: z.string().default("5"),
  AUTH_REGISTER_IP_RATE_LIMIT: z.string().default("5"),
  AUDIT_LOG_RETENTION_DAYS: z.string().default("365"),
  INTERNAL_ADMIN_TOKEN: z.string().optional().nullable(),
  SCIM_BEARER_TOKEN: z.string().optional().nullable(),
});

export type Env = z.infer<typeof EnvSchema>;

export function loadEnv(): Env {
  const parsed = EnvSchema.safeParse(process.env);
  if (!parsed.success) {
    const issues = parsed.error.issues.map((issue) => `${issue.path.join(".")}: ${issue.message}`).join(", ");
    throw new Error(`Invalid environment: ${issues}`);
  }
  const env = parsed.data;
  if (env.NODE_ENV === "production") {
    const required = [
      "DATABASE_URL",
      "REDIS_URL",
      "SESSION_SECRET",
      "API_BASE_URL",
      "WEB_BASE_URL",
      "STRIPE_SECRET_KEY",
      "STRIPE_WEBHOOK_SECRET",
    ] as const;
    const missing = required.filter((key) => !env[key]);
    if (missing.length > 0) {
      throw new Error(`Missing required production env vars: ${missing.join(", ")}`);
    }
    if (env.SESSION_SECRET.length < 32) {
      throw new Error("SESSION_SECRET must be at least 32 characters in production");
    }
  }
  return env;
}
