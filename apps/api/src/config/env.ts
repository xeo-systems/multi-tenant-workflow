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
  DATABASE_URL: z.string().url(),
  REDIS_URL: z.string().url(),
  SESSION_SECRET: z.string().min(16),
  LOG_LEVEL: z.string().default("info"),
  COOKIE_DOMAIN: z.string().optional().nullable(),
  COOKIE_SECURE: z.string().default("false"),
  SENTRY_DSN: z.string().optional().nullable(),
  SENTRY_ENABLED: z.string().default("false"),
  STRIPE_SECRET_KEY: z.string().optional().nullable(),
  STRIPE_WEBHOOK_SECRET: z.string().optional().nullable(),
  STRIPE_PRICE_ID: z.string().optional().nullable(),
  STRIPE_SUCCESS_URL: z.string().optional().nullable(),
  STRIPE_CANCEL_URL: z.string().optional().nullable(),
  STRIPE_PORTAL_RETURN_URL: z.string().optional().nullable(),
  STRIPE_WEBHOOK_TOLERANCE: z.string().default("300"),
  WORKER_CONCURRENCY: z.string().default("5"),
  API_KEY_RATE_LIMIT: z.string().default("1000")
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
  }
  return env;
}
