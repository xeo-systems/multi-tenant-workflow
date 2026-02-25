import crypto from "crypto";
import { FastifyRequest } from "fastify";
import { AppError } from "../lib/errors";

type RateLimitBucket = {
  count: number;
  resetAt: number;
};

type EnforceRateLimitOptions = {
  scope: string;
  keyParts: Array<string | undefined | null>;
  limit: number;
  windowSec: number;
  message: string;
  field?: string;
};

const buckets = new Map<string, RateLimitBucket>();

export function normalizeIdentifier(value: unknown) {
  if (typeof value !== "string") {
    return "unknown";
  }
  const normalized = value.trim().toLowerCase();
  if (!normalized) {
    return "unknown";
  }
  return normalized.slice(0, 254);
}

export function enforceRateLimit(request: FastifyRequest, options: EnforceRateLimitOptions) {
  if (!Number.isFinite(options.limit) || options.limit <= 0) {
    return;
  }
  const windowSec = Number.isFinite(options.windowSec) && options.windowSec > 0 ? options.windowSec : 60;
  const key = makeBucketKey(options.scope, options.keyParts);
  const now = Date.now();
  const windowMs = windowSec * 1000;
  const existing = buckets.get(key);

  if (!existing || now >= existing.resetAt) {
    buckets.set(key, {
      count: 1,
      resetAt: now + windowMs,
    });
    pruneBuckets(now);
    return;
  }

  if (existing.count >= options.limit) {
    const retryAfterSec = Math.max(1, Math.ceil((existing.resetAt - now) / 1000));
    throw new AppError("RATE_LIMIT", 429, options.message, {
      field: options.field,
      scope: options.scope,
      limit: options.limit,
      windowSec,
      retryAfterSec,
    });
  }

  existing.count += 1;
}

function makeBucketKey(scope: string, keyParts: Array<string | undefined | null>) {
  const raw = `${scope}|${keyParts.map((part) => part || "-").join("|")}`;
  const hash = crypto.createHash("sha256").update(raw).digest("hex");
  return `${scope}:${hash}`;
}

function pruneBuckets(now: number) {
  if (buckets.size < 1000) {
    return;
  }
  for (const [key, value] of buckets.entries()) {
    if (now >= value.resetAt) {
      buckets.delete(key);
    }
  }
}

export function resetRateLimitStore() {
  buckets.clear();
}
