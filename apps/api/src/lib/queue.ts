import { JobsOptions, Queue } from "bullmq";
import { loadEnv } from "../config/env";

type QueueLike = {
  add: (name: string, data: unknown, opts?: JobsOptions) => Promise<unknown>;
  close: () => Promise<void>;
  ping: () => Promise<string>;
};

const env = loadEnv();
const QUEUE_JOB_ATTEMPTS = readPositiveInt(env.QUEUE_JOB_ATTEMPTS, 5);
const QUEUE_BACKOFF_DELAY_MS = readPositiveInt(env.QUEUE_BACKOFF_DELAY_MS, 2000);

export const QUEUE_RETRY_DEFAULTS = {
  attempts: QUEUE_JOB_ATTEMPTS,
  backoffDelayMs: QUEUE_BACKOFF_DELAY_MS,
} as const;

function createQueue(name: string): QueueLike {
  if (env.NODE_ENV === "test") {
    return {
      add: async () => undefined,
      close: async () => undefined,
      ping: async () => "PONG",
    };
  }
  const redisUrl = new URL(env.REDIS_URL);
  const connection = {
    host: redisUrl.hostname,
    port: Number(redisUrl.port || "6379"),
    password: redisUrl.password || undefined,
    tls: redisUrl.protocol === "rediss:" ? {} : undefined,
  };
  const queue = new Queue(name, { connection });
  return {
    add: (jobName, data, opts) => queue.add(jobName, data, opts),
    close: () => queue.close(),
    ping: async () => {
      const client = await queue.client;
      return client.ping();
    },
  };
}

export const stripeQueue = createQueue("stripe-events");
export const usageQueue = createQueue("usage-rollups");

export async function pingRedis() {
  return stripeQueue.ping();
}

export async function closeQueues() {
  await Promise.all([stripeQueue.close(), usageQueue.close()]);
}

export function buildRetryJobOptions(opts: JobsOptions = {}): JobsOptions {
  return {
    attempts: QUEUE_JOB_ATTEMPTS,
    backoff: {
      type: "exponential",
      delay: QUEUE_BACKOFF_DELAY_MS,
    },
    removeOnComplete: 1000,
    removeOnFail: false,
    ...opts,
  };
}

function readPositiveInt(value: string | undefined, fallback: number) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return Math.floor(parsed);
}
