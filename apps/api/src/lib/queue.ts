import { JobsOptions, Queue } from "bullmq";
import { loadEnv } from "../config/env";

type QueueLike = {
  add: (name: string, data: unknown, opts?: JobsOptions) => Promise<unknown>;
  close: () => Promise<void>;
  ping: () => Promise<string>;
};

const env = loadEnv();

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
