import { JobsOptions, Queue } from "bullmq";
import { loadEnv } from "../config/env";

type QueueLike = {
  add: (name: string, data: unknown, opts?: JobsOptions) => Promise<unknown>;
};

const env = loadEnv();

function createQueue(name: string): QueueLike {
  if (env.NODE_ENV === "test") {
    return { add: async () => undefined };
  }
  const redisUrl = new URL(env.REDIS_URL);
  const connection = {
    host: redisUrl.hostname,
    port: Number(redisUrl.port || "6379"),
    password: redisUrl.password || undefined,
  };
  return new Queue(name, { connection });
}

export const stripeQueue = createQueue("stripe-events");
export const usageQueue = createQueue("usage-rollups");
