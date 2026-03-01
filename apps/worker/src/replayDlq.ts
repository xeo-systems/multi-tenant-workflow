import { Queue, JobsOptions } from "bullmq";
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
  REDIS_URL: z.string().url(),
  DLQ_REPLAY_BATCH_SIZE: z.string().default("100"),
});

type DeadLetterPayload = {
  originalQueue: string;
  originalJobName: string;
  originalJobId?: string;
  originalData: unknown;
  originalOptions?: JobsOptions;
};

async function main() {
  const env = EnvSchema.parse(process.env);
  const redisUrl = new URL(env.REDIS_URL);
  const connection = {
    host: redisUrl.hostname,
    port: Number(redisUrl.port || "6379"),
    password: redisUrl.password || undefined,
    tls: redisUrl.protocol === "rediss:" ? {} : undefined,
  };
  const batchSize = Math.max(1, Number(env.DLQ_REPLAY_BATCH_SIZE || "100"));
  const requestedQueue = process.argv[2];
  const dlqNames = requestedQueue
    ? [`${requestedQueue}-dlq`]
    : ["stripe-events-dlq", "usage-rollups-dlq", "maintenance-jobs-dlq"];

  let replayed = 0;
  const opened = new Map<string, Queue>();

  try {
    for (const dlqName of dlqNames) {
      const dlq = new Queue(dlqName, { connection });
      const jobs = await dlq.getJobs(["waiting", "delayed", "prioritized"], 0, batchSize - 1, false);

      for (const job of jobs) {
        const payload = job.data as DeadLetterPayload;
        if (!payload?.originalQueue || !payload?.originalJobName) {
          continue;
        }
        if (!opened.has(payload.originalQueue)) {
          opened.set(payload.originalQueue, new Queue(payload.originalQueue, { connection }));
        }

        const target = opened.get(payload.originalQueue)!;
        await target.add(payload.originalJobName, payload.originalData, {
          ...(payload.originalOptions || {}),
          jobId: payload.originalJobId
            ? `${payload.originalJobId}:dlq-replay:${Date.now()}`
            : `dlq-replay:${Date.now()}:${Math.random().toString(36).slice(2, 10)}`,
        });
        await job.remove();
        replayed += 1;
      }

      await dlq.close();
    }
  } finally {
    await Promise.all(Array.from(opened.values()).map((queue) => queue.close()));
  }

  console.log(JSON.stringify({ event: "dlq.replay.complete", replayed, dlqNames }));
}

void main().catch((error) => {
  console.error(JSON.stringify({ event: "dlq.replay.failed", error: String(error) }));
  process.exit(1);
});
