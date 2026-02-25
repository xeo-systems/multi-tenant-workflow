import pino from "pino";
import { Env } from "../config/env";

export function createLogger(env: Env) {
  return pino({
    level: env.LOG_LEVEL,
    base: undefined,
  });
}
