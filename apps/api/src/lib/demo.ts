import { Env } from "../config/env";
import { AppError } from "./errors";

export function isDemoMode(env: Env) {
  return env.DEMO_MODE === "true";
}

export function assertNotDemo(env: Env, message = "Action disabled in demo mode") {
  if (isDemoMode(env)) {
    throw new AppError("FORBIDDEN", 403, message);
  }
}
