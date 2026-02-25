import crypto from "crypto";

export function randomToken(bytes = 32) {
  return crypto.randomBytes(bytes).toString("hex");
}

export function sha256(value: string) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

export function apiKeySecret() {
  return `ak_${randomToken(24)}`;
}

export function prefixFromKey(key: string, length = 8) {
  return key.slice(0, length);
}
