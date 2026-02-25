import { TenantContext } from "../lib/tenant";
import { apiKeySecret, prefixFromKey, sha256 } from "../lib/crypto";
import { AppError } from "../lib/errors";
import { writeAudit } from "./audit";
import * as apiKeyRepo from "../repo/apiKeyRepo";

export async function listApiKeys(ctx: TenantContext) {
  return apiKeyRepo.listByOrg(ctx.orgId);
}

export async function createApiKey(ctx: TenantContext, name: string) {
  const secret = apiKeySecret();
  const prefix = prefixFromKey(secret);

  const apiKey = await apiKeyRepo.createApiKey(ctx.orgId, name, prefix, sha256(secret));

  await writeAudit(ctx, "apiKey.create", "apiKey", apiKey.id);

  return { apiKey, secret };
}

export async function revokeApiKey(ctx: TenantContext, id: string) {
  const apiKey = await apiKeyRepo.findById(ctx.orgId, id);
  if (!apiKey) {
    throw new AppError("NOT_FOUND", 404, "API key not found");
  }

  await apiKeyRepo.revokeApiKey(ctx.orgId, id);

  await writeAudit(ctx, "apiKey.revoke", "apiKey", id);
}
