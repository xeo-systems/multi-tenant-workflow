import { Role } from "@saas/db";

export type TenantContext = {
  orgId: string;
  userId?: string;
  role?: Role;
  apiKeyId?: string;
};
