import { z } from "zod";

const SharedRoleSchema = z.enum(["OWNER", "ADMIN", "MEMBER", "BILLING", "READONLY"]);

export const PermissionSchema = z.enum([
  "org.read",
  "members.read",
  "members.invite",
  "members.role.update",
  "members.remove",
  "apiKeys.read",
  "apiKeys.create",
  "apiKeys.revoke",
  "usage.read",
  "billing.read",
  "billing.manage",
  "audit.read",
  "plan.read",
  "plan.update",
]);

export type Permission = z.infer<typeof PermissionSchema>;
export type SharedRole = z.infer<typeof SharedRoleSchema>;

export const ROLE_PERMISSIONS: Record<SharedRole, Permission[]> = {
  OWNER: PermissionSchema.options,
  ADMIN: [
    "org.read",
    "members.read",
    "members.invite",
    "members.role.update",
    "members.remove",
    "apiKeys.read",
    "apiKeys.create",
    "apiKeys.revoke",
    "usage.read",
    "billing.read",
    "billing.manage",
    "audit.read",
    "plan.read",
  ],
  MEMBER: ["org.read", "members.read"],
  BILLING: [
    "org.read",
    "members.read",
    "apiKeys.read",
    "apiKeys.create",
    "apiKeys.revoke",
    "usage.read",
    "billing.read",
    "billing.manage",
  ],
  READONLY: ["org.read", "members.read"],
};

export function hasPermission(role: SharedRole, permission: Permission) {
  return ROLE_PERMISSIONS[role].includes(permission);
}
