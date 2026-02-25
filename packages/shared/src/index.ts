import { z } from "zod";
import {
  apiKeyNameSchema,
  emailSchema,
  loginPasswordSchema,
  orgNameSchema,
  passwordSchema,
} from "./validation";

export const RoleSchema = z.enum(["OWNER", "ADMIN", "MEMBER", "BILLING", "READONLY"]);

export const LoginSchema = z.object({
  email: emailSchema,
  password: loginPasswordSchema,
});

export const RegisterSchema = z.object({
  email: emailSchema,
  password: passwordSchema,
  orgName: orgNameSchema,
});

export const ApiKeyCreateSchema = z.object({
  name: apiKeyNameSchema,
});

export const MembershipInviteSchema = z.object({
  email: emailSchema,
  role: RoleSchema,
});

export const UsageEventSchema = z.object({
  metric: z.string().min(1),
  quantity: z.number().int().positive(),
});

export type Role = z.infer<typeof RoleSchema>;
export { emailSchema, passwordSchema, orgNameSchema, apiKeyNameSchema };
