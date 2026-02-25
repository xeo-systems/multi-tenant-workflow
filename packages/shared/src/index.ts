import { z } from "zod";

export const RoleSchema = z.enum(["OWNER", "ADMIN", "MEMBER", "BILLING", "READONLY"]);

export const LoginSchema = z.object({
  email: z.string().email(),
  password: z.string().min(8),
});

export const RegisterSchema = z.object({
  email: z.string().email(),
  password: z.string().min(8),
  orgName: z.string().min(2),
});

export const ApiKeyCreateSchema = z.object({
  name: z.string().min(2).max(120),
});

export const MembershipInviteSchema = z.object({
  email: z.string().email(),
  role: RoleSchema,
});

export const UsageEventSchema = z.object({
  metric: z.string().min(1),
  quantity: z.number().int().positive(),
});

export type Role = z.infer<typeof RoleSchema>;
