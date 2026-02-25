import { z } from "zod";

const DISPOSABLE_EMAIL_DOMAINS = new Set([
  "mailinator.com",
  "10minutemail.com",
  "guerrillamail.com",
  "tempmail.com",
  "yopmail.com",
]);

export const emailSchema = z
  .string()
  .trim()
  .toLowerCase()
  .max(254, "Email must be 254 characters or fewer")
  .email("Enter a valid email address")
  .refine((email) => !DISPOSABLE_EMAIL_DOMAINS.has(email.split("@").at(-1) || ""), {
    message: "Use a non-disposable email address",
  });

export const passwordSchema = z
  .string()
  .trim()
  .min(10, "Password must be at least 10 characters")
  .max(128, "Password must be 128 characters or fewer")
  .regex(/[A-Z]/, "Password must include at least one uppercase letter")
  .regex(/[a-z]/, "Password must include at least one lowercase letter")
  .regex(/[0-9]/, "Password must include at least one number")
  .regex(/[^A-Za-z0-9]/, "Password must include at least one symbol");

export const loginPasswordSchema = z
  .string()
  .trim()
  .min(1, "Password is required")
  .max(128, "Password must be 128 characters or fewer");

export const orgNameSchema = z
  .string()
  .trim()
  .min(2, "Organization name must be at least 2 characters")
  .max(60, "Organization name must be 60 characters or fewer")
  .regex(/^[A-Za-z0-9 -]+$/, "Organization name can only contain letters, numbers, spaces, and dashes");

export const apiKeyNameSchema = z
  .string()
  .trim()
  .min(1, "API key name is required")
  .max(50, "API key name must be 50 characters or fewer")
  .regex(/^[A-Za-z0-9_-]+$/, "API key name can only contain letters, numbers, dashes, and underscores");
