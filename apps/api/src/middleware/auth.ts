import { FastifyRequest } from "fastify";
import { prisma } from "../lib/prisma";
import { sha256 } from "../lib/crypto";
import { AppError } from "../lib/errors";
import { Role } from "@saas/db";
import { hasPermission } from "@saas/shared";
import type { Permission } from "@saas/shared";

export type AuthContext = {
  userId: string;
  email: string;
  role: Role;
  orgId: string;
};

export async function requireUser(request: FastifyRequest) {
  const sessionToken = readSessionToken(request);
  if (!sessionToken) {
    request.log.warn({ reason: "missing_session" }, "Auth failed");
    throw new AppError("UNAUTHORIZED", 401, "Missing session");
  }

  const tokenHash = sha256(sessionToken);
  const session = await prisma.session.findUnique({
    where: { tokenHash },
    include: { user: true },
  });

  if (!session || session.expiresAt < new Date()) {
    request.log.warn({ reason: "invalid_session", userId: session?.userId }, "Auth failed");
    throw new AppError("UNAUTHORIZED", 401, "Invalid session");
  }

  const absoluteTtlDays = readPositiveNumber(request.server.env.SESSION_TTL_DAYS, 7);
  const absoluteExpiry = new Date(session.createdAt.getTime() + absoluteTtlDays * 24 * 60 * 60 * 1000);
  if (new Date() > absoluteExpiry) {
    await prisma.session.deleteMany({ where: { id: session.id } });
    request.log.warn({ reason: "session_absolute_expired", userId: session.userId }, "Auth failed");
    throw new AppError("UNAUTHORIZED", 401, "Invalid session");
  }

  const slidingEnabled = request.server.env.SESSION_SLIDING_ENABLED !== "false";
  if (slidingEnabled) {
    const idleHours = readPositiveNumber(request.server.env.SESSION_IDLE_TIMEOUT_HOURS, 24);
    const idleExpiry = new Date(Date.now() + idleHours * 60 * 60 * 1000);
    const nextExpiry = idleExpiry < absoluteExpiry ? idleExpiry : absoluteExpiry;
    const refreshThresholdMs = Math.floor((idleHours * 60 * 60 * 1000) / 2);
    if (session.expiresAt.getTime() - Date.now() < refreshThresholdMs) {
      await prisma.session.update({
        where: { id: session.id },
        data: { expiresAt: nextExpiry },
      });
    }
  }

  const requestedOrgId = readOrgIdHeader(request);
  const membership = requestedOrgId
    ? await prisma.membership.findUnique({
        where: { orgId_userId: { orgId: requestedOrgId, userId: session.userId } },
      })
    : await prisma.membership.findFirst({
        where: { userId: session.userId },
        orderBy: { createdAt: "asc" },
      });

  if (!membership) {
    if (!requestedOrgId) {
      request.log.warn({ reason: "missing_default_membership", userId: session.userId }, "Auth failed");
      throw new AppError("FORBIDDEN", 403, "No organization membership found");
    }
    request.log.warn({ reason: "missing_membership", userId: session.userId, orgId: requestedOrgId }, "Auth failed");
    throw new AppError("FORBIDDEN", 403, "Not a member of this organization");
  }

  request.auth = {
    userId: session.userId,
    email: session.user.email,
    role: membership.role,
    orgId: membership.orgId,
  };
  request.log = request.log.child({ orgId: membership.orgId, userId: session.userId, role: membership.role });
}

export function readSessionToken(request: FastifyRequest): string | undefined {
  const cookieToken = request.cookies["sid"] as string | undefined;
  if (cookieToken) {
    return cookieToken;
  }
  const authHeader = request.headers.authorization;
  const raw = Array.isArray(authHeader) ? authHeader[0] : authHeader;
  if (!raw || !raw.startsWith("Bearer ")) {
    return undefined;
  }
  const token = raw.slice("Bearer ".length).trim();
  return token || undefined;
}

function readPositiveNumber(value: string | undefined, fallback: number) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

export async function requireRole(request: FastifyRequest, roles: Role[]) {
  if (!request.auth) {
    await requireUser(request);
  }
  if (!request.auth || !roles.includes(request.auth.role)) {
    throw new AppError("FORBIDDEN", 403, "Insufficient role");
  }
}

export async function requirePermission(request: FastifyRequest, permission: Permission) {
  if (!request.auth) {
    await requireUser(request);
  }
  if (!request.auth || !hasPermission(request.auth.role, permission)) {
    throw new AppError("FORBIDDEN", 403, "Insufficient permission");
  }
}

export function getOrgId(request: FastifyRequest) {
  const orgId = readOrgIdHeader(request);
  if (!orgId) {
    throw new AppError("BAD_REQUEST", 400, "Missing X-Org-Id header");
  }
  return orgId;
}

function readOrgIdHeader(request: FastifyRequest) {
  return request.headers["x-org-id"] as string | undefined;
}

declare module "fastify" {
  interface FastifyRequest {
    auth?: AuthContext;
  }
}
