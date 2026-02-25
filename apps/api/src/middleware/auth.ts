import { FastifyRequest } from "fastify";
import { prisma } from "../lib/prisma";
import { sha256 } from "../lib/crypto";
import { AppError } from "../lib/errors";
import { Role } from "@saas/db";

export type AuthContext = {
  userId: string;
  email: string;
  role: Role;
  orgId: string;
};

export async function requireUser(request: FastifyRequest) {
  const sessionToken = request.cookies["sid"] as string | undefined;
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

  const orgId = getOrgId(request);
  const membership = await prisma.membership.findUnique({
    where: { orgId_userId: { orgId, userId: session.userId } },
  });

  if (!membership) {
    request.log.warn({ reason: "missing_membership", userId: session.userId, orgId }, "Auth failed");
    throw new AppError("FORBIDDEN", 403, "Not a member of this organization");
  }

  request.auth = {
    userId: session.userId,
    email: session.user.email,
    role: membership.role,
    orgId,
  };
  request.log = request.log.child({ orgId, userId: session.userId, role: membership.role });
}

export async function requireRole(request: FastifyRequest, roles: Role[]) {
  if (!request.auth) {
    await requireUser(request);
  }
  if (!request.auth || !roles.includes(request.auth.role)) {
    throw new AppError("FORBIDDEN", 403, "Insufficient role");
  }
}

export function getOrgId(request: FastifyRequest) {
  const orgId = request.headers["x-org-id"] as string | undefined;
  if (!orgId) {
    throw new AppError("BAD_REQUEST", 400, "Missing X-Org-Id header");
  }
  return orgId;
}

declare module "fastify" {
  interface FastifyRequest {
    auth?: AuthContext;
  }
}
