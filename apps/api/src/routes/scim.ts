import { FastifyInstance, FastifyRequest } from "fastify";
import bcrypt from "bcryptjs";
import { z } from "zod";
import { prisma } from "../lib/prisma";
import { AppError } from "../lib/errors";
import { randomToken } from "../lib/crypto";
import { getOrgId } from "../middleware/auth";

const PASSWORD_COST = 12;

const ScimCreateUserSchema = z.object({
  userName: z.string().email(),
  active: z.boolean().optional(),
  roles: z.array(z.object({ value: z.string() })).optional(),
});

const ScimPatchUserSchema = z.object({
  active: z.boolean().optional(),
  role: z.enum(["OWNER", "ADMIN", "MEMBER", "BILLING", "READONLY"]).optional(),
});

export async function scimRoutes(app: FastifyInstance) {
  const scimBearerToken = getScimBearerToken(app);

  app.get("/Users", async (request) => {
    requireScimToken(request, scimBearerToken);
    const orgId = getOrgId(request);
    const query = parseScimQuery(request.query as Record<string, unknown> | undefined);

    const where = {
      orgId,
      ...(query.userName ? { user: { email: query.userName } } : {}),
    };

    const [totalResults, rows] = await Promise.all([
      prisma.membership.count({ where }),
      prisma.membership.findMany({
        where,
        include: { user: true },
        orderBy: { createdAt: "asc" },
        skip: query.startIndex - 1,
        take: query.count,
      }),
    ]);

    return {
      schemas: ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
      totalResults,
      startIndex: query.startIndex,
      itemsPerPage: rows.length,
      Resources: rows.map((row) => ({
        id: row.userId,
        userName: row.user.email,
        active: true,
        roles: [{ value: row.role }],
      })),
    };
  });

  app.post("/Users", async (request, reply) => {
    requireScimToken(request, scimBearerToken);
    const orgId = getOrgId(request);
    const input = ScimCreateUserSchema.parse(request.body);
    const role = normalizeRole(input.roles?.[0]?.value);
    const email = input.userName.trim().toLowerCase();

    const user = await findOrCreateUser(email);
    const membership = await prisma.membership.upsert({
      where: { orgId_userId: { orgId, userId: user.id } },
      create: { orgId, userId: user.id, role },
      update: { role },
    });

    await prisma.auditLog.create({
      data: {
        orgId,
        action: "scim.user.provisioned",
        targetType: "membership",
        targetId: membership.id,
        metadata: { email, role },
      },
    });

    reply.status(201).send({
      schemas: ["urn:ietf:params:scim:schemas:core:2.0:User"],
      id: user.id,
      userName: user.email,
      active: input.active !== false,
      roles: [{ value: membership.role }],
    });
  });

  app.patch("/Users/:id", async (request) => {
    requireScimToken(request, scimBearerToken);
    const orgId = getOrgId(request);
    const { id } = request.params as { id: string };
    const input = ScimPatchUserSchema.parse(request.body);

    if (input.active === false) {
      await prisma.membership.deleteMany({ where: { orgId, userId: id } });
      await prisma.auditLog.create({
        data: {
          orgId,
          action: "scim.user.deprovisioned",
          targetType: "user",
          targetId: id,
        },
      });
      return { ok: true };
    }

    if (input.role) {
      const updated = await prisma.membership.updateMany({
        where: { orgId, userId: id },
        data: { role: input.role },
      });
      if (updated.count === 0) {
        throw new AppError("NOT_FOUND", 404, "User not found in organization");
      }
      await prisma.auditLog.create({
        data: {
          orgId,
          action: "scim.user.role.updated",
          targetType: "user",
          targetId: id,
          metadata: { role: input.role },
        },
      });
    }

    return { ok: true };
  });

  app.delete("/Users/:id", async (request) => {
    requireScimToken(request, scimBearerToken);
    const orgId = getOrgId(request);
    const { id } = request.params as { id: string };
    await prisma.membership.deleteMany({ where: { orgId, userId: id } });
    await prisma.auditLog.create({
      data: {
        orgId,
        action: "scim.user.deprovisioned",
        targetType: "user",
        targetId: id,
      },
    });
    return { ok: true };
  });
}

function getScimBearerToken(app: FastifyInstance): string | null | undefined {
  return (app.env as { SCIM_BEARER_TOKEN?: string | null }).SCIM_BEARER_TOKEN;
}

function requireScimToken(request: FastifyRequest, expected?: string | null) {
  if (!expected) {
    throw new AppError("FORBIDDEN", 403, "SCIM not configured");
  }
  const header = request.headers.authorization;
  const raw = Array.isArray(header) ? header[0] : header;
  const token = raw?.startsWith("Bearer ") ? raw.slice("Bearer ".length).trim() : "";
  if (!token || token !== expected) {
    throw new AppError("UNAUTHORIZED", 401, "Unauthorized");
  }
}

function parseScimQuery(query: Record<string, unknown> | undefined) {
  const startIndex = Math.max(1, Number(query?.["startIndex"] || 1));
  const count = Math.min(100, Math.max(1, Number(query?.["count"] || 100)));
  const filterRaw = String(query?.["filter"] || "");
  const userName = extractUserNameFilter(filterRaw);
  return { startIndex, count, userName };
}

function extractUserNameFilter(filter: string) {
  const match = filter.match(/^userName eq "(.+)"$/i);
  if (!match) {
    return undefined;
  }
  return match[1]?.trim().toLowerCase();
}

function normalizeRole(value?: string) {
  const candidate = (value || "MEMBER").toUpperCase();
  if (candidate === "OWNER" || candidate === "ADMIN" || candidate === "MEMBER" || candidate === "BILLING" || candidate === "READONLY") {
    return candidate;
  }
  return "MEMBER";
}

async function findOrCreateUser(email: string) {
  const existing = await prisma.user.findUnique({ where: { email } });
  if (existing) {
    return existing;
  }
  const randomPasswordHash = await bcrypt.hash(randomToken(24), PASSWORD_COST);
  return prisma.user.create({
    data: {
      email,
      passwordHash: randomPasswordHash,
    },
  });
}
