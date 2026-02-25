import { FastifyInstance } from "fastify";
import { requireRole, requireUser } from "../middleware/auth";
import { enforceUsageLimit } from "../middleware/usage";
import { MembershipInviteSchema, RoleSchema } from "@saas/shared";
import { addMember, getOrg, listMembers, removeMember, updateMember } from "../services/org";

export async function orgRoutes(app: FastifyInstance) {
  app.get("/", { preHandler: [enforceUsageLimit, requireUser] }, async (request) => {
    const org = await getOrg({
      orgId: request.auth!.orgId,
      userId: request.auth!.userId,
      role: request.auth!.role,
      requestId: request.id,
      ip: request.ip,
      userAgent: readUserAgent(request.headers["user-agent"]),
    });
    return { org, role: request.auth!.role };
  });

  app.get("/members", { preHandler: [enforceUsageLimit, requireUser] }, async (request) => {
    const members = await listMembers({
      orgId: request.auth!.orgId,
      userId: request.auth!.userId,
      role: request.auth!.role,
      requestId: request.id,
      ip: request.ip,
      userAgent: readUserAgent(request.headers["user-agent"]),
    });
    return members.map((member: { id: string; role: string; user: { id: string; email: string }; createdAt: Date }) => ({
      id: member.id,
      role: member.role,
      user: { id: member.user.id, email: member.user.email },
      createdAt: member.createdAt,
    }));
  });

  app.post("/members", { preHandler: [enforceUsageLimit, requireUser, (req) => requireRole(req, ["OWNER", "ADMIN"]) ] }, async (request, reply) => {
    const input = MembershipInviteSchema.parse(request.body);
    const membership = await addMember(
      {
        orgId: request.auth!.orgId,
        userId: request.auth!.userId,
        role: request.auth!.role,
        requestId: request.id,
        ip: request.ip,
        userAgent: readUserAgent(request.headers["user-agent"]),
      },
      input.email,
      input.role
    );

    reply.status(201).send(membership);
  });

  app.patch("/members/:memberId", { preHandler: [enforceUsageLimit, requireUser, (req) => requireRole(req, ["OWNER", "ADMIN"]) ] }, async (request) => {
    const memberId = request.params as { memberId: string };
    const input = RoleSchema.parse((request.body as { role?: string })?.role);

    const updated = await updateMember(
      {
        orgId: request.auth!.orgId,
        userId: request.auth!.userId,
        role: request.auth!.role,
        requestId: request.id,
        ip: request.ip,
        userAgent: readUserAgent(request.headers["user-agent"]),
      },
      memberId.memberId,
      input
    );
    return updated;
  });

  app.delete("/members/:memberId", { preHandler: [enforceUsageLimit, requireUser, (req) => requireRole(req, ["OWNER", "ADMIN"]) ] }, async (request) => {
    const { memberId } = request.params as { memberId: string };
    await removeMember(
      {
        orgId: request.auth!.orgId,
        userId: request.auth!.userId,
        role: request.auth!.role,
        requestId: request.id,
        ip: request.ip,
        userAgent: readUserAgent(request.headers["user-agent"]),
      },
      memberId
    );
    return { ok: true };
  });
}

function readUserAgent(value: string | string[] | undefined) {
  if (Array.isArray(value)) {
    return value[0];
  }
  return value;
}
