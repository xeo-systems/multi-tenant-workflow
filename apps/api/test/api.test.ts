import { beforeAll, afterAll, describe, expect, it, beforeEach, vi } from "vitest";
import { buildApp } from "../src/app";
import { prisma } from "@saas/db";
import { resetDb, closeDb } from "./setup";
import { sha256 } from "../src/lib/crypto";
import Stripe from "stripe";
import fs from "fs";
import path from "path";
import bcrypt from "bcryptjs";
import { apiKeyNameSchema, emailSchema, orgNameSchema, passwordSchema } from "@saas/shared";
import { resetRateLimitStore } from "../src/middleware/rateLimit";
import * as readiness from "../src/lib/readiness";
import { QUEUE_RETRY_DEFAULTS, stripeQueue } from "../src/lib/queue";

const stripe = new Stripe("sk_test_123", { apiVersion: "2023-10-16" });

beforeAll(async () => {
  process.env["NODE_ENV"] = "test";
  process.env["API_PORT"] = "0";
  process.env["API_BASE_URL"] = "http://localhost:4000";
  process.env["WEB_BASE_URL"] = "http://localhost:3000";
  process.env["DATABASE_URL"] = process.env["DATABASE_URL"] || "postgresql://postgres:postgres@localhost:5432/saas";
  process.env["REDIS_URL"] = process.env["REDIS_URL"] || "redis://localhost:6379";
  process.env["SESSION_SECRET"] = "test-session-secret-12345-test-session-secret";
  process.env["STRIPE_SECRET_KEY"] = "sk_test_123";
  process.env["STRIPE_WEBHOOK_SECRET"] = "whsec_test";
});

beforeEach(async () => {
  process.env["API_KEY_RATE_LIMIT"] = "1000";
  process.env["API_KEY_RATE_LIMIT_WINDOW_SEC"] = "60";
  process.env["AUTH_RATE_LIMIT_WINDOW_SEC"] = "60";
  process.env["AUTH_LOGIN_IP_RATE_LIMIT"] = "10";
  process.env["AUTH_LOGIN_IDENTIFIER_RATE_LIMIT"] = "5";
  process.env["AUTH_REGISTER_IP_RATE_LIMIT"] = "5";
  resetRateLimitStore();
  await resetDb();
});

afterAll(async () => {
  await closeDb();
});

describe("tenant isolation", () => {
  it("prevents org A user from accessing org B", async () => {
    const orgA = await prisma.organization.create({ data: { name: "Org A" } });
    const orgB = await prisma.organization.create({ data: { name: "Org B" } });
    const user = await prisma.user.create({ data: { email: "a@example.com", passwordHash: "hash" } });
    await prisma.membership.create({ data: { orgId: orgA.id, userId: user.id, role: "ADMIN" } });

    const token = "sessiontoken";
    await prisma.session.create({
      data: {
        userId: user.id,
        tokenHash: sha256(token),
        expiresAt: new Date(Date.now() + 86400 * 1000),
      },
    });

    const app = buildApp();
    const res = await app.inject({
      method: "GET",
      url: "/org",
      headers: {
        "x-org-id": orgB.id,
        cookie: `sid=${token}`,
      },
    });

    expect(res.statusCode).toBe(403);
    await app.close();
  });

  it("prevents org A user from reading org B usage and audit logs", async () => {
    const orgA = await prisma.organization.create({ data: { name: "Org A Usage" } });
    const orgB = await prisma.organization.create({ data: { name: "Org B Usage" } });
    const user = await prisma.user.create({ data: { email: "usage-isolation@example.com", passwordHash: "hash" } });
    await prisma.membership.create({ data: { orgId: orgA.id, userId: user.id, role: "ADMIN" } });

    const token = "sessiontoken-usage";
    await prisma.session.create({
      data: {
        userId: user.id,
        tokenHash: sha256(token),
        expiresAt: new Date(Date.now() + 86400 * 1000),
      },
    });

    const app = buildApp();
    const usageRes = await app.inject({
      method: "GET",
      url: "/usage",
      headers: {
        "x-org-id": orgB.id,
        cookie: `sid=${token}`,
      },
    });
    const auditRes = await app.inject({
      method: "GET",
      url: "/audit",
      headers: {
        "x-org-id": orgB.id,
        cookie: `sid=${token}`,
      },
    });

    expect(usageRes.statusCode).toBe(403);
    expect(auditRes.statusCode).toBe(403);
    await app.close();
  });
});

describe("audit logging", () => {
  it("writes audit entries for api key create/revoke and member role change", async () => {
    const org = await prisma.organization.create({ data: { name: "Org Audit" } });
    const owner = await prisma.user.create({ data: { email: "owner-audit@example.com", passwordHash: "hash" } });
    const memberUser = await prisma.user.create({ data: { email: "member-audit@example.com", passwordHash: "hash" } });
    await prisma.membership.create({ data: { orgId: org.id, userId: owner.id, role: "OWNER" } });
    const member = await prisma.membership.create({ data: { orgId: org.id, userId: memberUser.id, role: "MEMBER" } });

    const token = "sessiontoken-audit";
    await prisma.session.create({
      data: {
        userId: owner.id,
        tokenHash: sha256(token),
        expiresAt: new Date(Date.now() + 86400 * 1000),
      },
    });

    const app = buildApp();
    const createRes = await app.inject({
      method: "POST",
      url: "/api-keys",
      headers: {
        "x-org-id": org.id,
        cookie: `sid=${token}`,
      },
      payload: { name: "audit_key" },
    });
    expect(createRes.statusCode).toBe(201);
    const created = createRes.json() as { id: string };

    const updateRoleRes = await app.inject({
      method: "PATCH",
      url: `/org/members/${member.id}`,
      headers: {
        "x-org-id": org.id,
        cookie: `sid=${token}`,
      },
      payload: { role: "READONLY" },
    });
    expect(updateRoleRes.statusCode).toBe(200);

    const revokeRes = await app.inject({
      method: "DELETE",
      url: `/api-keys/${created.id}`,
      headers: {
        "x-org-id": org.id,
        cookie: `sid=${token}`,
      },
    });
    expect(revokeRes.statusCode).toBe(200);

    const logs = await prisma.auditLog.findMany({
      where: { orgId: org.id },
      orderBy: { createdAt: "asc" },
    });
    const actions = logs.map((log) => log.action);
    expect(actions).toContain("apiKey.create");
    expect(actions).toContain("apiKey.revoke");
    expect(actions).toContain("org.member.update");
    await app.close();
  });
});

describe("rbac negative cases", () => {
  it("blocks MEMBER role from invite, role change, and key revoke", async () => {
    const org = await prisma.organization.create({ data: { name: "Org RBAC" } });
    const owner = await prisma.user.create({ data: { email: "owner-rbac@example.com", passwordHash: "hash" } });
    const memberUser = await prisma.user.create({ data: { email: "member-rbac@example.com", passwordHash: "hash" } });
    await prisma.membership.create({ data: { orgId: org.id, userId: owner.id, role: "OWNER" } });
    const memberMembership = await prisma.membership.create({
      data: { orgId: org.id, userId: memberUser.id, role: "MEMBER" },
    });

    const memberToken = "sessiontoken-rbac-member";
    await prisma.session.create({
      data: {
        userId: memberUser.id,
        tokenHash: sha256(memberToken),
        expiresAt: new Date(Date.now() + 86400 * 1000),
      },
    });

    const key = await prisma.apiKey.create({
      data: {
        orgId: org.id,
        name: "rbac_key",
        prefix: "ak_rbac_",
        hash: sha256("ak_rbac_secret"),
      },
    });

    const app = buildApp();

    const inviteRes = await app.inject({
      method: "POST",
      url: "/org/members",
      headers: {
        "x-org-id": org.id,
        cookie: `sid=${memberToken}`,
      },
      payload: { email: "new-user@example.com", role: "MEMBER" },
    });

    const changeRoleRes = await app.inject({
      method: "PATCH",
      url: `/org/members/${memberMembership.id}`,
      headers: {
        "x-org-id": org.id,
        cookie: `sid=${memberToken}`,
      },
      payload: { role: "ADMIN" },
    });

    const revokeRes = await app.inject({
      method: "DELETE",
      url: `/api-keys/${key.id}`,
      headers: {
        "x-org-id": org.id,
        cookie: `sid=${memberToken}`,
      },
    });

    expect(inviteRes.statusCode).toBe(403);
    expect(changeRoleRes.statusCode).toBe(403);
    expect(revokeRes.statusCode).toBe(403);
    await app.close();
  });
});

describe("readiness", () => {
  it("returns 200 when DB and Redis are healthy", async () => {
    const app = buildApp();
    const res = await app.inject({
      method: "GET",
      url: "/ready",
    });
    expect(res.statusCode).toBe(200);
    expect((res.json() as { status?: string }).status).toBe("ready");
    await app.close();
  });

  it("returns 503 when readiness checks fail", async () => {
    const dbSpy = vi.spyOn(readiness, "checkDbReady").mockRejectedValueOnce(new Error("db down"));
    const redisSpy = vi.spyOn(readiness, "checkRedisReady").mockResolvedValue("PONG");
    const app = buildApp();
    const res = await app.inject({
      method: "GET",
      url: "/ready",
    });
    expect(res.statusCode).toBe(503);
    expect((res.json() as { error?: { code?: string } }).error?.code).toBe("INTERNAL");
    dbSpy.mockRestore();
    redisSpy.mockRestore();
    await app.close();
  });
});

describe("internal admin summary endpoint", () => {
  it("requires INTERNAL_ADMIN_TOKEN and returns org summary", async () => {
    const previous = process.env["INTERNAL_ADMIN_TOKEN"];
    process.env["INTERNAL_ADMIN_TOKEN"] = "internal-secret-token";

    const org = await prisma.organization.create({ data: { name: "Internal Org", plan: "free", planLimit: 1000 } });
    const user = await prisma.user.create({ data: { email: "internal-owner@example.com", passwordHash: "hash" } });
    await prisma.membership.create({ data: { orgId: org.id, userId: user.id, role: "OWNER" } });
    await prisma.usageDaily.create({
      data: {
        orgId: org.id,
        metric: "api_requests",
        date: new Date(Date.UTC(2026, 0, 1)),
        quantity: 12,
      },
    });

    const app = buildApp();
    const denied = await app.inject({
      method: "GET",
      url: `/admin/orgs/${org.id}/summary`,
    });
    expect(denied.statusCode).toBe(401);

    const ok = await app.inject({
      method: "GET",
      url: `/admin/orgs/${org.id}/summary`,
      headers: { authorization: "Bearer internal-secret-token" },
    });
    expect(ok.statusCode).toBe(200);
    const body = ok.json() as {
      org?: { id?: string };
      memberCount?: number;
      usage?: { limit?: number };
    };
    expect(body.org?.id).toBe(org.id);
    expect(body.memberCount).toBe(1);
    expect(body.usage?.limit).toBe(1000);

    process.env["INTERNAL_ADMIN_TOKEN"] = previous;
    await app.close();
  });
});

describe("security headers", () => {
  it("returns baseline security headers on API responses", async () => {
    const app = buildApp();
    const res = await app.inject({
      method: "GET",
      url: "/health",
    });
    expect(res.statusCode).toBe(200);
    expect(res.headers["x-content-type-options"]).toBe("nosniff");
    expect(res.headers["referrer-policy"]).toBe("strict-origin-when-cross-origin");
    expect(res.headers["x-frame-options"]).toBe("DENY");
    await app.close();
  });
});

describe("webhook idempotency", () => {
  it("enqueues stripe jobs with retry/backoff policy", async () => {
    const addSpy = vi.spyOn(stripeQueue, "add").mockResolvedValue(undefined as any);
    const app = buildApp();
    const payload = fs.readFileSync(
      path.join(__dirname, "fixtures/stripe/subscription_updated.json"),
      "utf8"
    );
    const header = stripe.webhooks.generateTestHeaderString({
      payload,
      secret: "whsec_test",
    });

    const res = await app.inject({
      method: "POST",
      url: "/billing/webhook",
      headers: {
        "stripe-signature": header,
        "content-type": "application/json",
      },
      payload,
    });

    expect(res.statusCode).toBe(200);
    expect(addSpy).toHaveBeenCalledTimes(1);
    expect(addSpy).toHaveBeenCalledWith(
      "stripe-event",
      expect.objectContaining({ event: expect.objectContaining({ id: "evt_test_sub_updated" }) }),
      expect.objectContaining({
        jobId: "evt_test_sub_updated",
        attempts: QUEUE_RETRY_DEFAULTS.attempts,
        backoff: {
          type: "exponential",
          delay: QUEUE_RETRY_DEFAULTS.backoffDelayMs,
        },
      })
    );

    addSpy.mockRestore();
    await app.close();
  });

  it("stores event once", async () => {
    const app = buildApp();
    const payload = fs.readFileSync(
      path.join(__dirname, "fixtures/stripe/subscription_updated.json"),
      "utf8"
    );
    const header = stripe.webhooks.generateTestHeaderString({
      payload,
      secret: "whsec_test",
    });

    const res1 = await app.inject({
      method: "POST",
      url: "/billing/webhook",
      headers: {
        "stripe-signature": header,
        "content-type": "application/json",
      },
      payload,
    });

    const res2 = await app.inject({
      method: "POST",
      url: "/billing/webhook",
      headers: {
        "stripe-signature": header,
        "content-type": "application/json",
      },
      payload,
    });

    expect(res1.statusCode).toBe(200);
    expect(res2.statusCode).toBe(200);

    const count = await prisma.stripeEvent.count({ where: { stripeId: "evt_test_sub_updated" } });
    expect(count).toBe(1);

    await app.close();
  });

  it("accepts valid saved payload", async () => {
    const app = buildApp();
    const payload = fs.readFileSync(
      path.join(__dirname, "fixtures/stripe/checkout_completed.json"),
      "utf8"
    );
    const header = stripe.webhooks.generateTestHeaderString({
      payload,
      secret: "whsec_test",
    });

    const res = await app.inject({
      method: "POST",
      url: "/billing/webhook",
      headers: {
        "stripe-signature": header,
        "content-type": "application/json",
      },
      payload,
    });

    expect(res.statusCode).toBe(200);
    const count = await prisma.stripeEvent.count({ where: { stripeId: "evt_test_checkout" } });
    expect(count).toBe(1);

    await app.close();
  });

  it("rejects invalid signature", async () => {
    const app = buildApp();
    const payload = fs.readFileSync(
      path.join(__dirname, "fixtures/stripe/subscription_updated.json"),
      "utf8"
    );

    const res = await app.inject({
      method: "POST",
      url: "/billing/webhook",
      headers: {
        "stripe-signature": "invalid",
        "content-type": "application/json",
      },
      payload,
    });

    expect(res.statusCode).toBe(400);
    await app.close();
  });

  it("rolls back persisted event if enqueue fails", async () => {
    const addSpy = vi.spyOn(stripeQueue, "add").mockRejectedValue(new Error("queue unavailable"));
    const app = buildApp();
    const payload = fs.readFileSync(
      path.join(__dirname, "fixtures/stripe/subscription_updated.json"),
      "utf8"
    );
    const header = stripe.webhooks.generateTestHeaderString({
      payload,
      secret: "whsec_test",
    });

    const res = await app.inject({
      method: "POST",
      url: "/billing/webhook",
      headers: {
        "stripe-signature": header,
        "content-type": "application/json",
      },
      payload,
    });

    expect(res.statusCode).toBe(500);
    const count = await prisma.stripeEvent.count({ where: { stripeId: "evt_test_sub_updated" } });
    expect(count).toBe(0);

    addSpy.mockRestore();
    await app.close();
  });
});

describe("api key auth", () => {
  it("allows access with valid key", async () => {
    const org = await prisma.organization.create({ data: { name: "Org" } });
    const key = "ak_test_123456789";
    await prisma.apiKey.create({
      data: {
        orgId: org.id,
        name: "test",
        prefix: key.slice(0, 8),
        hash: sha256(key),
      },
    });

    const app = buildApp();
    const res = await app.inject({
      method: "GET",
      url: "/data",
      headers: {
        authorization: `Bearer ${key}`,
        "x-org-id": org.id,
      },
    });

    expect(res.statusCode).toBe(200);
    const data = res.json();
    expect(data.orgId).toBe(org.id);

    await app.close();
  });

  it("rejects invalid key", async () => {
    const org = await prisma.organization.create({ data: { name: "Org" } });
    const app = buildApp();
    const res = await app.inject({
      method: "GET",
      url: "/data",
      headers: {
        authorization: "Bearer invalid_key",
        "x-org-id": org.id,
      },
    });
    expect(res.statusCode).toBe(401);
    await app.close();
  });

  it("rejects revoked key", async () => {
    const org = await prisma.organization.create({ data: { name: "Org" } });
    const key = "ak_revoked_123456";
    await prisma.apiKey.create({
      data: {
        orgId: org.id,
        name: "revoked",
        prefix: key.slice(0, 8),
        hash: sha256(key),
        revokedAt: new Date(),
      },
    });
    const app = buildApp();
    const res = await app.inject({
      method: "GET",
      url: "/data",
      headers: {
        authorization: `Bearer ${key}`,
        "x-org-id": org.id,
      },
    });
    expect(res.statusCode).toBe(401);
    await app.close();
  });

  it("blocks cross-org key usage", async () => {
    const orgA = await prisma.organization.create({ data: { name: "Org A" } });
    const orgB = await prisma.organization.create({ data: { name: "Org B" } });
    const key = "ak_cross_org_123456";
    await prisma.apiKey.create({
      data: {
        orgId: orgA.id,
        name: "org-a",
        prefix: key.slice(0, 8),
        hash: sha256(key),
      },
    });
    const app = buildApp();
    const res = await app.inject({
      method: "GET",
      url: "/data",
      headers: {
        authorization: `Bearer ${key}`,
        "x-org-id": orgB.id,
      },
    });
    expect(res.statusCode).toBe(401);
    await app.close();
  });
});

describe("tenant isolation on api keys", () => {
  it("prevents org A from revoking org B key", async () => {
    const orgA = await prisma.organization.create({ data: { name: "Org A" } });
    const orgB = await prisma.organization.create({ data: { name: "Org B" } });
    const user = await prisma.user.create({ data: { email: "owner@example.com", passwordHash: "hash" } });
    await prisma.membership.create({ data: { orgId: orgA.id, userId: user.id, role: "OWNER" } });

    const key = "ak_test_orgb_123456";
    const apiKey = await prisma.apiKey.create({
      data: {
        orgId: orgB.id,
        name: "orgb",
        prefix: key.slice(0, 8),
        hash: sha256(key),
      },
    });

    const token = "sessiontoken-owner";
    await prisma.session.create({
      data: {
        userId: user.id,
        tokenHash: sha256(token),
        expiresAt: new Date(Date.now() + 86400 * 1000),
      },
    });

    const app = buildApp();
    const res = await app.inject({
      method: "DELETE",
      url: `/api-keys/${apiKey.id}`,
      headers: {
        "x-org-id": orgA.id,
        cookie: `sid=${token}`,
      },
    });

    expect(res.statusCode).toBe(404);
    await app.close();
  });
});

describe("usage enforcement", () => {
  it("rejects when plan limit exceeded", async () => {
    const org = await prisma.organization.create({ data: { name: "Org", planLimit: 1 } });
    const key = "ak_limit_123456";
    await prisma.apiKey.create({
      data: {
        orgId: org.id,
        name: "limit",
        prefix: key.slice(0, 8),
        hash: sha256(key),
      },
    });

    const app = buildApp();
    const res1 = await app.inject({
      method: "GET",
      url: "/data",
      headers: {
        authorization: `Bearer ${key}`,
        "x-org-id": org.id,
      },
    });
    const res2 = await app.inject({
      method: "GET",
      url: "/data",
      headers: {
        authorization: `Bearer ${key}`,
        "x-org-id": org.id,
      },
    });

    expect(res1.statusCode).toBe(200);
    expect(res2.statusCode).toBe(429);
    await app.close();
  });

  it("tracks concurrent increments safely", async () => {
    const org = await prisma.organization.create({ data: { name: "Org", planLimit: 1000 } });
    const key = "ak_concurrent_123456";
    await prisma.apiKey.create({
      data: {
        orgId: org.id,
        name: "concurrent",
        prefix: key.slice(0, 8),
        hash: sha256(key),
      },
    });

    const app = buildApp();
    const requests = Array.from({ length: 10 }, () =>
      app.inject({
        method: "GET",
        url: "/data",
        headers: {
          authorization: `Bearer ${key}`,
          "x-org-id": org.id,
        },
      })
    );
    const results = await Promise.all(requests);
    results.forEach((res) => expect(res.statusCode).toBe(200));

    const today = new Date();
    const date = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate()));
    const usage = await prisma.usageDaily.findUnique({
      where: { orgId_metric_date: { orgId: org.id, metric: "api_requests", date } },
    });
    expect(usage?.quantity).toBe(10);
    await app.close();
  });

  it("resets usage on billing cycle", async () => {
    const org = await prisma.organization.create({ data: { name: "Org", planLimit: 1 } });
    const key = "ak_cycle_123456";
    await prisma.apiKey.create({
      data: {
        orgId: org.id,
        name: "cycle",
        prefix: key.slice(0, 8),
        hash: sha256(key),
      },
    });

    const now = new Date();
    const cycleStart = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
    await prisma.subscription.create({
      data: {
        orgId: org.id,
        stripeCustomerId: "cus_cycle",
        stripeSubscriptionId: "sub_cycle",
        status: "active",
        currentPeriodStart: cycleStart,
        currentPeriodEnd: new Date(cycleStart.getTime() + 30 * 24 * 60 * 60 * 1000),
      },
    });

    const yesterday = new Date(cycleStart.getTime() - 24 * 60 * 60 * 1000);
    await prisma.usageDaily.create({
      data: {
        orgId: org.id,
        metric: "api_requests",
        date: yesterday,
        quantity: 1,
      },
    });

    const app = buildApp();
    const res = await app.inject({
      method: "GET",
      url: "/data",
      headers: {
        authorization: `Bearer ${key}`,
        "x-org-id": org.id,
      },
    });

    expect(res.statusCode).toBe(200);
    await app.close();
  });

  it("does not count session-auth dashboard calls as billable usage", async () => {
    const org = await prisma.organization.create({ data: { name: "Org Dashboard", planLimit: 1000 } });
    const user = await prisma.user.create({ data: { email: "dash@example.com", passwordHash: "hash" } });
    await prisma.membership.create({ data: { orgId: org.id, userId: user.id, role: "OWNER" } });
    const token = "sessiontoken-dashboard";
    await prisma.session.create({
      data: {
        userId: user.id,
        tokenHash: sha256(token),
        expiresAt: new Date(Date.now() + 86400 * 1000),
      },
    });

    const app = buildApp();
    const orgRes = await app.inject({
      method: "GET",
      url: "/org",
      headers: {
        "x-org-id": org.id,
        cookie: `sid=${token}`,
      },
    });
    expect(orgRes.statusCode).toBe(200);

    const today = new Date();
    const date = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate()));
    const usage = await prisma.usageDaily.findUnique({
      where: { orgId_metric_date: { orgId: org.id, metric: "api_requests", date } },
    });
    expect(usage).toBeNull();
    await app.close();
  });
});

describe("error shape", () => {
  it("includes requestId in error response", async () => {
    const app = buildApp();
    const res = await app.inject({
      method: "GET",
      url: "/org",
      headers: {
        "x-org-id": "missing",
      },
    });
    expect(res.statusCode).toBe(401);
    const body = res.json() as { error?: { code?: string; message?: string; requestId?: string } };
    expect(body.error?.code).toBe("UNAUTHORIZED");
    expect(typeof body.error?.requestId).toBe("string");
    await app.close();
  });
});

describe("auth logout", () => {
  it("returns success even when usage recording fails", async () => {
    const user = await prisma.user.create({ data: { email: "logout@example.com", passwordHash: "hash" } });
    const token = "sessiontoken-logout";
    await prisma.session.create({
      data: {
        userId: user.id,
        tokenHash: sha256(token),
        expiresAt: new Date(Date.now() + 86400 * 1000),
      },
    });

    const app = buildApp();
    const res = await app.inject({
      method: "POST",
      url: "/auth/logout",
      headers: {
        "x-org-id": "nonexistent-org-id",
        cookie: `sid=${token}`,
      },
    });

    expect(res.statusCode).toBe(200);
    const body = res.json() as { ok?: boolean };
    expect(body.ok).toBe(true);
    await app.close();
  });

  it("invalidates session so protected routes reject after logout", async () => {
    const org = await prisma.organization.create({ data: { name: "Logout Org" } });
    const user = await prisma.user.create({ data: { email: "logout2@example.com", passwordHash: "hash" } });
    await prisma.membership.create({ data: { orgId: org.id, userId: user.id, role: "OWNER" } });

    const token = "sessiontoken-logout-2";
    await prisma.session.create({
      data: {
        userId: user.id,
        tokenHash: sha256(token),
        expiresAt: new Date(Date.now() + 86400 * 1000),
      },
    });

    const app = buildApp();
    const logoutRes = await app.inject({
      method: "POST",
      url: "/auth/logout",
      headers: {
        "x-org-id": org.id,
        host: "localhost:3000",
        cookie: `sid=${token}`,
      },
    });
    expect(logoutRes.statusCode).toBe(200);

    const protectedRes = await app.inject({
      method: "GET",
      url: "/org",
      headers: {
        "x-org-id": org.id,
        cookie: `sid=${token}`,
      },
    });
    expect(protectedRes.statusCode).toBe(401);
    await app.close();
  });

  it("rejects auth mutation when origin does not match trusted web origin", async () => {
    const app = buildApp();
    const res = await app.inject({
      method: "POST",
      url: "/auth/login",
      headers: {
        origin: "https://malicious.example.com",
      },
      payload: {
        email: "nobody@example.com",
        password: "StrongPass1!",
      },
    });
    expect(res.statusCode).toBe(403);
    expect((res.json() as { error?: { code?: string; message?: string } }).error?.code).toBe("FORBIDDEN");
    await app.close();
  });
});

describe("session cookie security", () => {
  it("sets secure cookie flags in production mode", async () => {
    const prevNodeEnv = process.env["NODE_ENV"];
    const prevCookieSecure = process.env["COOKIE_SECURE"];
    const prevCookieDomain = process.env["COOKIE_DOMAIN"];
    const prevSessionSecret = process.env["SESSION_SECRET"];
    const prevApiBase = process.env["API_BASE_URL"];
    const prevStripeSecret = process.env["STRIPE_SECRET_KEY"];
    const prevStripeWebhook = process.env["STRIPE_WEBHOOK_SECRET"];
    const prevWebBase = process.env["WEB_BASE_URL"];
    try {
      process.env["NODE_ENV"] = "production";
      process.env["COOKIE_SECURE"] = "false";
      process.env["COOKIE_DOMAIN"] = "example.com";
      process.env["SESSION_SECRET"] = "production-session-secret-0123456789ABCDEF";
      process.env["STRIPE_SECRET_KEY"] = "sk_test_123";
      process.env["STRIPE_WEBHOOK_SECRET"] = "whsec_test";
      process.env["WEB_BASE_URL"] = "https://app.example.com";

      const app = buildApp();
      const res = await app.inject({
        method: "POST",
        url: "/auth/register",
        headers: {
          host: "app.example.com",
          origin: "https://app.example.com",
        },
        payload: {
          email: "cookie-secure@example.com",
          password: "StrongPass1!",
          orgName: "Cookie Org",
        },
      });

      expect(res.statusCode).toBe(201);
      const cookieHeader = res.headers["set-cookie"];
      const cookie = Array.isArray(cookieHeader) ? cookieHeader[0] : cookieHeader;
      expect(cookie).toBeTruthy();
      expect(cookie).toContain("HttpOnly");
      expect(cookie).toContain("Secure");
      expect(cookie).toContain("SameSite=None");
      expect(cookie).toContain("Path=/");
      expect(cookie).toContain("Domain=example.com");

      await app.close();
    } finally {
      process.env["NODE_ENV"] = prevNodeEnv;
      process.env["COOKIE_SECURE"] = prevCookieSecure;
      process.env["COOKIE_DOMAIN"] = prevCookieDomain;
      process.env["SESSION_SECRET"] = prevSessionSecret;
      process.env["API_BASE_URL"] = prevApiBase;
      process.env["STRIPE_SECRET_KEY"] = prevStripeSecret;
      process.env["STRIPE_WEBHOOK_SECRET"] = prevStripeWebhook;
      process.env["WEB_BASE_URL"] = prevWebBase;
    }
  });
});

describe("auth login", () => {
  it("returns a default orgId for the authenticated user", async () => {
    const org = await prisma.organization.create({ data: { name: "Org Login" } });
    const password = "password123";
    const passwordHash = await bcrypt.hash(password, 12);
    const user = await prisma.user.create({
      data: { email: "login-org@example.com", passwordHash },
    });
    await prisma.membership.create({
      data: { orgId: org.id, userId: user.id, role: "OWNER" },
    });

    const app = buildApp();
    const res = await app.inject({
      method: "POST",
      url: "/auth/login",
      payload: { email: user.email, password },
    });

    expect(res.statusCode).toBe(200);
    const body = res.json() as { userId?: string; orgId?: string };
    expect(body.userId).toBe(user.id);
    expect(body.orgId).toBe(org.id);
    await app.close();
  });

  it("returns the same generic unauthorized error for missing user and wrong password", async () => {
    const org = await prisma.organization.create({ data: { name: "Org Generic" } });
    const passwordHash = await bcrypt.hash("Password123!", 12);
    const user = await prisma.user.create({
      data: { email: "generic-login@example.com", passwordHash },
    });
    await prisma.membership.create({
      data: { orgId: org.id, userId: user.id, role: "OWNER" },
    });

    const app = buildApp();
    const missingUser = await app.inject({
      method: "POST",
      url: "/auth/login",
      payload: { email: "missing-user@example.com", password: "WrongPass1!" },
    });
    const wrongPassword = await app.inject({
      method: "POST",
      url: "/auth/login",
      payload: { email: user.email, password: "WrongPass1!" },
    });

    expect(missingUser.statusCode).toBe(401);
    expect(wrongPassword.statusCode).toBe(401);
    expect((missingUser.json() as { error?: { message?: string } }).error?.message).toBe("Invalid credentials");
    expect((wrongPassword.json() as { error?: { message?: string } }).error?.message).toBe("Invalid credentials");
    await app.close();
  });
});

describe("password hashing", () => {
  it("stores hashed password and verifies with bcrypt", async () => {
    const app = buildApp();
    const registerRes = await app.inject({
      method: "POST",
      url: "/auth/register",
      payload: {
        email: "hash-check@example.com",
        password: "StrongPass1!",
        orgName: "Hash Org",
      },
    });
    expect(registerRes.statusCode).toBe(201);

    const created = await prisma.user.findUnique({ where: { email: "hash-check@example.com" } });
    expect(created).toBeTruthy();
    expect(created?.passwordHash).not.toBe("StrongPass1!");
    expect(created?.passwordHash.startsWith("$2")).toBe(true);
    const valid = await bcrypt.compare("StrongPass1!", created!.passwordHash);
    expect(valid).toBe(true);
    await app.close();
  });
});

describe("validation standards", () => {
  it("validates email schema for valid and invalid addresses", () => {
    expect(emailSchema.safeParse("User@Example.com ").success).toBe(true);
    expect(emailSchema.safeParse("bad-email").success).toBe(false);
    expect(emailSchema.safeParse("user@mailinator.com").success).toBe(false);
  });

  it("rejects weak password and accepts strong password", async () => {
    expect(passwordSchema.safeParse("weakpass").success).toBe(false);
    expect(passwordSchema.safeParse("StrongPass1!").success).toBe(true);

    const app = buildApp();
    const weak = await app.inject({
      method: "POST",
      url: "/auth/register",
      payload: {
        email: "weak@example.com",
        password: "weakpass",
        orgName: "Valid Org",
      },
    });
    expect(weak.statusCode).toBe(400);
    expect((weak.json() as { error?: { field?: string } }).error?.field).toBe("password");

    const strong = await app.inject({
      method: "POST",
      url: "/auth/register",
      payload: {
        email: "strong@example.com",
        password: "StrongPass1!",
        orgName: "Valid Org",
      },
    });
    expect(strong.statusCode).toBe(201);
    await app.close();
  });

  it("rejects invalid org name in register", async () => {
    expect(orgNameSchema.safeParse("!").success).toBe(false);
    const app = buildApp();
    const res = await app.inject({
      method: "POST",
      url: "/auth/register",
      payload: {
        email: "org-invalid@example.com",
        password: "StrongPass1!",
        orgName: "!!",
      },
    });
    expect(res.statusCode).toBe(400);
    expect((res.json() as { error?: { field?: string } }).error?.field).toBe("orgName");
    await app.close();
  });

  it("rejects invalid api key name", async () => {
    expect(apiKeyNameSchema.safeParse("bad key").success).toBe(false);
    const org = await prisma.organization.create({ data: { name: "Org Keys" } });
    const passwordHash = await bcrypt.hash("StrongPass1!", 12);
    const user = await prisma.user.create({ data: { email: "keys@example.com", passwordHash } });
    await prisma.membership.create({ data: { orgId: org.id, userId: user.id, role: "OWNER" } });
    const token = "sessiontoken-keys";
    await prisma.session.create({
      data: {
        userId: user.id,
        tokenHash: sha256(token),
        expiresAt: new Date(Date.now() + 86400 * 1000),
      },
    });

    const app = buildApp();
    const res = await app.inject({
      method: "POST",
      url: "/api-keys",
      headers: {
        "x-org-id": org.id,
        cookie: `sid=${token}`,
      },
      payload: { name: "bad key" },
    });
    expect(res.statusCode).toBe(400);
    expect((res.json() as { error?: { field?: string } }).error?.field).toBe("name");
    await app.close();
  });
});

describe("rate limits", () => {
  it("returns 429 when login attempts exceed identifier threshold", async () => {
    process.env["AUTH_LOGIN_IP_RATE_LIMIT"] = "100";
    process.env["AUTH_LOGIN_IDENTIFIER_RATE_LIMIT"] = "2";
    process.env["AUTH_RATE_LIMIT_WINDOW_SEC"] = "60";

    const passwordHash = await bcrypt.hash("StrongPass1!", 12);
    await prisma.user.create({
      data: {
        email: "ratelimit-login@example.com",
        passwordHash,
      },
    });

    const app = buildApp();
    const req = {
      method: "POST" as const,
      url: "/auth/login",
      headers: { "x-forwarded-for": "198.51.100.44" },
      payload: { email: "ratelimit-login@example.com", password: "WrongPass1!" },
    };

    const res1 = await app.inject(req);
    const res2 = await app.inject(req);
    const res3 = await app.inject(req);

    expect(res1.statusCode).toBe(401);
    expect(res2.statusCode).toBe(401);
    expect(res3.statusCode).toBe(429);
    const body = res3.json() as { error?: { code?: string; message?: string; retryAfterSec?: number } };
    expect(body.error?.code).toBe("RATE_LIMIT");
    expect(body.error?.message).toBe("Too many login attempts. Please try again later.");
    expect(typeof body.error?.retryAfterSec).toBe("number");
    expect(res3.headers["retry-after"]).toBeDefined();

    await app.close();
  });

  it("returns 429 for api key rate limit with stable error shape", async () => {
    process.env["API_KEY_RATE_LIMIT"] = "2";
    process.env["API_KEY_RATE_LIMIT_WINDOW_SEC"] = "60";

    const org = await prisma.organization.create({ data: { name: "Rate Limit Org", planLimit: 1000 } });
    const key = "ak_rate_limit_test_123456";
    await prisma.apiKey.create({
      data: {
        orgId: org.id,
        name: "rate_limit_test",
        prefix: key.slice(0, 8),
        hash: sha256(key),
      },
    });

    const app = buildApp();
    const headers = {
      authorization: `Bearer ${key}`,
      "x-org-id": org.id,
    };

    const res1 = await app.inject({ method: "GET", url: "/data", headers });
    const res2 = await app.inject({ method: "GET", url: "/data", headers });
    const res3 = await app.inject({ method: "GET", url: "/data", headers });

    expect(res1.statusCode).toBe(200);
    expect(res2.statusCode).toBe(200);
    expect(res3.statusCode).toBe(429);
    const body = res3.json() as { error?: { code?: string; message?: string; requestId?: string } };
    expect(body.error?.code).toBe("RATE_LIMIT");
    expect(body.error?.message).toBe("API key rate limit exceeded");
    expect(typeof body.error?.requestId).toBe("string");
    expect(body.error?.message?.toLowerCase()).not.toContain("prisma");
    expect(res3.headers["retry-after"]).toBeDefined();

    await app.close();
  });

  it("resets auth login rate limit after window", async () => {
    process.env["AUTH_LOGIN_IP_RATE_LIMIT"] = "100";
    process.env["AUTH_LOGIN_IDENTIFIER_RATE_LIMIT"] = "1";
    process.env["AUTH_RATE_LIMIT_WINDOW_SEC"] = "3";

    const passwordHash = await bcrypt.hash("StrongPass1!", 12);
    await prisma.user.create({
      data: {
        email: "ratelimit-reset@example.com",
        passwordHash,
      },
    });

    const app = buildApp();
    const req = {
      method: "POST" as const,
      url: "/auth/login",
      headers: { "x-forwarded-for": "198.51.100.99" },
      payload: { email: "ratelimit-reset@example.com", password: "WrongPass1!" },
    };

    const first = await app.inject(req);
    const limited = await app.inject(req);
    expect(first.statusCode).toBe(401);
    expect(limited.statusCode).toBe(429);

    await new Promise((resolve) => setTimeout(resolve, 3200));
    const afterReset = await app.inject(req);
    expect(afterReset.statusCode).toBe(401);

    await app.close();
  }, 10000);
});
