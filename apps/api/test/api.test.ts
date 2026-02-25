import { beforeAll, afterAll, describe, expect, it, beforeEach } from "vitest";
import { buildApp } from "../src/app";
import { prisma } from "@saas/db";
import { resetDb, closeDb } from "./setup";
import { sha256 } from "../src/lib/crypto";
import Stripe from "stripe";
import fs from "fs";
import path from "path";

const stripe = new Stripe("sk_test_123", { apiVersion: "2023-10-16" });

beforeAll(async () => {
  process.env["NODE_ENV"] = "test";
  process.env["API_PORT"] = "0";
  process.env["API_BASE_URL"] = "http://localhost:4000";
  process.env["WEB_BASE_URL"] = "http://localhost:3000";
  process.env["DATABASE_URL"] = process.env["DATABASE_URL"] || "postgresql://postgres:postgres@localhost:5432/saas";
  process.env["REDIS_URL"] = process.env["REDIS_URL"] || "redis://localhost:6379";
  process.env["SESSION_SECRET"] = "test-session-secret-12345";
  process.env["STRIPE_SECRET_KEY"] = "sk_test_123";
  process.env["STRIPE_WEBHOOK_SECRET"] = "whsec_test";
});

beforeEach(async () => {
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
});

describe("webhook idempotency", () => {
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
    expect(res.statusCode).toBe(404);
    const body = res.json() as { error?: { code?: string; message?: string; requestId?: string } };
    expect(body.error?.code).toBe("NOT_FOUND");
    expect(typeof body.error?.requestId).toBe("string");
    await app.close();
  });
});
