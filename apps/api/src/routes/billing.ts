import { FastifyInstance } from "fastify";
import Stripe from "stripe";
import { prisma } from "../lib/prisma";
import { AppError } from "../lib/errors";
import { requireRole, requireUser } from "../middleware/auth";
import { enforceUsageLimit } from "../middleware/usage";
import { stripeQueue } from "../lib/queue";
import { writeAudit } from "../services/audit";

export async function billingRoutes(app: FastifyInstance) {
  const env = app.env;
  const stripeKey = env.STRIPE_SECRET_KEY;
  const stripe = stripeKey ? new Stripe(stripeKey, { apiVersion: "2023-10-16" }) : null;

  app.get("/status", { preHandler: [enforceUsageLimit, requireUser] }, async (request) => {
    const org = await prisma.organization.findUnique({
      where: { id: request.auth!.orgId },
      select: { plan: true, planLimit: true },
    });
    if (!org) {
      throw new AppError("NOT_FOUND", 404, "Organization not found");
    }

    const subscription = await prisma.subscription.findFirst({
      where: { orgId: request.auth!.orgId },
      orderBy: { createdAt: "desc" },
    });

    return {
      billingConfigured: Boolean(stripe),
      plan: org.plan,
      planLimit: org.planLimit,
      subscription: subscription
        ? {
            status: subscription.status,
            currentPeriodStart: subscription.currentPeriodStart,
            currentPeriodEnd: subscription.currentPeriodEnd,
          }
        : null,
    };
  });

  app.post("/checkout", { preHandler: [enforceUsageLimit, requireUser, (req) => requireRole(req, ["OWNER", "ADMIN", "BILLING"]) ] }, async (request) => {
    if (!stripe) {
      throw new AppError("BAD_REQUEST", 400, "Stripe not configured");
    }
    if (!env.STRIPE_PRICE_ID || !env.STRIPE_SUCCESS_URL || !env.STRIPE_CANCEL_URL) {
      throw new AppError("BAD_REQUEST", 400, "Stripe price or URLs missing");
    }

    const orgId = request.auth!.orgId;
    const existing = await prisma.subscription.findFirst({ where: { orgId } });

    let customerId = existing?.stripeCustomerId;
    if (!customerId) {
      const customer = await stripe.customers.create({
        metadata: { orgId },
      });
      customerId = customer.id;
    }

    const session = await stripe.checkout.sessions.create({
      mode: "subscription",
      customer: customerId,
      line_items: [{ price: env.STRIPE_PRICE_ID, quantity: 1 }],
      success_url: env.STRIPE_SUCCESS_URL,
      cancel_url: env.STRIPE_CANCEL_URL,
      metadata: { orgId },
    });

    await writeAudit(
      {
        orgId: orgId,
        userId: request.auth!.userId,
        role: request.auth!.role,
        requestId: request.id,
        ip: request.ip,
        userAgent: readUserAgent(request.headers["user-agent"]),
      },
      "billing.checkout.initiated",
      "organization",
      orgId
    );

    return { url: session.url };
  });

  app.post("/portal", { preHandler: [enforceUsageLimit, requireUser, (req) => requireRole(req, ["OWNER", "ADMIN", "BILLING"]) ] }, async (request) => {
    if (!stripe) {
      throw new AppError("BAD_REQUEST", 400, "Stripe not configured");
    }
    if (!env.STRIPE_PORTAL_RETURN_URL) {
      throw new AppError("BAD_REQUEST", 400, "Stripe portal return URL missing");
    }

    const subscription = await prisma.subscription.findFirst({ where: { orgId: request.auth!.orgId } });
    if (!subscription) {
      throw new AppError("NOT_FOUND", 404, "Subscription not found");
    }

    const session = await stripe.billingPortal.sessions.create({
      customer: subscription.stripeCustomerId,
      return_url: env.STRIPE_PORTAL_RETURN_URL,
    });

    await writeAudit(
      {
        orgId: request.auth!.orgId,
        userId: request.auth!.userId,
        role: request.auth!.role,
        requestId: request.id,
        ip: request.ip,
        userAgent: readUserAgent(request.headers["user-agent"]),
      },
      "billing.portal.opened",
      "subscription",
      subscription.id
    );

    return { url: session.url };
  });

  app.post("/webhook", { config: { rawBody: true } }, async (request, reply) => {
    if (!stripe) {
      throw new AppError("BAD_REQUEST", 400, "Stripe not configured");
    }
    const signature = request.headers["stripe-signature"] as string | undefined;
    if (!signature) {
      request.log.warn("Missing Stripe signature");
      throw new AppError("BAD_REQUEST", 400, "Missing Stripe signature");
    }

    const rawBody = (request as any).rawBody as string | undefined;
    if (!rawBody) {
      request.log.warn("Missing raw body for Stripe webhook");
      throw new AppError("BAD_REQUEST", 400, "Missing raw body");
    }
    if (!env.STRIPE_WEBHOOK_SECRET) {
      throw new AppError("BAD_REQUEST", 400, "Stripe webhook secret not configured");
    }

    const tolerance = Number(env.STRIPE_WEBHOOK_TOLERANCE || 300);
    let event: Stripe.Event;
    try {
      event = stripe.webhooks.constructEvent(rawBody, signature, env.STRIPE_WEBHOOK_SECRET, tolerance);
    } catch (err) {
      request.log.warn({ err }, "Stripe signature verification failed");
      throw new AppError("BAD_REQUEST", 400, "Invalid signature");
    }

    const existing = await prisma.stripeEvent.findUnique({ where: { stripeId: event.id } });
    if (existing) {
      request.log.info({ stripeId: event.id }, "Duplicate Stripe event ignored");
      return reply.status(200).send({ received: true, duplicate: true });
    }

    try {
      await prisma.stripeEvent.create({
        data: {
          stripeId: event.id,
          type: event.type,
        },
      });

      await stripeQueue.add("stripe-event", { event }, { jobId: event.id });
    } catch (err) {
      request.log.error({ err, stripeId: event.id }, "Failed to enqueue Stripe event");
      await prisma.stripeEvent.deleteMany({ where: { stripeId: event.id } });
      throw new AppError("INTERNAL", 500, "Webhook processing failed");
    }

    const eventOrgId = await resolveWebhookOrgId(event);
    const canAuditOrg = eventOrgId
      ? await prisma.organization.findUnique({ where: { id: eventOrgId }, select: { id: true } })
      : null;
    if (canAuditOrg) {
      await writeAudit(
        {
          orgId: canAuditOrg.id,
          apiKeyId: "system",
          requestId: request.id,
          ip: request.ip,
          userAgent: readUserAgent(request.headers["user-agent"]),
        },
        "billing.webhook.processed",
        "stripeEvent",
        event.id,
        { type: event.type }
      );
    }

    reply.status(200).send({ received: true });
  });
}

async function resolveWebhookOrgId(event: Stripe.Event) {
  if (event.type === "checkout.session.completed") {
    const session = event.data.object as Stripe.Checkout.Session;
    return session.metadata?.["orgId"] || null;
  }

  if (event.type === "customer.subscription.updated" || event.type === "customer.subscription.deleted") {
    const subscription = event.data.object as Stripe.Subscription;
    const existing = await prisma.subscription.findUnique({
      where: { stripeSubscriptionId: subscription.id },
      select: { orgId: true },
    });
    return existing?.orgId || null;
  }

  return null;
}

function readUserAgent(value: string | string[] | undefined) {
  if (Array.isArray(value)) {
    return value[0];
  }
  return value;
}
