import { FastifyInstance } from "fastify";
import Stripe from "stripe";
import { prisma } from "../lib/prisma";
import { AppError } from "../lib/errors";
import { requireRole, requireUser } from "../middleware/auth";
import { enforceUsageLimit } from "../middleware/usage";
import { stripeQueue } from "../lib/queue";

export async function billingRoutes(app: FastifyInstance) {
  const env = app.env;
  const stripeKey = env.STRIPE_SECRET_KEY;
  const stripe = stripeKey ? new Stripe(stripeKey, { apiVersion: "2023-10-16" }) : null;

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

    reply.status(200).send({ received: true });
  });
}
