# Multi-tenant SaaS Monorepo

## What this is
A multi-tenant SaaS foundation: org accounts, RBAC, API keys, usage tracking, Stripe subscriptions + webhooks, and background worker jobs. Tenants share a single Postgres database with strict `orgId` scoping on every tenant-owned query.

## Architecture Overview
Repository layout:
```text
apps/
  web       Next.js App Router UI
  api       Fastify API + auth + billing/webhooks
  worker    BullMQ workers for rollups + Stripe events
packages/
  db        Prisma schema + migrations
  shared    Zod schemas + shared types
```

Request flow:
- Web → API → Postgres
- Stripe webhooks → API (raw body verification + idempotency) → Redis queue → Worker → Postgres

Data model highlights:
- `Organization`, `Membership` (RBAC roles)
- `ApiKey` stores `prefix` + `secretHash` only (secret shown once)
- `StripeEvent` ensures webhook idempotency
- `UsageEvent` + `UsageDaily` rollups for metering and limits

Related docs:
- [Architecture](docs/architecture.md)
- [Product Flow](docs/product-flow.md)
- [Demo Guide](docs/demo.md)

## Features
- Org and RBAC (OWNER, ADMIN, MEMBER, BILLING, READONLY)
- API keys (prefix + hash, secret shown once)
- Usage tracking and per-plan limits
- Stripe billing (checkout, portal, webhooks)
- Audit logs for sensitive actions
- Worker processing via BullMQ + Redis
- Health endpoint (`GET /health`)
- Readiness endpoint (`GET /ready`) for DB + Redis

## Quickstart
Prereqs: Node 20+, pnpm, Docker.

```bash
pnpm install
pnpm docker:up
pnpm db:generate
pnpm db:migrate
pnpm db:seed
pnpm dev
```

Ports (defaults):
- Web: `http://localhost:3000`
- API: `http://localhost:4000`

Change ports via `apps/web` (Next.js) and `API_PORT` in `.env`.

Seed user:
- Email: `owner@example.com`
- Password: `password123` (local development seed only; change/remove for non-local environments)

## Configuration
Copy `.env.example` to `.env` and fill values.

### API (`apps/api`)
- `DATABASE_URL`: Postgres connection string
- `REDIS_URL`: Redis connection string
- `SESSION_SECRET`: >= 32 chars; used to sign session cookies
- `SESSION_TTL_DAYS`: Absolute session lifetime in days
- `SESSION_IDLE_TIMEOUT_HOURS`: Idle timeout window in hours
- `SESSION_SLIDING_ENABLED`: Refresh idle expiry on activity (`true`/`false`)
- `SESSION_MAX_PER_USER`: Maximum active sessions per user (oldest sessions evicted)
- `API_BASE_URL`: Public API base URL
- `WEB_BASE_URL`: Public web base URL
- `AUTH_PROVIDER`: `local` (default). `oidc`/`saml` reserved for enterprise SSO integration.
- `API_PORT`: Port for Fastify
- `COOKIE_DOMAIN`: Optional cookie domain
- `COOKIE_SECURE`: `true` for HTTPS
- `INTERNAL_ADMIN_TOKEN`: Bearer token for internal org summary endpoint
- `SCIM_BEARER_TOKEN`: Bearer token for SCIM provisioning endpoints (`/scim/v2/*`)
- `LOG_LEVEL`: `info`, `debug`, etc
- `SENTRY_ENABLED` + `SENTRY_DSN`: Optional Sentry
- `DEMO_MODE`: `true` to enable demo-safe limits and disable destructive actions
- `STRIPE_SECRET_KEY`: Stripe secret key (test or live)
- `STRIPE_WEBHOOK_SECRET`: Stripe webhook signing secret
- `STRIPE_PRICE_ID`: Stripe price for subscription checkout
- `STRIPE_SUCCESS_URL`: Redirect after successful checkout
- `STRIPE_CANCEL_URL`: Redirect after cancel
- `STRIPE_PORTAL_RETURN_URL`: Return URL for billing portal
- `STRIPE_WEBHOOK_TOLERANCE`: Seconds for signature timestamp tolerance
- `API_KEY_RATE_LIMIT`: Requests per minute per API key
- `API_KEY_RATE_LIMIT_WINDOW_SEC`: API key rate limit window in seconds
- `AUTH_RATE_LIMIT_WINDOW_SEC`: Auth rate limit window in seconds
- `AUTH_LOGIN_IP_RATE_LIMIT`: Max login attempts per IP per window
- `AUTH_LOGIN_IDENTIFIER_RATE_LIMIT`: Max login attempts per email/user identifier per window
- `AUTH_REGISTER_IP_RATE_LIMIT`: Max registration attempts per IP per window
- `AUDIT_LOG_RETENTION_DAYS`: Retain audit logs for this many days (default `365`)

Session/cookie security notes:
- Session cookie is `HttpOnly`, `SameSite=Lax`, `Path=/`, and `Secure` in production.
- `SameSite=Lax` is used to reduce CSRF risk while still allowing common same-site navigation/login flows.
- Rotate `SESSION_SECRET` by replacing it with a new random 32+ char value and redeploying all API instances. This invalidates all existing sessions.

### Worker (`apps/worker`)
- `DATABASE_URL`
- `REDIS_URL`
- `WORKER_CONCURRENCY`: Parallel job processing
- `STRIPE_SECRET_KEY`: Optional (not required for current handlers)

### Web (`apps/web`)
- `NEXT_PUBLIC_API_BASE_URL`: API base URL used by the browser
- `NEXT_PUBLIC_DEMO_MODE`: `true` to show the demo banner and disable destructive UI actions
- `INTERNAL_ADMIN_TOKEN`: Server-side token used by `/app/support` proxy to call internal admin summary endpoint; required for Support page summary features (otherwise `/app/api/support/org-summary` returns `503 NOT_CONFIGURED`)

## Core Workflows
### Register and login
```bash
curl -c cookie.txt -X POST http://localhost:4000/auth/register \
  -H "Content-Type: application/json" \
  --data '{"email":"demo@example.com","password":"StrongPass1!","orgName":"Demo Org"}'
```

```bash
curl -b cookie.txt -c cookie.txt -X POST http://localhost:4000/auth/login \
  -H "Content-Type: application/json" \
  --data '{"email":"demo@example.com","password":"StrongPass1!"}'
```

### Create org / tenant resolution
Tenant selection is done via `X-Org-Id` on API calls. The web app stores this in local storage and includes it automatically.
You can update it in the Settings page (`/app/settings`).

How to obtain `ORG_ID`:
- Use the Settings page (`/app/settings`) where the org ID is shown and can be updated.

### Create an API key and call an endpoint
```bash
curl -b cookie.txt -c cookie.txt -X POST http://localhost:4000/api-keys \
  -H "Content-Type: application/json" \
  -H "X-Org-Id: <ORG_ID>" \
  --data '{"name":"demo"}'
```

```bash
curl -H "Authorization: Bearer <API_KEY_SECRET>" \
  -H "X-Org-Id: <ORG_ID>" \
  http://localhost:4000/data
```

### Usage tracking
Each request to `/data` increments `usage_events` and `usage_daily`.

```bash
curl -H "Authorization: Bearer <API_KEY_SECRET>" \
  -H "X-Org-Id: <ORG_ID>" \
  http://localhost:4000/data
```

```bash
curl -b cookie.txt -c cookie.txt \
  -H "Content-Type: application/json" \
  -H "X-Org-Id: <ORG_ID>" \
  http://localhost:4000/usage
```

### Stripe test flow
1. Set `STRIPE_SECRET_KEY`, `STRIPE_PRICE_ID`, and `STRIPE_WEBHOOK_SECRET`.
2. Use Stripe CLI to forward events:

```bash
stripe listen --forward-to http://localhost:4000/billing/webhook
```

3. Trigger a checkout in `/app/billing`.

Webhook idempotency:
- `stripe_events` stores event IDs and ignores duplicates safely.

## Testing & CI
```bash
pnpm -r typecheck
pnpm -r test
```

Integration tests cover:
- Tenant isolation
- API key auth (invalid/revoked/cross-org)
- Stripe webhook idempotency

## Production / Staging Notes
- Use `docker-compose.prod.yml` for production builds.
- Run migrations before API start:

```bash
pnpm db:migrate
```

- Run API and worker as separate processes/containers.
- Graceful shutdown is enabled; API logs include `requestId`.

## Security Notes
- Tenant isolation is enforced by `orgId` scoping and membership checks.
- API keys are stored as prefix + hashed secret only.
- Stripe webhook signature verification uses the raw body + timestamp tolerance.
- Webhooks are idempotent via `stripe_events`.
- Internal support summary endpoint: `GET /admin/orgs/:orgId/summary` with `Authorization: Bearer $INTERNAL_ADMIN_TOKEN`.
- Audit logs can be exported from `GET /audit?format=csv|json&limit=...` (OWNER/ADMIN only).
- Audit logs support filters: `action`, `actorUserId`, `targetType`, `from`, `to`.
- SCIM v2 user provisioning endpoints:
  - `GET /scim/v2/Users`
  - `POST /scim/v2/Users`
  - `PATCH /scim/v2/Users/:id`
  - `DELETE /scim/v2/Users/:id`
  Requires `Authorization: Bearer $SCIM_BEARER_TOKEN` and `X-Org-Id`.

## Demo Flow
1. Register at `/register` to create a new org and owner user.
2. Visit `/app/api-keys` to create an API key (copy the secret once).
3. Call `/data` with the API key and `X-Org-Id` header.
4. Open `/app/usage` to see usage metrics.
5. Go to `/app/billing` to start checkout or open the billing portal.
