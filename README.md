# Multi-tenant SaaS Monorepo

## Stack
- `apps/web`: Next.js App Router + React + TypeScript
- `apps/api`: Fastify + TypeScript + Zod
- `apps/worker`: BullMQ + Redis
- `packages/db`: Prisma + Postgres
- `packages/shared`: Shared Zod schemas + types

## Setup
1. Copy `.env.example` to `.env` and fill required values.
2. Start Postgres + Redis:

```bash
pnpm docker:up
```

3. Install deps:

```bash
pnpm install
```

4. Generate Prisma client:

```bash
pnpm db:generate
```

5. Run migrations + seed:

```bash
pnpm db:migrate
pnpm db:seed
```

6. Start services:

```bash
pnpm dev
```

## Scripts
- `pnpm db:migrate`: Prisma migrate deploy
- `pnpm db:seed`: Seed script
- `pnpm db:generate`: Prisma client generation
- `pnpm test`: Unit + integration tests
- `pnpm typecheck`: TypeScript typecheck

## Testing
Integration tests use the configured `DATABASE_URL`. Make sure Postgres is running.

## Notes
- Stripe webhooks require raw body verification and idempotency; configure `STRIPE_WEBHOOK_SECRET`.
- API uses `X-Org-Id` for tenant resolution and validates membership.
- Seed user: `owner@example.com` / `password123`.

## Demo flow
1. Register at `/register` to create a new org and owner user.
2. Store the returned org ID in the UI prompt (Org switcher or Settings page).
3. Visit `/app/api-keys` to create an API key (copy the secret once).
4. Open `/app/usage` to see usage metrics populate after API calls.
5. Go to `/app/billing` to start checkout or open the billing portal.
