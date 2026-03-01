# Architecture

## Overview
This repository is a multi-tenant SaaS foundation built as a monorepo with three apps and two shared packages. Tenants share a single Postgres database and all tenant-owned data is scoped by `orgId`.

```text
apps/
  web       Next.js App Router UI
  api       Fastify API + auth + billing/webhooks
  worker    BullMQ workers for rollups + billing events
packages/
  db        Prisma schema + migrations
  shared    Zod schemas + shared types
```

## Request Flows
Web → API → Postgres
- Browser UI uses a single API client (`apps/web/lib/api.ts`) that attaches `X-Org-Id`.
- API validates session cookies and membership for `X-Org-Id`.
- Data is scoped by `orgId` in repository/service calls.

Billing webhooks → API → Worker → Postgres
- Webhook handler verifies billing signatures using the raw body.
- Events are stored in `billing_events` for idempotency.
- The worker consumes jobs from Redis to process subscription updates and usage rollups.

## Data Model Highlights
- `Organization` and `Membership` with RBAC roles
- `ApiKey` stores `prefix` + `secretHash` only; secret is shown once at creation
- `UsageEvent` and `UsageDaily` store raw events and daily rollups
- `BillingEvent` enforces webhook idempotency
