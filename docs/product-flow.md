# Product Flow: Multi-tenant SaaS Console

This document defines the end-to-end user product flow for the internal SaaS console and translates it into implementation-ready acceptance criteria.

## 1) High-Level User Journey Map

| Stage | User Intent | Product Outcome |
|---|---|---|
| Awareness | Understand what the product solves | User sees value proposition: org-scoped API access, usage control, billing |
| Sign Up | Create account and organization | User + org + owner membership created |
| Activation | Reach first meaningful success | User creates API key and completes first authenticated `/data` call |
| Core Usage | Operate day-to-day | User manages members, keys, usage, and org context |
| Billing | Manage plan and spend | User starts checkout or opens billing portal |
| Retention | Return and maintain operations | User monitors usage, rotates keys, manages access and billing |

## 2) Detailed User Flows

## A) Sign Up and Onboarding

- Entry point: `/register`
- Exit criteria: authenticated session + org created + redirected to `/app`

Flow:

```text
[Landing/Login] -> [/register]
  -> [Enter orgName/email/password]
  -> [Submit]
    ->(valid) [POST /auth/register]
      -> [Store orgId]
      -> [/app Dashboard]
    ->(invalid) [Inline validation errors]
    ->(conflict/api fail) [Error toast]
```

- Screens: `/register`, `/app`
- Success state: account created toast, dashboard rendered
- Failure states:
  - invalid email/password/org name
  - email already registered
  - network/API failure
- Post-condition: owner user is inside tenant-scoped app with `orgId` stored

## B) Login and Dashboard Entry

- Entry point: `/login`
- Exit criteria: session valid + org context set + dashboard data loaded

Flow:

```text
[/login]
  -> [Enter email/password]
  -> [Submit]
    ->(valid) [POST /auth/login]
      -> [Store orgId]
      -> [/app]
      -> [GET /org]
    ->(invalid creds) [Login failed]
    ->(network fail) [Error toast]
```

- Screens: `/login`, `/app`
- Success state: org/plan/role cards visible
- Failure states:
  - invalid credentials
  - 401 (expired/missing session) => redirect `/login`
  - missing org context => dashboard fallback message
- Post-condition: user can navigate protected `/app/*` pages

## C) Creating and Switching Organizations

- Entry point: first org at register; switch via top bar or `/app/settings`
- Exit criteria: API requests use intended `X-Org-Id`

Flow:

```text
[/app or /app/settings]
  -> [Open org switcher]
  -> [Edit orgId]
  -> [Save]
  -> [localStorage orgId updated]
  -> [Next API call includes X-Org-Id]
```

- Screens: `/register`, top nav org switcher, `/app/settings`
- Success state: `/org` and related data resolve for selected org
- Failure states:
  - wrong orgId / no membership => 403
  - missing orgId => BAD_REQUEST-like behavior on protected routes
- Post-condition: tenant context switched safely for future calls

## D) Inviting and Managing Members

- Entry point: `/app/members`
- Exit criteria: membership list reflects intended changes (or read-only for non-admin)

Flow:

```text
[/app/members]
  -> [GET /org + GET /org/members]
  -> (OWNER/ADMIN?)
      -> yes: [Invite] [Change role] [Remove member]
      -> no: [Read-only table]
```

- Screens: `/app/members`
- Success state: updated table after invite/role/remove
- Failure states:
  - 403 for forbidden mutations
  - invalid invite payload
  - API/network failures
- Post-condition: org access model remains accurate

## E) Creating API Keys and Using Them

- Entry point: `/app/api-keys`
- Exit criteria: active key exists and first API-key-authenticated call succeeds

Flow:

```text
[/app/api-keys]
  -> [GET /api-keys]
  -> [Open create modal]
  -> [Submit key name]
  -> [POST /api-keys]
  -> [Show secret once + copy]
  -> [Use key on /data with X-Org-Id]
  -> [Optional revoke /api-keys/:id]
```

- Screens: `/app/api-keys`, external API client/curl
- Success state: key listed, secret copied once, `/data` returns success
- Failure states:
  - invalid key name
  - invalid/revoked key => unauthorized
  - wrong org header => auth failure
- Post-condition: secure programmatic access established

## F) Viewing Usage and Understanding Limits

- Entry point: `/app/usage`
- Exit criteria: user understands used vs limit and risk status

Flow:

```text
[/app/usage]
  -> [GET /org]
  -> [GET /usage?days=30]
  -> [Render used/limit summary + table]
  -> [Show normal/near-limit/over-limit message]
```

- Screens: `/app/usage`
- Success state: current period usage and historical rows visible
- Failure states:
  - role-limited access (403) => permission state
  - empty dataset => empty state
  - API/network failure => error state
- Post-condition: user can decide whether to optimize usage or upgrade

## G) Billing (Checkout + Portal)

- Entry point: `/app/billing`
- Exit criteria: user reaches checkout/portal or sees clear config-blocking state

Flow:

```text
[/app/billing]
  -> [GET /billing/status]
  -> (billingConfigured?)
      -> no: [Show "Billing not configured"]
      -> yes: [Upgrade -> POST /billing/checkout -> Stripe]
              [Manage -> POST /billing/portal -> Stripe]
```

- Screens: `/app/billing`, `/billing/success`, `/billing/cancel`, Stripe-hosted pages
- Success state: Stripe session opens and user can complete flow
- Failure states:
  - Stripe env missing => disabled + message
  - no subscription for portal => not found/error
- Post-condition: plan/subscription managed through Stripe

## 3) UX Checkpoints

- Aha moment:
  - Primary: create API key + first successful `/data` call
  - Secondary: usage increases on `/app/usage`
- Error handling checkpoints:
  - 401: redirect to `/login`
  - 403: clear permission message and disabled actions
  - Missing org context: prompt via settings/org switcher
  - Stripe not configured: explicit blocked state in billing
- Onboarding guidance opportunities:
  - first-run checklist on dashboard
  - contextual key-copy warning and quick API-call snippet
  - usage explanation tooltip near limit messaging

## 4) Progressive Disclosure and In-app Guidance

- Dashboard checklist (dismissible):
  - Set org ID
  - Create API key
  - Make first `/data` call
  - Verify usage row appears
  - Invite teammate
- Contextual guidance:
  - tooltips on role restrictions in members page
  - inline explanations for billing-disabled state
  - show advanced controls only when needed
- Keep default screens summary-first; move advanced details behind expandable sections.

## 5) Flow Metrics

- Activation metric:
  - User creates API key AND completes first successful `/data` request within first session
- Usage metric:
  - Usage table increments after API traffic (daily rollup/event appears)
- Access control metric:
  - Forbidden actions blocked for non-authorized roles (members/billing/usage)

## 6) Acceptance Criteria by Page

## `/register`
- Required UI:
  - org name, email, password inputs
  - submit button
  - inline validation messages
- Required API:
  - `POST /auth/register`
- Required states:
  - loading submit state
  - validation error
  - API error toast
  - success redirect to `/app`

## `/login`
- Required UI:
  - email + password fields
  - submit button
- Required API:
  - `POST /auth/login`
- Required states:
  - loading submit state
  - invalid credentials error
  - success sets `orgId` and redirects `/app`

## `/app`
- Required UI:
  - org card, plan card, role card
  - app shell with nav and org summary
- Required API:
  - `GET /org`
- Required states:
  - loading skeletons
  - missing org/data fallback
  - 401 redirect, 403 permission error

## `/app/members`
- Required UI:
  - members table: email, role, status, created
  - invite form
  - role update + remove actions
- Required API:
  - `GET /org`
  - `GET /org/members`
  - `POST /org/members`
  - `PATCH /org/members/:memberId`
  - `DELETE /org/members/:memberId`
- Required states:
  - loading skeleton
  - empty state
  - read-only mode for non OWNER/ADMIN
  - mutation success/error toasts

## `/app/api-keys`
- Required UI:
  - keys table (name, prefix, createdAt, lastUsedAt, status)
  - create modal
  - one-time secret display + copy action
  - revoke action with confirm
- Required API:
  - `GET /api-keys`
  - `POST /api-keys`
  - `DELETE /api-keys/:id`
- Required states:
  - loading skeleton
  - empty state
  - create/revoke success and error states

## `/app/usage`
- Required UI:
  - used vs limit summary
  - usage table for recent days/events
  - limit messaging (normal/near/over)
- Required API:
  - `GET /org`
  - `GET /usage?days=30`
- Required states:
  - loading skeleton
  - empty state
  - permission denied state for 403
  - error toast

## `/app/billing`
- Required UI:
  - plan/subscription status summary
  - buttons: Upgrade, Manage billing
  - billing-not-configured message when blocked
- Required API:
  - `GET /billing/status`
  - `POST /billing/checkout`
  - `POST /billing/portal`
- Required states:
  - loading state
  - success redirect to Stripe URL
  - clear failure messaging

## `/app/settings`
- Required UI:
  - profile email block
  - org details block
  - copy org ID
  - update org ID input + save
- Required API:
  - `GET /org`
  - `GET /auth/me`
- Required states:
  - loading skeleton
  - save-success feedback
  - data load error state

## 7) Engineering Ticket Breakdown

1. **Auth Entry Hardening**
- Scope: register/login success/failure states and org context persistence
- Files: `apps/web/app/(auth)/*`, `apps/web/lib/api.ts`
- Tests: form validation + login/register smoke

2. **App Shell & Tenant Context Reliability**
- Scope: sidebar/nav behavior, mobile drawer, org summary fallback
- Files: `apps/web/components/app-shell.tsx`, `apps/web/components/org-switcher.tsx`
- Tests: shell render + mobile toggle behavior

3. **Members Management Flow**
- Scope: read/list/invite/update/remove with RBAC-aware UI states
- Files: `apps/web/app/(app)/members/page.tsx`
- Tests: read-only vs admin action tests + error states

4. **API Keys Lifecycle Flow**
- Scope: list/create/revoke, one-time secret UX, confirmation dialogs
- Files: `apps/web/app/(app)/api-keys/page.tsx`
- Tests: create/revoke path + secret shown-once expectations

5. **Usage Visibility Flow**
- Scope: summary + table + limit warnings + permission handling
- Files: `apps/web/app/(app)/usage/page.tsx`
- Tests: normal, empty, and 403 states

6. **Billing Flow Integration**
- Scope: billing status, checkout/portal CTA behavior, config-blocked state
- Files: `apps/web/app/(app)/billing/page.tsx`
- Tests: configured/unconfigured branches and API failure paths

7. **Settings & Org Context Management**
- Scope: profile/org rendering, orgId update/copy, context persistence
- Files: `apps/web/app/(app)/settings/page.tsx`
- Tests: save orgId and subsequent request context behavior

8. **Cross-flow Quality Gate**
- Scope: smoke journey across login -> all `/app/*` pages
- Files: `apps/web/test/ui.smoke.test.tsx`
- Tests: route render assertions + key heading checks
