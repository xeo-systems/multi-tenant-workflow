# Public Demo Deployment (Repo-Specific)

This guide uses this repo's real scripts and runtime behavior:
- Vercel: `apps/web`
- Render: `apps/api` (Web Service) and `apps/worker` (Background Worker)
- Supabase: Postgres
- Upstash: Redis
- Region: **US West / Oregon** (Render + Supabase + Upstash)

## 1) Supabase (Postgres)

1. Create a Supabase project in **US West (Oregon)**.
2. Open **Project Settings -> Database**.
3. Copy the **Direct connection** string (port `5432`) and use it as `DATABASE_URL`.
   - Use this placeholder: `postgresql://____`
   - Prefer direct `:5432` for Prisma migrations (not pooler).
4. Run Prisma migrations from repo root:

```bash
DATABASE_URL='postgresql://____' pnpm db:migrate
```

## 2) Upstash (Redis)

1. Create a Redis database in **US West (Oregon)**.
2. Copy the Redis URL and set `REDIS_URL`.
   - If Upstash gives TLS, use `rediss://...`.
   - Use this placeholder: `rediss://____`

## 3) Render API (apps/api Web Service)

Create a Render **Web Service** in Oregon.

- Root Directory: repo root
- Build Command (from `apps/api/package.json`):

```bash
pnpm --filter @saas/api build
```

- Start Command (from `apps/api/package.json`, with Render port binding):

```bash
API_PORT=$PORT pnpm --filter @saas/api start
```

- Health Check Path: `/ready`
  - `/ready` checks DB + Redis.
  - `/health` is liveness only.

- Service URL placeholder: `https://____`

### Render API env vars

| Variable | Required | Value for demo |
| --- | --- | --- |
| `DATABASE_URL` | Yes | `postgresql://____` |
| `REDIS_URL` | Yes | `rediss://____` |
| `SESSION_SECRET` | Yes | 32+ char random string |
| `API_BASE_URL` | Yes | `https://____` (Render API URL) |
| `WEB_BASE_URL` | Yes | `https://____` (Vercel URL) |
| `PORT` | Render-provided | Do not set manually |
| `API_PORT` | Derived | Set via start command: `API_PORT=$PORT` |

Recommended for production behavior:
- `NODE_ENV=production`
- `COOKIE_SECURE=true`

## 4) Render Worker (apps/worker Background Worker)

Create a Render **Background Worker** in Oregon.

- Root Directory: repo root
- Build Command (from `apps/worker/package.json`):

```bash
pnpm --filter @saas/worker build
```

- Start Command (from `apps/worker/package.json`):

```bash
pnpm --filter @saas/worker start
```

### Render Worker env vars

| Variable | Required | Value for demo |
| --- | --- | --- |
| `DATABASE_URL` | Yes | `postgresql://____` |
| `REDIS_URL` | Yes | `rediss://____` |

Recommended:
- `NODE_ENV=production`

## 5) Vercel Web (apps/web)

Create/import Vercel project for this repo.

- Root Directory: `apps/web`
- Framework: Next.js
- Build Command (from `apps/web/package.json`): `pnpm build`
- Start Command (from `apps/web/package.json`): `pnpm start`

- Vercel URL placeholder: `https://____`

### Vercel env vars

| Variable | Required | Value for demo |
| --- | --- | --- |
| `NEXT_PUBLIC_API_BASE_URL` | Yes | `https://____` (Render API URL) |

## 6) Wiring order

1. Deploy API and Worker on Render with `DATABASE_URL` + `REDIS_URL`.
2. Set `API_BASE_URL` to `https://____` (Render API URL).
3. Set `WEB_BASE_URL` to `https://____` (Vercel URL).
4. Deploy Web on Vercel with `NEXT_PUBLIC_API_BASE_URL=https://____`.
5. Re-deploy services after env var changes.

## Common issues

1. `NEXT_PUBLIC_` rules (Next.js):
   - Only `NEXT_PUBLIC_*` vars are exposed to browser code.
   - Set `NEXT_PUBLIC_API_BASE_URL` in Vercel project env, then redeploy.

2. `WEB_BASE_URL` mismatch (cookies/CORS):
   - `WEB_BASE_URL` must exactly match your Vercel origin (`https://...`).
   - Mismatch causes auth cookie and CORS failures.

3. Prisma migrations and Supabase URL:
   - Use direct Postgres URL on `:5432` for `pnpm db:migrate`.
   - Pooler/proxy URLs can break Prisma migration flow.

4. Render health checks:
   - Use `/ready` as Health Check Path so Render checks dependencies (DB/Redis).
   - `/health` only verifies process liveness.

## Placeholder recap

- Render API URL: `https://____`
- Vercel URL: `https://____`
- Supabase `DATABASE_URL` (direct 5432): `postgresql://____`
- Upstash `REDIS_URL`: `rediss://____`
