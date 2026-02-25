FROM node:20-alpine AS deps
WORKDIR /app
COPY package.json pnpm-workspace.yaml ./
COPY apps/worker/package.json apps/worker/package.json
COPY packages/db/package.json packages/db/package.json
RUN corepack enable && pnpm install --frozen-lockfile

FROM node:20-alpine AS build
WORKDIR /app
COPY . .
RUN corepack enable && pnpm db:generate
RUN pnpm --filter @saas/worker build

FROM node:20-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production
COPY --from=build /app/apps/worker/dist apps/worker/dist
COPY --from=build /app/apps/worker/package.json apps/worker/package.json
COPY --from=build /app/node_modules node_modules
CMD ["node", "apps/worker/dist/index.js"]
