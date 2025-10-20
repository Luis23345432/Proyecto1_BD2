# Frontend Dockerfile (Next.js)

FROM node:lts-bookworm-slim AS base

WORKDIR /app

# Install pnpm (preferred due to existing lockfile)
RUN corepack enable && corepack prepare pnpm@9.12.3 --activate

# Tools for healthchecks and general diagnostics
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

COPY package.json pnpm-lock.yaml ./
RUN pnpm fetch

COPY . .
RUN pnpm install --no-frozen-lockfile \
    && pnpm build

EXPOSE 3000
ENV PORT=3000 HOST=0.0.0.0 \
    NODE_ENV=production \
    NEXT_TELEMETRY_DISABLED=1

CMD ["pnpm", "start"]
