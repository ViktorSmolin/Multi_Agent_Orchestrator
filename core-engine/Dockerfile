FROM node:20-alpine AS deps

WORKDIR /app

COPY core-engine/package.json core-engine/package-lock.json ./

RUN npm ci --omit=dev --no-audit --no-fund

FROM node:20-alpine

ENV NODE_ENV=production

WORKDIR /app

COPY --from=deps /app/node_modules ./node_modules
COPY core-engine/package.json ./
COPY core-engine/src ./src
COPY contracts ./contracts
COPY db/migrations ./migrations

USER node

EXPOSE 8090

CMD ["node", "src/server.js"]
