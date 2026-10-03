FROM node:20-alpine

WORKDIR /app

COPY services/aggregator-worker/package.json ./
RUN npm install --production=false

COPY services/aggregator-worker/tsconfig.json ./
COPY services/aggregator-worker/src ./src

RUN npm run build

CMD ["node", "dist/index.js"]
