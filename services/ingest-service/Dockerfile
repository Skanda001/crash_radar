FROM node:20-alpine

WORKDIR /app

# Copy protos (accessible because build context = project root)
COPY protos /protos

# Copy and install deps
COPY services/ingest-service/package.json ./
RUN npm.cmd install --production=false 2>/dev/null || npm install --production=false

# Copy source and build
COPY services/ingest-service/tsconfig.json ./
COPY services/ingest-service/src ./src

RUN npm run build

CMD ["node", "dist/index.js"]
