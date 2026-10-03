# CrashRadar 🚨

> A distributed, real-time crash aggregation and error tracking engine.  
> Built with **TypeScript · gRPC · Apache Kafka · GraphQL · Docker · PostgreSQL**

---

## What Is This?

When a production app crashes, it might emit **100,000 identical error logs in 5 seconds**.  
Writing all of them directly to a database would crash the database too.

**CrashRadar solves this:**
- Apps report crashes via **high-speed gRPC** (binary protocol, not slow HTTP+JSON)
- Crashes are buffered by **Apache Kafka** — no spike can overwhelm the database
- A worker groups identical errors into a single **incident** with a count
- Engineers query grouped incidents and live alerts via **GraphQL**
- **Zero messages are lost** even if Kafka goes down (Transactional Outbox Pattern)

---

## Architecture

```
[ Your Production App ]
        │
        │  gRPC (high-speed binary)
        ▼
┌─────────────────────────────┐
│    Ingest Service           │  ← Receives crash reports
│    TypeScript + gRPC        │    Saves to PostgreSQL Outbox
└─────────────┬───────────────┘    (atomic transaction)
              │
              │  Outbox → Kafka
              ▼
┌─────────────────────────────┐
│      Apache Kafka           │  ← Absorbs traffic spikes
│      Topic: crashes.raw     │    Buffers 100k events safely
└─────────────┬───────────────┘
              │
              │  Consumer Group
              ▼
┌─────────────────────────────┐
│    Aggregator Worker        │  ← Groups identical crashes
│    TypeScript + KafkaJS     │    (100k errors → 1 incident)
└─────────────┬───────────────┘
              │
              │  PostgreSQL
              ▼
┌─────────────────────────────┐
│    API Gateway              │  ← GraphQL queries & subscriptions
│    TypeScript + GraphQL     │    Live alerts via WebSocket
└─────────────────────────────┘
```

---

## Why Each Technology Was Chosen

| Technology | Why |
|---|---|
| **gRPC** | 5–10x faster than REST for high-frequency crash ingestion. Strict typed contracts via Protocol Buffers prevent missing fields. |
| **Apache Kafka** | Acts as a shock absorber. During a crash spike (100k errors/sec), Kafka holds them all safely so the database is never overwhelmed. |
| **Transactional Outbox (PostgreSQL)** | If Kafka is down, crash reports are NOT lost. They wait in the `outbox` table and are sent to Kafka when it recovers. |
| **GraphQL** | Clients can query nested incident data, filter by project, and subscribe to real-time crash alerts — all from a single endpoint. |
| **TypeScript** | End-to-end type safety. Proto files compile to TypeScript types via `ts-proto`. No runtime surprises. |
| **Docker Compose** | One command spins up the entire distributed system: Postgres, Kafka, and all 3 microservices. |

---

## Project Structure

```
crash-radar/
├── docker-compose.yml          ← Runs everything
├── protos/
│   └── ingest.proto            ← gRPC contract definition
├── scripts/
│   ├── init.sql                ← Auto-creates DB tables on startup
│   └── simulate-crash.js       ← Test script (fires fake crashes via gRPC)
└── services/
    ├── ingest-service/         ← gRPC server: receives crash reports
    ├── aggregator-worker/      ← Kafka consumer: groups duplicate errors
    └── api-gateway/            ← GraphQL API: queries + subscriptions
```

---

## Quick Start (One Command)

**Prerequisites:** Docker Desktop installed and running.

```bash
# Clone the repo
git clone <your-repo-url>
cd crash-radar

# Start everything
docker compose up --build
```

Wait for all services to report "ready". Then open:
- **GraphQL Playground:** http://localhost:4000/graphql
- **gRPC Ingest:** `localhost:50051`

---

## Testing It (Step by Step)

### Step 1 — Send Simulated Crashes

Install gRPC dependencies locally and run the simulator:

```bash
cd scripts
npm.cmd install @grpc/grpc-js @grpc/proto-loader
node simulate-crash.js
```

This fires **5 crash reports** (including 3 identical TypeErrors) to the Ingest Service via gRPC.

### Step 2 — Query the Incidents via GraphQL

Open **http://localhost:4000/graphql** and run:

```graphql
query {
  getIncidents(projectId: "00000000-0000-0000-0000-000000000001") {
    errorType
    message
    count
    firstSeenAt
    lastSeenAt
  }
}
```

**Expected result:** The 3 identical TypeErrors are grouped into **1 incident with count: 3**.

### Step 3 — Subscribe to Live Alerts

In GraphQL Playground, run this subscription in a second tab:

```graphql
subscription {
  onNewIncident(projectId: "00000000-0000-0000-0000-000000000001") {
    errorType
    message
    count
  }
}
```

Then run the simulator again — you will see new incidents appear **in real time**.

---

## The Key Interview Answer

> **"What happens if Kafka goes down for 10 minutes?"**

The Ingest Service writes every crash into **two places in one atomic database transaction**:
1. The `outbox` table in PostgreSQL
2. The `incidents` table metadata

When Kafka comes back online, the background **Outbox Publisher** reads the unprocessed rows from the `outbox` table and streams them to Kafka automatically.  
**Zero crash reports are lost.**

---

## Real-World Market Equivalent

This is the open-source backend engine powering tools like:
- [Sentry.io](https://sentry.io) — \$26–\$80+/month
- [Bugsnag](https://bugsnag.com) — \$50+/month  
- [Datadog Error Tracking](https://datadoghq.com) — \$15/host/month

Self-hosted companies (banks, hospitals, government) that cannot send crash logs to external SaaS providers need exactly this.
