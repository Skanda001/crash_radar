/**
 * CrashRadar — Crash Simulator Script
 *
 * This script simulates a production app sending crash reports
 * to the Ingest Service via gRPC.
 *
 * Run it with:
 *   node simulate-crash.js
 *
 * Watch the terminal running docker compose to see:
 *   1. Ingest Service receives the crash and saves to Outbox
 *   2. Outbox Publisher sends to Kafka
 *   3. Aggregator Worker groups duplicate errors
 *   4. GraphQL query shows the incident with count
 */

const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
const path = require("path");

const PROTO_PATH = path.join(__dirname, "../protos/ingest.proto");

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const proto = grpc.loadPackageDefinition(packageDefinition);

// Connect to the Ingest Service gRPC server
const client = new proto.ingest.IngestService(
  "localhost:50051",
  grpc.credentials.createInsecure()
);

// ================================================================
//  Simulated crash reports (mix of unique and duplicate errors)
// ================================================================
const CRASH_SCENARIOS = [
  {
    api_key: "demo-api-key-123",
    error_type: "TypeError",
    message: "Cannot read properties of undefined (reading 'id')",
    stack_trace: `TypeError: Cannot read properties of undefined (reading 'id')
    at getUserCart (checkout.ts:42:15)
    at processPayment (payment.ts:88:22)
    at async handleCheckout (handler.ts:33:5)`,
    service: "checkout-service",
  },
  {
    api_key: "demo-api-key-123",
    error_type: "DatabaseError",
    message: "Connection pool exhausted: timeout after 30000ms",
    stack_trace: `DatabaseError: Connection pool exhausted
    at Pool.acquire (pool.ts:156:11)
    at UserRepository.findById (user.repo.ts:29:18)
    at AuthService.validateSession (auth.ts:77:9)`,
    service: "auth-service",
  },
  {
    api_key: "demo-api-key-123",
    error_type: "TypeError",
    message: "Cannot read properties of undefined (reading 'id')",
    stack_trace: `TypeError: Cannot read properties of undefined (reading 'id')
    at getUserCart (checkout.ts:42:15)
    at processPayment (payment.ts:88:22)
    at async handleCheckout (handler.ts:33:5)`,
    service: "checkout-service",
  },
  {
    api_key: "demo-api-key-123",
    error_type: "UnhandledPromiseRejection",
    message: "Kafka broker unreachable: ECONNREFUSED 127.0.0.1:9092",
    stack_trace: `UnhandledPromiseRejection: Kafka broker unreachable
    at KafkaProducer.connect (kafka.ts:55:19)
    at OrderService.publishEvent (order.service.ts:102:11)`,
    service: "order-service",
  },
  {
    api_key: "demo-api-key-123",
    error_type: "TypeError",
    message: "Cannot read properties of undefined (reading 'id')",
    stack_trace: `TypeError: Cannot read properties of undefined (reading 'id')
    at getUserCart (checkout.ts:42:15)
    at processPayment (payment.ts:88:22)
    at async handleCheckout (handler.ts:33:5)`,
    service: "checkout-service",
  },
];

// ================================================================
//  Send each crash scenario to gRPC one by one
// ================================================================
function sendCrash(scenario, index) {
  client.ReportCrash(scenario, (error, response) => {
    if (error) {
      console.error(`[${index + 1}] ❌ gRPC Error:`, error.message);
      return;
    }
    console.log(
      `[${index + 1}] ✅ Crash reported: "${scenario.error_type}" → Event ID: ${response.event_id}`
    );
  });
}

console.log("🚨 Sending simulated crash reports to CrashRadar...\n");
CRASH_SCENARIOS.forEach((scenario, i) => {
  setTimeout(() => sendCrash(scenario, i), i * 500);
});

setTimeout(() => {
  console.log(`
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ Simulation complete!

Now open GraphQL Playground at http://localhost:4000/graphql
and run this query to see the grouped incidents:

  query {
    getIncidents(projectId: "00000000-0000-0000-0000-000000000001") {
      errorType
      message
      count
      firstSeenAt
      lastSeenAt
    }
  }

You should see the TypeError with count: 3
(3 identical crashes were grouped into 1 incident)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  `);
  process.exit(0);
}, CRASH_SCENARIOS.length * 500 + 2000);
