import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import * as path from "path";
import { saveCrashReport, validateApiKey, startOutboxPublisher } from "./db";
import { createKafkaProducer, publishCrash } from "./kafka";

// ================================================================
//  Load the .proto file at runtime
//  This tells gRPC what methods and message shapes to expect
// ================================================================
// In Docker: protos are at /protos/ingest.proto
// In local dev: protos are at ../../protos/ingest.proto
const PROTO_PATH = process.env.PROTO_PATH ||
  (require("fs").existsSync("/protos/ingest.proto")
    ? "/protos/ingest.proto"
    : path.join(__dirname, "../../../protos/ingest.proto"));

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const proto = grpc.loadPackageDefinition(packageDefinition) as any;

// ================================================================
//  ReportCrash Handler
//
//  This is the function that runs every time a client calls
//  the `ReportCrash` gRPC method.
//
//  Flow:
//    1. Validate the API key → find the project
//    2. Save to PostgreSQL + Outbox (atomic transaction)
//    3. Return immediate acknowledgement to the client
//       (Kafka processing happens async in the background)
// ================================================================
async function handleReportCrash(
  call: grpc.ServerUnaryCall<
    {
      api_key: string;
      error_type: string;
      message: string;
      stack_trace: string;
      service: string;
    },
    { success: boolean; message: string; event_id: string }
  >,
  callback: grpc.sendUnaryData<{
    success: boolean;
    message: string;
    event_id: string;
  }>
): Promise<void> {
  const { api_key, error_type, message, stack_trace, service } = call.request;

  try {
    // Step 1: Validate the API key
    const projectId = await validateApiKey(api_key);
    if (!projectId) {
      callback(null, {
        success: false,
        message: "Invalid API key",
        event_id: "",
      });
      return;
    }

    // Step 2: Save to DB with Transactional Outbox (atomic)
    const { eventId } = await saveCrashReport({
      projectId,
      errorType: error_type,
      message,
      stackTrace: stack_trace,
      service,
    });

    // Step 3: Respond immediately — don't wait for Kafka
    callback(null, {
      success: true,
      message: "Crash received. Processing in background.",
      event_id: eventId,
    });

    console.log(
      `[gRPC] Crash reported from service: "${service}" | Type: "${error_type}"`
    );
  } catch (error) {
    console.error("[gRPC] Error handling ReportCrash:", error);
    callback({
      code: grpc.status.INTERNAL,
      message: "Internal server error",
    });
  }
}

// ================================================================
//  startGrpcServer
//  Starts the gRPC server and begins listening for crash reports
// ================================================================
export async function startGrpcServer(): Promise<void> {
  // Create and connect Kafka producer
  const producer = await createKafkaProducer();

  // Start the Outbox publisher (polls DB every 2s → sends to Kafka)
  startOutboxPublisher(async (payload) => {
    await publishCrash(producer, payload);
  });

  // Create gRPC server
  const server = new grpc.Server();

  // Register our IngestService handler
  server.addService(proto.ingest.IngestService.service, {
    ReportCrash: handleReportCrash,
  });

  const port = process.env.GRPC_PORT || "50051";
  const address = `0.0.0.0:${port}`;

  server.bindAsync(
    address,
    grpc.ServerCredentials.createInsecure(), // No TLS for simplicity (add in production)
    (error, boundPort) => {
      if (error) {
        console.error("[gRPC] Failed to start server:", error);
        process.exit(1);
      }
      server.start();
      console.log(
        `[gRPC] Ingest Service running on port ${boundPort}`
      );
    }
  );
}
