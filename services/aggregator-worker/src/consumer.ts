import { Kafka, Consumer, EachMessagePayload } from "kafkajs";
import { aggregateCrash } from "./aggregator";

const KAFKA_TOPIC = "crashes.raw";
const CONSUMER_GROUP = "crashradar-aggregator-group";

// ================================================================
//  startConsumer
//
//  Connects to Kafka as a Consumer and processes crash events.
//
//  Key concepts:
//  - Consumer Group: if you run 2 worker containers,
//    Kafka automatically splits the partitions between them
//    (horizontal scaling for free).
//  - fromBeginning: true means if the worker restarts,
//    it replays all unprocessed messages from where it left off.
// ================================================================
export async function startConsumer(): Promise<void> {
  const kafka = new Kafka({
    clientId: "crashradar-aggregator",
    brokers: [(process.env.KAFKA_BROKER as string) || "localhost:9092"],
  });

  const consumer: Consumer = kafka.consumer({
    groupId: CONSUMER_GROUP,
  });

  // Retry connection with backoff (Kafka might not be ready immediately)
  let retries = 0;
  while (retries < 10) {
    try {
      await consumer.connect();
      break;
    } catch {
      retries++;
      console.log(
        `[Kafka] Consumer connection attempt ${retries}/10. Retrying in 5s...`
      );
      await new Promise((r) => setTimeout(r, 5000));
    }
  }

  await consumer.subscribe({
    topic: KAFKA_TOPIC,
    fromBeginning: true,
  });

  console.log(
    `[Kafka] Aggregator Worker subscribed to topic: "${KAFKA_TOPIC}" (group: ${CONSUMER_GROUP})`
  );

  // This runs for EVERY message Kafka delivers
  await consumer.run({
    eachMessage: async ({ message }: EachMessagePayload) => {
      if (!message.value) return;

      try {
        // Parse the JSON payload from Kafka
        const payload = JSON.parse(message.value.toString()) as {
          projectId: string;
          fingerprint: string;
          errorType: string;
          message: string;
          stackTrace: string;
          service: string;
          reportedAt: string;
        };

        console.log(
          `[Kafka] Received crash event | Project: ${payload.projectId} | Type: ${payload.errorType}`
        );

        // Hand off to aggregator: upsert into incidents table
        await aggregateCrash(payload);
      } catch (error) {
        // Log but don't crash the worker — bad messages go to logs
        // In production, you'd forward these to a Dead Letter Queue
        console.error("[Kafka] Failed to process message:", error);
      }
    },
  });
}
