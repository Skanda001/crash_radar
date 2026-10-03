import { Kafka, Producer, Partitioners } from "kafkajs";

// ================================================================
//  Kafka Setup
//  We use a single topic: `crashes.raw`
//  All raw crash events go here before aggregation
// ================================================================
const KAFKA_TOPIC = "crashes.raw";

const kafka = new Kafka({
  clientId: "crashradar-ingest",
  brokers: [(process.env.KAFKA_BROKER as string) || "localhost:9092"],
});

// ================================================================
//  createKafkaProducer
//  Connects to Kafka and returns a ready-to-use producer
// ================================================================
export async function createKafkaProducer(): Promise<Producer> {
  const producer = kafka.producer({
    createPartitioner: Partitioners.LegacyPartitioner,
  });

  await producer.connect();
  console.log(`[Kafka] Producer connected. Publishing to topic: ${KAFKA_TOPIC}`);
  return producer;
}

// ================================================================
//  publishCrash
//  Sends a single crash event to the Kafka topic.
//
//  Key = projectId: this ensures all crashes from the same project
//  land on the same Kafka partition (strict ordering per project).
// ================================================================
export async function publishCrash(
  producer: Producer,
  payload: Record<string, unknown>
): Promise<void> {
  await producer.send({
    topic: KAFKA_TOPIC,
    messages: [
      {
        key: payload["projectId"] as string,
        value: JSON.stringify(payload),
      },
    ],
  });

  console.log(
    `[Kafka] Published crash event for project: ${payload["projectId"]}`
  );
}
