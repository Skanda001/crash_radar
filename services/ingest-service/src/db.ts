import { Pool } from "pg";
import { v4 as uuidv4 } from "uuid";
import * as crypto from "crypto";

// ================================================================
//  Database connection pool
//  Pool = keeps multiple connections open for speed
// ================================================================
const pool = new Pool({
  connectionString:
    process.env.DATABASE_URL ||
    "postgres://crashradar:crashradar@localhost:5432/crashradar",
});

// ================================================================
//  saveCrashReport
//
//  THE MOST IMPORTANT FUNCTION IN THE INGEST SERVICE.
//
//  It does TWO things inside ONE database transaction:
//    1. Save the raw crash report to the outbox table
//    2. Commit everything at once
//
//  If the database crashes mid-way, BOTH writes are rolled back.
//  No orphan events, no partial data, no lost messages.
//
//  This is the "Transactional Outbox Pattern".
// ================================================================
export async function saveCrashReport(data: {
  projectId: string;
  errorType: string;
  message: string;
  stackTrace: string;
  service: string;
}): Promise<{ eventId: string }> {
  // Create a fingerprint: a unique hash of the stack trace.
  // Same bug = same fingerprint, so we can group duplicates.
  const fingerprint = crypto
    .createHash("sha256")
    .update(data.stackTrace.trim())
    .digest("hex")
    .substring(0, 64);

  const eventId = uuidv4();

  // Get a dedicated connection from the pool for this transaction
  const client = await pool.connect();

  try {
    // START the transaction — everything below is atomic
    await client.query("BEGIN");

    // Write the crash event to the Outbox table.
    // Our background publisher will pick this up and send to Kafka.
    await client.query(
      `INSERT INTO outbox (event_type, payload, processed)
       VALUES ($1, $2, FALSE)`,
      [
        "CRASH_REPORTED",
        JSON.stringify({
          eventId,
          projectId: data.projectId,
          fingerprint,
          errorType: data.errorType,
          message: data.message,
          stackTrace: data.stackTrace,
          service: data.service,
          reportedAt: new Date().toISOString(),
        }),
      ]
    );

    // COMMIT — both writes succeed together, or neither does
    await client.query("COMMIT");

    console.log(
      `[DB] Crash saved to outbox. Event ID: ${eventId} | Fingerprint: ${fingerprint.substring(0, 12)}...`
    );

    return { eventId };
  } catch (error) {
    // If ANYTHING goes wrong, roll back both writes
    await client.query("ROLLBACK");
    console.error("[DB] Transaction rolled back:", error);
    throw error;
  } finally {
    // Always release the connection back to the pool
    client.release();
  }
}

// ================================================================
//  validateApiKey
//  Looks up the project by its API key.
//  Returns the project ID if valid, null if not found.
// ================================================================
export async function validateApiKey(
  apiKey: string
): Promise<string | null> {
  const result = await pool.query(
    "SELECT id FROM projects WHERE api_key = $1",
    [apiKey]
  );

  if (result.rows.length === 0) {
    return null;
  }

  return result.rows[0].id as string;
}

// ================================================================
//  startOutboxPublisher
//
//  This runs as a background loop every 2 seconds.
//  It reads unprocessed rows from the outbox table,
//  publishes them to Kafka, and marks them as processed.
//
//  Why poll and not use CDC?
//  For simplicity. Polling every 2 seconds is perfectly fine
//  for a project at this scale and easy to understand.
// ================================================================
export function startOutboxPublisher(
  publishToKafka: (payload: Record<string, unknown>) => Promise<void>
): void {
  const POLL_INTERVAL_MS = 2000;

  const poll = async () => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      // Pick up to 50 unprocessed events at a time
      // FOR UPDATE SKIP LOCKED: if two publisher pods are running,
      // they won't process the same row twice
      const result = await client.query(
        `SELECT id, payload FROM outbox
         WHERE processed = FALSE
         ORDER BY created_at ASC
         LIMIT 50
         FOR UPDATE SKIP LOCKED`
      );

      if (result.rows.length > 0) {
        console.log(
          `[Outbox] Found ${result.rows.length} unprocessed event(s). Publishing to Kafka...`
        );

        for (const row of result.rows) {
          // Push to Kafka
          await publishToKafka(row.payload as Record<string, unknown>);

          // Mark as processed
          await client.query(
            "UPDATE outbox SET processed = TRUE WHERE id = $1",
            [row.id]
          );
        }
      }

      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      console.error("[Outbox] Publisher error:", err);
    } finally {
      client.release();
    }
  };

  // Run immediately then every 2 seconds
  poll();
  setInterval(poll, POLL_INTERVAL_MS);
  console.log(
    `[Outbox] Publisher started. Polling every ${POLL_INTERVAL_MS}ms...`
  );
}
