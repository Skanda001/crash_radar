import { Pool } from "pg";

const pool = new Pool({
  connectionString:
    process.env.DATABASE_URL ||
    "postgres://crashradar:crashradar@localhost:5432/crashradar",
});

// ================================================================
//  aggregateCrash
//
//  THE CORE INTELLIGENCE OF CRASHRADAR.
//
//  This function receives one crash event from Kafka and does:
//
//  Case A — First time we see this bug:
//    → INSERT a new incident row into PostgreSQL
//
//  Case B — We've seen this bug before (same fingerprint):
//    → UPDATE: increment count by 1, update last_seen_at
//
//  This turns 100,000 identical error rows into:
//    { error: "Cannot read property 'id'...", count: 100000 }
//
//  The SQL trick: INSERT ... ON CONFLICT DO UPDATE
//  This is an "upsert" — insert if not exists, update if exists.
//  It's atomic so no race conditions between concurrent workers.
// ================================================================
export async function aggregateCrash(payload: {
  projectId: string;
  fingerprint: string;
  errorType: string;
  message: string;
  stackTrace: string;
  service: string;
  reportedAt: string;
}): Promise<void> {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // Upsert: create incident OR increment existing one
    const result = await client.query(
      `INSERT INTO incidents
         (project_id, fingerprint, error_type, message, stack_trace, count, first_seen_at, last_seen_at)
       VALUES
         ($1, $2, $3, $4, $5, 1, NOW(), NOW())
       ON CONFLICT (project_id, fingerprint)
       DO UPDATE SET
         count        = incidents.count + 1,
         last_seen_at = NOW()
       RETURNING id, count`,
      [
        payload.projectId,
        payload.fingerprint,
        payload.errorType,
        payload.message,
        payload.stackTrace,
      ]
    );

    await client.query("COMMIT");

    const { id, count } = result.rows[0] as { id: string; count: number };

    if (count === 1) {
      console.log(
        `[Aggregator] 🆕 NEW incident created: "${payload.errorType}" | ID: ${id}`
      );
    } else {
      console.log(
        `[Aggregator] 🔁 Existing incident updated: "${payload.errorType}" | Count: ${count} | ID: ${id}`
      );
    }
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("[Aggregator] DB error during aggregation:", error);
    throw error;
  } finally {
    client.release();
  }
}
