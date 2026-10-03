import { startGrpcServer } from "./server";

console.log("========================================");
console.log("  CrashRadar — Ingest Service");
console.log("========================================");

// ================================================================
//  Retry startup with backoff
//  Postgres and Kafka need a few seconds to be fully ready
//  even after Docker marks them "healthy". This retries up to
//  10 times with a 5-second gap before giving up.
// ================================================================
async function startWithRetry(retries = 10): Promise<void> {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      await startGrpcServer();
      return; // success — exit the retry loop
    } catch (err) {
      console.error(`[Startup] Attempt ${attempt}/${retries} failed:`, err);
      if (attempt === retries) {
        console.error("[Startup] All retries exhausted. Exiting.");
        process.exit(1);
      }
      console.log(`[Startup] Retrying in 5 seconds...`);
      await new Promise((r) => setTimeout(r, 5000));
    }
  }
}

startWithRetry();
