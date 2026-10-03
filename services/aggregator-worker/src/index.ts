import { startConsumer } from "./consumer";

console.log("========================================");
console.log("  CrashRadar — Aggregator Worker");
console.log("========================================");

startConsumer().catch((err) => {
  console.error("Failed to start Aggregator Worker:", err);
  process.exit(1);
});
