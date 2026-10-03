-- ================================================================
--  CrashRadar: Database Schema
--  Run automatically by Docker on first startup
-- ================================================================

-- 1. Projects Table
--    Each project has a unique API key used to identify
--    which app the crash came from.
CREATE TABLE IF NOT EXISTS projects (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        VARCHAR(100) NOT NULL,
    api_key     VARCHAR(64)  NOT NULL UNIQUE,
    created_at  TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- 2. Incidents Table
--    Grouped, deduplicated error reports.
--    Instead of 100,000 rows for the same bug,
--    we store 1 row and increment `count`.
CREATE TABLE IF NOT EXISTS incidents (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id   UUID         NOT NULL REFERENCES projects(id),
    fingerprint  VARCHAR(64)  NOT NULL,   -- SHA-256 hash of stack trace
    error_type   VARCHAR(200) NOT NULL,   -- e.g. "TypeError", "NullPointerException"
    message      TEXT         NOT NULL,   -- e.g. "Cannot read property 'id' of undefined"
    stack_trace  TEXT         NOT NULL,   -- Full stack trace string
    count        INT          NOT NULL DEFAULT 1,
    first_seen_at TIMESTAMP   NOT NULL DEFAULT NOW(),
    last_seen_at  TIMESTAMP   NOT NULL DEFAULT NOW(),
    UNIQUE(project_id, fingerprint)       -- One incident per unique error per project
);

-- 3. Outbox Table
--    The heart of the Transactional Outbox Pattern.
--    When the Ingest Service saves an error,
--    it writes here in the SAME database transaction.
--    A background poller reads this and pushes to Kafka.
--    This guarantees ZERO lost messages even if Kafka is down.
CREATE TABLE IF NOT EXISTS outbox (
    id          BIGSERIAL    PRIMARY KEY,
    event_type  VARCHAR(100) NOT NULL,   -- e.g. "CRASH_REPORTED"
    payload     JSONB        NOT NULL,   -- The full error payload
    processed   BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- Index for fast Outbox polling (only unprocessed rows)
CREATE INDEX IF NOT EXISTS idx_outbox_unprocessed
    ON outbox(processed, created_at)
    WHERE processed = FALSE;

-- Index for fast incident lookup by fingerprint
CREATE INDEX IF NOT EXISTS idx_incidents_fingerprint
    ON incidents(project_id, fingerprint);

-- ================================================================
--  Seed Data: One default project to test with immediately
-- ================================================================
INSERT INTO projects (id, name, api_key) VALUES
    ('00000000-0000-0000-0000-000000000001', 'Demo App', 'demo-api-key-123')
ON CONFLICT DO NOTHING;
