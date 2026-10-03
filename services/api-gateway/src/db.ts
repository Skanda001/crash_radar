import { Pool } from "pg";

const pool = new Pool({
  connectionString:
    process.env.DATABASE_URL ||
    "postgres://crashradar:crashradar@localhost:5432/crashradar",
});

// ================================================================
//  Types matching our database schema
// ================================================================
export interface Incident {
  id: string;
  project_id: string;
  fingerprint: string;
  error_type: string;
  message: string;
  stack_trace: string;
  count: number;
  first_seen_at: Date;
  last_seen_at: Date;
}

// ================================================================
//  getIncidents
//  Fetches all incidents for a project, sorted by most frequent
// ================================================================
export async function getIncidents(projectId: string): Promise<Incident[]> {
  const result = await pool.query(
    `SELECT * FROM incidents
     WHERE project_id = $1
     ORDER BY count DESC, last_seen_at DESC`,
    [projectId]
  );
  return result.rows as Incident[];
}

// ================================================================
//  getIncidentById
//  Fetches a single incident with full stack trace details
// ================================================================
export async function getIncidentById(id: string): Promise<Incident | null> {
  const result = await pool.query(
    "SELECT * FROM incidents WHERE id = $1",
    [id]
  );
  if (result.rows.length === 0) return null;
  return result.rows[0] as Incident;
}
