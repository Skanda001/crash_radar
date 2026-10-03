import { createSchema, createYoga, createPubSub } from "graphql-yoga";
import { createServer } from "http";
import { getIncidents, getIncidentById, Incident } from "./db";

// ================================================================
//  PubSub — simple in-memory event bus for GraphQL Subscriptions.
//  When a new incident is detected, we publish here and it streams
//  instantly to all connected WebSocket clients.
// ================================================================
type IncidentPayload = ReturnType<typeof toGraphQL>;

const pubSub = createPubSub<{
  NEW_INCIDENT: [incident: IncidentPayload];
}>();

// ================================================================
//  GraphQL Schema (SDL — Schema Definition Language)
// ================================================================
const schema = createSchema({
  typeDefs: /* GraphQL */ `
    type Incident {
      id: String!
      projectId: String!
      errorType: String!
      message: String!
      stackTrace: String!
      count: Int!
      firstSeenAt: String!
      lastSeenAt: String!
    }

    type Query {
      """
      Get all incidents for a project, sorted by most frequent first.
      Try: getIncidents(projectId: "00000000-0000-0000-0000-000000000001")
      """
      getIncidents(projectId: String!): [Incident!]!

      """
      Get one incident's full details (including stack trace).
      """
      getIncident(id: String!): Incident
    }

    type Subscription {
      """
      Real-time stream: fires whenever a new crash type is first detected.
      Open this in GraphQL Playground while running simulate-crash.js
      to see incidents appear live.
      """
      onNewIncident(projectId: String!): Incident!
    }
  `,

  resolvers: {
    Query: {
      getIncidents: async (_: unknown, args: { projectId: string }) => {
        const rows = await getIncidents(args.projectId);
        return rows.map(toGraphQL);
      },

      getIncident: async (_: unknown, args: { id: string }) => {
        const row = await getIncidentById(args.id);
        return row ? toGraphQL(row) : null;
      },
    },

    Subscription: {
      onNewIncident: {
        subscribe: (_: unknown, args: { projectId: string }) =>
          pubSub.subscribe("NEW_INCIDENT"),

        resolve: (incident: IncidentPayload) => incident,
      },
    },
  },
});

// ================================================================
//  Helper: convert DB snake_case row to GraphQL camelCase shape
// ================================================================
function toGraphQL(i: Incident) {
  return {
    id: i.id,
    projectId: i.project_id,
    errorType: i.error_type,
    message: i.message,
    stackTrace: i.stack_trace,
    count: i.count,
    firstSeenAt: new Date(i.first_seen_at).toISOString(),
    lastSeenAt: new Date(i.last_seen_at).toISOString(),
  };
}

// ================================================================
//  Poll PostgreSQL every 5 seconds, broadcast new incidents to
//  all connected GraphQL subscription clients.
// ================================================================
const seenIds = new Set<string>();

async function pollAndBroadcast(): Promise<void> {
  const DEMO_PROJECT = "00000000-0000-0000-0000-000000000001";
  try {
    const rows = await getIncidents(DEMO_PROJECT);
    for (const row of rows) {
      if (!seenIds.has(row.id)) {
        seenIds.add(row.id);
        pubSub.publish("NEW_INCIDENT", toGraphQL(row));
        console.log(`[GraphQL] 📢 New incident broadcast: "${row.error_type}"`);
      }
    }
  } catch {
    // DB might not be ready yet on first poll — ignore silently
  }
}

// ================================================================
//  Create Yoga server and standard Node HTTP server
// ================================================================
export function startGateway(): void {
  const yoga = createYoga({ schema });
  const server = createServer(yoga);

  const port = parseInt(process.env.PORT || "4000");

  // Poll for new incidents every 5 seconds
  setInterval(pollAndBroadcast, 5000);
  pollAndBroadcast();

  server.listen(port, () => {
    console.log(`[GraphQL] Gateway running at http://localhost:${port}/graphql`);
  });
}
