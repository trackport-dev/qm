import { createKeyedQueue } from "../util/async.ts";
import { withPgTransaction, type PgPool, type PoolClient } from "../persistence/pg-pool.ts";
import type { CoordinationEvent, CoordinationKind, CoordinationRows } from "./types.ts";

export interface CoordinationRead {
  get<K extends CoordinationKind>(kind: K, id: string): Promise<CoordinationRows[K] | null>;
  list<K extends CoordinationKind>(kind: K, filter?: Partial<CoordinationRows[K]>): Promise<CoordinationRows[K][]>;
  events(after: number, limit: number): Promise<CoordinationEvent[]>;
}

export interface CoordinationTransaction extends CoordinationRead {
  put<K extends CoordinationKind>(kind: K, row: CoordinationRows[K]): Promise<void>;
  event(kind: CoordinationKind, id: string, at: number): Promise<number>;
}

export interface CoordinationRepository extends CoordinationRead {
  transaction<T>(locks: readonly string[], action: (tx: CoordinationTransaction) => Promise<T>): Promise<T>;
}

function eventWindow(after: number, limit: number): void {
  if (!Number.isSafeInteger(after) || after < 0 || !Number.isInteger(limit) || limit < 1 || limit > 200)
    throw new Error("invalid coordination event window");
}

type MemoryRows = { [K in CoordinationKind]: Map<string, CoordinationRows[K]> };

export function createMemoryCoordinationRepository(): CoordinationRepository {
  let rows: MemoryRows = {
    peer: new Map(),
    message: new Map(),
    delivery: new Map(),
    spawn: new Map(),
    wait: new Map(),
  };
  let events: CoordinationEvent[] = [];
  const queue = createKeyedQueue();
  const read = (data: MemoryRows, log: CoordinationEvent[]): CoordinationRead => ({
    async get(kind, id) {
      return structuredClone(data[kind].get(id) ?? null);
    },
    async list(kind, filter = {}) {
      return structuredClone(
        [...data[kind].values()]
          .filter((row) =>
            Object.entries(filter).every(
              ([key, value]) => JSON.stringify(row[key as keyof typeof row]) === JSON.stringify(value),
            ),
          )
          .sort((a, b) => a.id.localeCompare(b.id)),
      );
    },
    async events(after, limit) {
      eventWindow(after, limit);
      return structuredClone(log.filter((event) => event.sequence > after).slice(0, limit));
    },
  });
  return {
    get: (kind, id) => read(rows, events).get(kind, id),
    list: (kind, filter) => read(rows, events).list(kind, filter),
    events: (after, limit) => read(rows, events).events(after, limit),
    transaction: (_locks, action) =>
      queue("transaction", async () => {
        const next = structuredClone(rows);
        const log = structuredClone(events);
        const tx: CoordinationTransaction = {
          ...read(next, log),
          async put<K extends CoordinationKind>(kind: K, row: CoordinationRows[K]) {
            const table: Map<string, CoordinationRows[K]> = next[kind];
            table.set(row.id, structuredClone(row));
          },
          async event(kind, id, at) {
            const sequence = (log.at(-1)?.sequence ?? 0) + 1;
            log.push({ sequence, kind, id, at });
            return sequence;
          },
        };
        const result = await action(tx);
        rows = next;
        events = log;
        return structuredClone(result);
      }),
  };
}

const migration = {
  id: "coordination/0001",
  statements: [
    `CREATE TABLE IF NOT EXISTS coordination_records (
      org_id TEXT NOT NULL, kind TEXT NOT NULL, id TEXT NOT NULL, json JSONB NOT NULL,
      PRIMARY KEY (org_id, kind, id),
      CHECK (kind IN ('peer', 'message', 'delivery', 'spawn', 'wait')),
      CHECK (json->>'id' = id))`,
    `CREATE INDEX IF NOT EXISTS coordination_records_filter ON coordination_records USING GIN (json jsonb_path_ops)`,
    `CREATE TABLE IF NOT EXISTS coordination_cursors (org_id TEXT PRIMARY KEY, seq BIGINT NOT NULL)`,
    `CREATE TABLE IF NOT EXISTS coordination_events (
      org_id TEXT NOT NULL, seq BIGINT NOT NULL, kind TEXT NOT NULL, id TEXT NOT NULL, at BIGINT NOT NULL,
      PRIMARY KEY (org_id, seq))`,
  ],
};

export function createPostgresCoordinationRepository(pg: PgPool, orgId: string): CoordinationRepository {
  if (!orgId) throw new Error("coordination requires an organization");
  pg.registerMigration(migration);
  let initializing: Promise<void> | undefined;
  const ready = () =>
    (initializing ??= pg.migrate(migration).catch((error) => {
      initializing = undefined;
      throw error;
    }));
  const read = (client?: PoolClient): CoordinationRead => {
    const query = async (sql: string, values: unknown[]) => {
      if (client) return (await client.query(sql, values)).rows;
      await ready();
      return pg.q(sql, values);
    };
    return {
      async get(kind, id) {
        const result = await query("SELECT json FROM coordination_records WHERE org_id=$1 AND kind=$2 AND id=$3", [
          orgId,
          kind,
          id,
        ]);
        return result[0]?.json ?? null;
      },
      async list(kind, filter = {}) {
        const result = await query(
          "SELECT json FROM coordination_records WHERE org_id=$1 AND kind=$2 AND json @> $3::jsonb ORDER BY id",
          [orgId, kind, JSON.stringify(filter)],
        );
        return result.map((row) => row.json);
      },
      async events(after, limit) {
        eventWindow(after, limit);
        const result = await query(
          "SELECT seq, kind, id, at FROM coordination_events WHERE org_id=$1 AND seq>$2 ORDER BY seq LIMIT $3",
          [orgId, after, limit],
        );
        return result.map((row) => ({ sequence: Number(row.seq), kind: row.kind, id: row.id, at: Number(row.at) }));
      },
    };
  };
  return {
    ...read(),
    async transaction(locks, action) {
      await ready();
      return withPgTransaction(await pg.pool(), async (client) => {
        await client.query("SET LOCAL lock_timeout = '10s'");
        await client.query("SET LOCAL statement_timeout = '15s'");
        for (const key of [...new Set(locks)].sort()) {
          await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [
            JSON.stringify(["coordination", orgId, key]),
          ]);
        }
        return action({
          ...read(client),
          async put(kind, row) {
            await client.query(
              "INSERT INTO coordination_records (org_id, kind, id, json) VALUES ($1,$2,$3,$4) ON CONFLICT (org_id,kind,id) DO UPDATE SET json=EXCLUDED.json",
              [orgId, kind, row.id, JSON.stringify(row)],
            );
          },
          async event(kind, id, at) {
            const result = await client.query(
              "INSERT INTO coordination_cursors (org_id,seq) VALUES ($1,1) ON CONFLICT (org_id) DO UPDATE SET seq=coordination_cursors.seq+1 RETURNING seq",
              [orgId],
            );
            const sequence = Number(result.rows[0].seq);
            if (!Number.isSafeInteger(sequence)) throw new Error("coordination event sequence exhausted");
            await client.query("INSERT INTO coordination_events (org_id,seq,kind,id,at) VALUES ($1,$2,$3,$4,$5)", [
              orgId,
              sequence,
              kind,
              id,
              at,
            ]);
            return sequence;
          },
        });
      });
    },
  };
}
