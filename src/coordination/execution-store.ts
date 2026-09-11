import { createKeyedQueue } from "../util/async.ts";
import { withPgTransaction, type PgPool } from "../persistence/pg-pool.ts";

export interface PeerExecution {
  runId: string;
  rootId: string;
  scopeId: string;
  token: string;
  active: boolean;
  autonomous: boolean;
  orphaned: boolean;
}
type ExecutionInput = Omit<PeerExecution, "active" | "orphaned">;
export type ExecutionAdmission = "admitted" | "concurrency_limit" | "turn_limit" | "attempt_busy";
export interface ExecutionUsage {
  activeInScope: number;
  orphanedInScope: number;
  autonomousTurns: number;
  concurrencyLimit: number;
  turnLimit: number;
}
export interface PeerExecutionStore {
  usage(rootId: string, scopeId: string): Promise<ExecutionUsage>;
  admit(input: ExecutionInput): Promise<ExecutionAdmission>;
  orphan(runId: string, token: string): Promise<void>;
  release(runId: string, token: string): Promise<void>;
  active(): Promise<PeerExecution[]>;
}

function checkBinding(existing: ExecutionInput | undefined, input: ExecutionInput): void {
  if (
    existing &&
    (existing.scopeId !== input.scopeId || existing.rootId !== input.rootId || existing.autonomous !== input.autonomous)
  )
    throw new Error("run execution binding cannot change");
}

function validateLimits(concurrency: number, turnLimit: number): void {
  if (!Number.isSafeInteger(concurrency) || concurrency < 1 || !Number.isSafeInteger(turnLimit) || turnLimit < 1)
    throw new Error("execution limits must be positive integers");
}

export function createMemoryPeerExecutionStore(concurrency = 4, turnLimit = 200): PeerExecutionStore {
  validateLimits(concurrency, turnLimit);
  const rows = new Map<string, PeerExecution>();
  const queue = createKeyedQueue();
  return {
    async usage(rootId, scopeId) {
      const values = [...rows.values()];
      return {
        activeInScope: values.filter((row) => row.active && row.scopeId === scopeId).length,
        orphanedInScope: values.filter((row) => row.active && row.orphaned && row.scopeId === scopeId).length,
        autonomousTurns: values.filter((row) => row.autonomous && row.rootId === rootId).length,
        concurrencyLimit: concurrency,
        turnLimit,
      };
    },
    admit: (input) =>
      queue("admission", async () => {
        const existing = rows.get(input.runId);
        checkBinding(existing, input);
        if (existing?.active) return existing.token === input.token && !existing.orphaned ? "admitted" : "attempt_busy";
        const values = [...rows.values()];
        if (values.filter((row) => row.active && row.scopeId === input.scopeId).length >= concurrency)
          return "concurrency_limit";
        if (
          !existing &&
          input.autonomous &&
          values.filter((row) => row.autonomous && row.rootId === input.rootId).length >= turnLimit
        )
          return "turn_limit";
        rows.set(input.runId, { ...input, active: true, orphaned: false });
        return "admitted";
      }),
    orphan: (runId, token) =>
      queue("admission", async () => {
        const row = rows.get(runId);
        if (row?.active && row.token === token) rows.set(runId, { ...row, orphaned: true });
      }),
    release: (runId, token) =>
      queue("admission", async () => {
        const row = rows.get(runId);
        if (row?.token === token) rows.set(runId, { ...row, active: false });
      }),
    async active() {
      return structuredClone([...rows.values()].filter((row) => row.active));
    },
  };
}

const migration = {
  id: "coordination/0005-execution-admission",
  statements: [
    `CREATE TABLE IF NOT EXISTS coordination_executions (
      org_id TEXT NOT NULL, run_id TEXT NOT NULL, root_id TEXT NOT NULL, scope_id TEXT NOT NULL,
      token TEXT NOT NULL, active BOOLEAN NOT NULL, PRIMARY KEY (org_id, run_id))`,
    `CREATE INDEX IF NOT EXISTS coordination_execution_roots ON coordination_executions(org_id, root_id)`,
    `CREATE INDEX IF NOT EXISTS coordination_execution_active ON coordination_executions(org_id, scope_id) WHERE active`,
  ],
};

export function createPostgresPeerExecutionStore(
  pg: PgPool,
  orgId: string,
  concurrency = 4,
  turnLimit = 200,
): PeerExecutionStore {
  validateLimits(concurrency, turnLimit);
  if (!orgId) throw new Error("execution admission requires an organization");
  pg.registerMigration(migration);
  const budgetMigration = {
    id: "coordination/0006-autonomous-execution-budget",
    statements: [
      "ALTER TABLE coordination_executions ADD COLUMN IF NOT EXISTS autonomous BOOLEAN NOT NULL DEFAULT true",
    ],
  };
  pg.registerMigration(budgetMigration);
  const orphanMigration = {
    id: "coordination/0007-orphaned-executions",
    statements: [
      "ALTER TABLE coordination_executions ADD COLUMN IF NOT EXISTS orphaned BOOLEAN NOT NULL DEFAULT false",
    ],
  };
  pg.registerMigration(orphanMigration);
  const ready = async () => {
    await pg.migrate(migration);
    await pg.migrate(budgetMigration);
    await pg.migrate(orphanMigration);
  };
  return {
    async usage(rootId, scopeId) {
      await ready();
      const rows = await pg.q(
        `SELECT
        count(*) FILTER (WHERE active AND scope_id=$3)::int AS active,
        count(*) FILTER (WHERE active AND orphaned AND scope_id=$3)::int AS orphaned,
        count(*) FILTER (WHERE autonomous AND root_id=$2)::int AS turns
        FROM coordination_executions WHERE org_id=$1 AND (root_id=$2 OR (scope_id=$3 AND active))`,
        [orgId, rootId, scopeId],
      );
      return {
        activeInScope: rows[0]!.active as number,
        orphanedInScope: rows[0]!.orphaned as number,
        autonomousTurns: rows[0]!.turns as number,
        concurrencyLimit: concurrency,
        turnLimit,
      };
    },
    async admit(input) {
      await ready();
      return withPgTransaction(await pg.pool(), async (client) => {
        await client.query("SET LOCAL lock_timeout='10s'");
        await client.query("SET LOCAL statement_timeout='15s'");
        for (const key of [`run:${input.runId}`, `root:${input.rootId}`, `scope:${input.scopeId}`].sort())
          await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [
            JSON.stringify(["peer-execution", orgId, key]),
          ]);
        const { rows } = await client.query(
          "SELECT root_id, scope_id, token, active, autonomous, orphaned FROM coordination_executions WHERE org_id=$1 AND run_id=$2",
          [orgId, input.runId],
        );
        const existing = rows[0];
        checkBinding(
          existing
            ? {
                runId: input.runId,
                rootId: existing.root_id,
                scopeId: existing.scope_id,
                token: existing.token,
                autonomous: existing.autonomous,
              }
            : undefined,
          input,
        );
        if (existing?.active) return existing.token === input.token && !existing.orphaned ? "admitted" : "attempt_busy";
        const running = await client.query(
          "SELECT count(*)::int AS count FROM coordination_executions WHERE org_id=$1 AND scope_id=$2 AND active",
          [orgId, input.scopeId],
        );
        if (running.rows[0].count >= concurrency) return "concurrency_limit";
        if (!existing && input.autonomous) {
          const used = await client.query(
            "SELECT count(*)::int AS count FROM coordination_executions WHERE org_id=$1 AND root_id=$2 AND autonomous",
            [orgId, input.rootId],
          );
          if (used.rows[0].count >= turnLimit) return "turn_limit";
        }
        await client.query(
          `INSERT INTO coordination_executions(org_id,run_id,root_id,scope_id,token,active,autonomous)
          VALUES($1,$2,$3,$4,$5,true,$6) ON CONFLICT(org_id,run_id) DO UPDATE SET token=EXCLUDED.token,active=true,orphaned=false`,
          [orgId, input.runId, input.rootId, input.scopeId, input.token, input.autonomous],
        );
        return "admitted";
      });
    },
    async orphan(runId, token) {
      await ready();
      await pg.q(
        "UPDATE coordination_executions SET orphaned=true WHERE org_id=$1 AND run_id=$2 AND token=$3 AND active",
        [orgId, runId, token],
      );
    },
    async release(runId, token) {
      await ready();
      await pg.q("UPDATE coordination_executions SET active=false WHERE org_id=$1 AND run_id=$2 AND token=$3", [
        orgId,
        runId,
        token,
      ]);
    },
    async active() {
      await ready();
      const rows = await pg.q(
        "SELECT run_id,root_id,scope_id,token,autonomous,orphaned FROM coordination_executions WHERE org_id=$1 AND active",
        [orgId],
      );
      return rows.map((row) => ({
        runId: row.run_id as string,
        rootId: row.root_id as string,
        scopeId: row.scope_id as string,
        token: row.token as string,
        active: true,
        autonomous: row.autonomous as boolean,
        orphaned: row.orphaned as boolean,
      }));
    },
  };
}
