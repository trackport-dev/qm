import { createKeyedQueue } from "../util/async.ts";
import { withPgTransaction, type PgPool, type PoolClient } from "../persistence/pg-pool.ts";
import { CoordinationError } from "./types.ts";
import { assertRunFence, withFencedPgTransaction, type CoordinationRunFence } from "./run-fence.ts";
import type { RunStore } from "../runs/run-store.ts";

interface PeerExecution {
  runId: string;
  rootId: string;
  scopeId: string;
  token: string;
  active: boolean;
  autonomous: boolean;
  orphaned: boolean;
}
type ExecutionInput = Omit<PeerExecution, "active" | "orphaned">;
type ExecutionAdmission = "admitted" | "concurrency_limit" | "turn_limit" | "attempt_busy";
export interface ExecutionUsage {
  activeInScope: number;
  orphanedInScope: number;
  autonomousTurns: number;
  concurrencyLimit: number;
  turnLimit: number;
}
export interface PeerExecutionStore {
  lowerLimits(
    rootId: string,
    scopeId: string,
    concurrencyLimit: number,
    turnLimit: number,
    fence?: CoordinationRunFence,
  ): Promise<void>;
  recordStart(runId: string, attempt: number): Promise<void>;
  recordRelease(runId: string, attempt: number, token: string | null): Promise<void>;
  wasTokenReleased(runId: string, token: string): Promise<boolean>;
  wasReleased(runId: string, attempt: number): Promise<boolean>;
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

function validateLowerLimits(concurrency: number, turns: number, usage: ExecutionUsage): void {
  if (!Number.isSafeInteger(concurrency) || concurrency < 1 || !Number.isSafeInteger(turns) || turns < 1)
    throw new CoordinationError(400, "invalid_execution_limits", "execution limits must be positive integers");
  if (concurrency > usage.concurrencyLimit || turns > usage.turnLimit)
    throw new CoordinationError(409, "execution_limit_increase", "agents may only lower execution limits");
  if (concurrency < usage.activeInScope || turns < usage.autonomousTurns)
    throw new CoordinationError(
      409,
      "execution_limit_below_usage",
      "execution limits cannot be lower than current usage",
    );
}

export function createMemoryPeerExecutionStore(
  concurrency = 4,
  turnLimit = 200,
  runs?: Pick<RunStore, "get">,
): PeerExecutionStore {
  validateLimits(concurrency, turnLimit);
  const rows = new Map<string, PeerExecution>();
  const released = new Map<string, Map<number, boolean>>();
  const releasedTokens = new Set<string>();
  const queue = createKeyedQueue();
  const scopeLimits = new Map<string, number>();
  const rootLimits = new Map<string, number>();
  function usage(rootId: string, scopeId: string): ExecutionUsage {
    const values = [...rows.values()];
    return {
      activeInScope: values.filter((row) => row.active && row.scopeId === scopeId).length,
      orphanedInScope: values.filter((row) => row.active && row.orphaned && row.scopeId === scopeId).length,
      autonomousTurns: values.filter((row) => row.autonomous && row.rootId === rootId).length,
      concurrencyLimit: scopeLimits.get(scopeId) ?? concurrency,
      turnLimit: rootLimits.get(rootId) ?? turnLimit,
    };
  }
  return {
    lowerLimits: (rootId, scopeId, nextConcurrency, nextTurns, fence) =>
      queue("admission", async () => {
        const run = fence ? ((await runs?.get(fence.runId)) ?? null) : null;
        if (fence) assertRunFence(fence, run);
        validateLowerLimits(nextConcurrency, nextTurns, usage(rootId, scopeId));
        scopeLimits.set(scopeId, nextConcurrency);
        rootLimits.set(rootId, nextTurns);
      }),
    async recordStart(runId, attempt) {
      const attempts = released.get(runId) ?? new Map<number, boolean>();
      if (!attempts.has(attempt)) attempts.set(attempt, false);
      released.set(runId, attempts);
    },
    async recordRelease(runId, attempt, token) {
      const attempts = released.get(runId) ?? new Map<number, boolean>();
      attempts.set(attempt, true);
      released.set(runId, attempts);
      if (token) releasedTokens.add(JSON.stringify([runId, token]));
    },
    async wasTokenReleased(runId, token) {
      return releasedTokens.has(JSON.stringify([runId, token]));
    },
    async wasReleased(runId, attempt) {
      const attempts = released.get(runId);
      return attempts?.get(attempt) === true && [...attempts.values()].every(Boolean);
    },
    async usage(rootId, scopeId) {
      return usage(rootId, scopeId);
    },
    admit: (input) =>
      queue("admission", async () => {
        const existing = rows.get(input.runId);
        checkBinding(existing, input);
        if (existing?.active) return existing.token === input.token && !existing.orphaned ? "admitted" : "attempt_busy";
        const current = usage(input.rootId, input.scopeId);
        if (current.activeInScope >= current.concurrencyLimit) return "concurrency_limit";
        if (!existing && input.autonomous && current.autonomousTurns >= current.turnLimit) return "turn_limit";
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
  memoryRuns?: () => RunStore,
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
  const releaseMigration = {
    id: "coordination/0011-execution-release-receipts",
    statements: [
      `CREATE TABLE IF NOT EXISTS coordination_execution_releases (
        org_id TEXT NOT NULL, run_id TEXT NOT NULL, attempt INTEGER NOT NULL,
        PRIMARY KEY (org_id, run_id, attempt))`,
    ],
  };
  pg.registerMigration(releaseMigration);
  const startedMigration = {
    id: "coordination/0012-execution-start-receipts",
    statements: [
      "ALTER TABLE coordination_execution_releases ADD COLUMN IF NOT EXISTS released BOOLEAN NOT NULL DEFAULT true",
    ],
  };
  pg.registerMigration(startedMigration);
  const tokenMigration = {
    id: "coordination/0014-execution-termination-tokens",
    statements: [
      `CREATE TABLE IF NOT EXISTS coordination_execution_terminations (
        org_id TEXT NOT NULL, run_id TEXT NOT NULL, token TEXT NOT NULL,
        PRIMARY KEY (org_id, run_id, token))`,
    ],
  };
  pg.registerMigration(tokenMigration);
  const limitsMigration = {
    id: "coordination/0015-lowered-execution-limits",
    statements: [
      `CREATE TABLE IF NOT EXISTS coordination_execution_limits (
        org_id TEXT NOT NULL, kind TEXT NOT NULL CHECK (kind IN ('scope','root')), id TEXT NOT NULL,
        ceiling BIGINT NOT NULL CHECK (ceiling > 0), PRIMARY KEY (org_id,kind,id))`,
    ],
  };
  pg.registerMigration(limitsMigration);
  const ready = async () => {
    await pg.migrate(migration);
    await pg.migrate(budgetMigration);
    await pg.migrate(orphanMigration);
    await pg.migrate(releaseMigration);
    await pg.migrate(startedMigration);
    await pg.migrate(tokenMigration);
    await pg.migrate(limitsMigration);
  };
  async function currentUsage(client: PoolClient, rootId: string, scopeId: string): Promise<ExecutionUsage> {
    const { rows } = await client.query(
      `SELECT
        count(*) FILTER (WHERE active AND scope_id=$3)::int AS active,
        count(*) FILTER (WHERE active AND orphaned AND scope_id=$3)::int AS orphaned,
        count(*) FILTER (WHERE autonomous AND root_id=$2)::int AS turns
        FROM coordination_executions WHERE org_id=$1 AND (root_id=$2 OR (scope_id=$3 AND active))`,
      [orgId, rootId, scopeId],
    );
    const limits = await client.query(
      "SELECT kind,ceiling FROM coordination_execution_limits WHERE org_id=$1 AND ((kind='root' AND id=$2) OR (kind='scope' AND id=$3))",
      [orgId, rootId, scopeId],
    );
    return {
      activeInScope: rows[0].active,
      orphanedInScope: rows[0].orphaned,
      autonomousTurns: rows[0].turns,
      concurrencyLimit: Math.min(
        concurrency,
        Number(limits.rows.find((row) => row.kind === "scope")?.ceiling ?? concurrency),
      ),
      turnLimit: Math.min(turnLimit, Number(limits.rows.find((row) => row.kind === "root")?.ceiling ?? turnLimit)),
    };
  }
  async function lockAdmission(client: PoolClient, keys: string[]): Promise<void> {
    await client.query("SET LOCAL lock_timeout='10s'");
    await client.query("SET LOCAL statement_timeout='15s'");
    for (const key of keys.sort())
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [
        JSON.stringify(["peer-execution", orgId, key]),
      ]);
  }
  return {
    async lowerLimits(rootId, scopeId, nextConcurrency, nextTurns, fence) {
      await ready();
      await withFencedPgTransaction(
        pg,
        async (client) => {
          await lockAdmission(client, [`root:${rootId}`, `scope:${scopeId}`]);
          validateLowerLimits(nextConcurrency, nextTurns, await currentUsage(client, rootId, scopeId));
          for (const [kind, id, ceiling] of [
            ["scope", scopeId, nextConcurrency],
            ["root", rootId, nextTurns],
          ])
            await client.query(
              "INSERT INTO coordination_execution_limits(org_id,kind,id,ceiling) VALUES($1,$2,$3,$4) ON CONFLICT(org_id,kind,id) DO UPDATE SET ceiling=EXCLUDED.ceiling",
              [orgId, kind, id, ceiling],
            );
        },
        fence,
        memoryRuns,
      );
    },
    async recordStart(runId, attempt) {
      await ready();
      await pg.q(
        "INSERT INTO coordination_execution_releases(org_id, run_id, attempt, released) VALUES ($1,$2,$3,false) ON CONFLICT DO NOTHING",
        [orgId, runId, attempt],
      );
    },
    async recordRelease(runId, attempt, token) {
      await ready();
      await withPgTransaction(await pg.pool(), async (client) => {
        await client.query(
          "INSERT INTO coordination_execution_releases(org_id, run_id, attempt, released) VALUES ($1,$2,$3,true) ON CONFLICT (org_id, run_id, attempt) DO UPDATE SET released=true",
          [orgId, runId, attempt],
        );
        if (token)
          await client.query(
            "INSERT INTO coordination_execution_terminations(org_id, run_id, token) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
            [orgId, runId, token],
          );
      });
    },
    async wasTokenReleased(runId, token) {
      await ready();
      const rows = await pg.q(
        "SELECT 1 FROM coordination_execution_terminations WHERE org_id=$1 AND run_id=$2 AND token=$3",
        [orgId, runId, token],
      );
      return rows.length > 0;
    },
    async wasReleased(runId, attempt) {
      await ready();
      const rows = await pg.q(
        "SELECT 1 FROM coordination_execution_releases WHERE org_id=$1 AND run_id=$2 AND attempt=$3 AND released AND NOT EXISTS (SELECT 1 FROM coordination_execution_releases WHERE org_id=$1 AND run_id=$2 AND NOT released)",
        [orgId, runId, attempt],
      );
      return rows.length > 0;
    },
    async usage(rootId, scopeId) {
      await ready();
      return withPgTransaction(await pg.pool(), (client) => currentUsage(client, rootId, scopeId));
    },
    async admit(input) {
      await ready();
      return withPgTransaction(await pg.pool(), async (client) => {
        await lockAdmission(client, [`run:${input.runId}`, `root:${input.rootId}`, `scope:${input.scopeId}`]);
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
        const current = await currentUsage(client, input.rootId, input.scopeId);
        if (current.activeInScope >= current.concurrencyLimit) return "concurrency_limit";
        if (!existing && input.autonomous) {
          if (current.autonomousTurns >= current.turnLimit) return "turn_limit";
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
