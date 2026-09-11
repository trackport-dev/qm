import assert from "node:assert/strict";
import { createPeerExecution } from "../../src/coordination/execution.ts";
import { createPostgresPeerExecutionStore } from "../../src/coordination/execution-store.ts";
import { createPostgresCoordinationRepository } from "../../src/coordination/repository.ts";
import { createPgPool } from "../../src/persistence/pg-pool.ts";
import { createPostgresRunStore } from "../../src/runs/postgres-run-store.ts";
import { processRun } from "../../src/runs/worker.ts";
import { createPostgresSessionStore } from "../../src/sessions/postgres-session-store.ts";

const [org, runId, window] = process.argv.slice(2);
assert.ok(org && runId);
assert.ok(window === "executing" || window === "released");
const database = process.env.COORDINATION_TEST_DATABASE_URL;
assert.ok(database);
const pool = createPgPool(database);
const { runs } = createPostgresRunStore(database);
const store = createPostgresPeerExecutionStore(pool, org);
const parked = (): Promise<never> => {
  process.send?.("ready");
  return new Promise(() => {});
};
store.release = parked;
const execution = createPeerExecution({
  repository: createPostgresCoordinationRepository(pool, org),
  sessions: createPostgresSessionStore(database),
  runs,
  store,
});
const claimed = await runs.claimById(runId, "crash-worker", 1_000);
assert.ok(claimed);
await processRun(
  {
    runs,
    leaseTtlMs: 1_000,
    heartbeatIntervalMs: 100,
    execution,
    orchestrator: {
      async handleTurn() {
        if (window === "executing") return parked();
        return { status: "ok" };
      },
    },
  },
  claimed,
);
