import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { setTimeout } from "node:timers/promises";
import { test } from "node:test";
import { createPeerExecution } from "../src/coordination/execution.ts";
import { createPostgresPeerExecutionStore } from "../src/coordination/execution-store.ts";
import { createPeerIdentity } from "../src/coordination/identity.ts";
import { createPostgresCoordinationRepository } from "../src/coordination/repository.ts";
import { createPgPool } from "../src/persistence/pg-pool.ts";
import { createPostgresRunStore } from "../src/runs/postgres-run-store.ts";
import { createPostgresSessionStore } from "../src/sessions/postgres-session-store.ts";

const database = process.env.COORDINATION_TEST_DATABASE_URL;

for (const window of ["executing", "released"] as const) {
  test(`Postgres execution sweep after worker SIGKILL: ${window}`, { skip: !database, timeout: 15_000 }, async (t) => {
    const org = randomUUID();
    const cleanup: Array<() => Promise<unknown>> = [];
    t.after(async () => {
      for (const close of cleanup.reverse()) await close();
    });
    const admin = createPgPool(database!);
    cleanup.push(() => admin.close());
    const schema = `coord_crash_${randomUUID().replaceAll("-", "")}`;
    await admin.query(`CREATE SCHEMA ${schema}`);
    cleanup.push(() => admin.query(`DROP SCHEMA ${schema} CASCADE`));
    const url = new URL(database!);
    url.searchParams.set("options", `${url.searchParams.get("options") ?? ""} -c search_path=${schema}`.trim());
    const isolatedDatabase = url.toString();
    const pool = createPgPool(isolatedDatabase);
    const runtime = createPostgresRunStore(isolatedDatabase);
    cleanup.push(
      () => pool.close(),
      () => runtime.close(),
    );
    const repository = createPostgresCoordinationRepository(pool, org);
    const sessions = createPostgresSessionStore(isolatedDatabase);
    const session = await sessions.getOrCreateByThread(`crash:${org}`, "dm", `personal:${org}`);
    await createPeerIdentity(repository).ensure({ id: session.id, scopeId: session.scopeId });
    const { run } = await runtime.runs.enqueue({
      sessionId: session.threadRef,
      request: {
        actor: { id: org, type: "internal" },
        conversation: { kind: "dm", threadRef: session.threadRef, audience: [] },
        text: "Crash fixture",
        origin: {
          kind: "peer",
          messageId: randomUUID(),
          deliveryId: randomUUID(),
          senderSessionId: randomUUID(),
          senderName: "Crash fixture",
          recipientSessionId: session.id,
        },
      },
    });
    const child = fork(new URL("./support/coordination-crash-worker.ts", import.meta.url), [org, run.id, window], {
      execArgv: [],
      stdio: ["ignore", "ignore", "pipe", "ipc"],
      env: { ...process.env, COORDINATION_TEST_DATABASE_URL: isolatedDatabase },
    });
    let errors = "";
    child.stderr!.on("data", (chunk: Buffer) => {
      errors = (errors + chunk.toString()).slice(-8_192);
    });
    const exited = once(child, "exit");
    cleanup.push(async () => {
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      await exited;
    });
    const ready = await Promise.race([
      once(child, "message").then(([message]) => message),
      exited.then(() => {
        throw new Error(`Crash fixture exited before readiness: ${errors}`);
      }),
    ]);
    assert.equal(ready, "ready");
    const store = createPostgresPeerExecutionStore(pool, org);
    const active = await store.active();
    assert.equal(active.length, 1);
    assert.equal(active[0]!.runId, run.id);
    const token = active[0]!.token;
    assert.equal(await store.wasTokenReleased(run.id, token), window === "released");
    assert.equal(child.kill("SIGKILL"), true);
    const [code, signal] = await exited;
    assert.equal(code, null);
    assert.equal(signal, "SIGKILL");
    if (window === "executing") {
      const captured = (await runtime.runs.get(run.id))!;
      await setTimeout(Math.max(0, captured.leaseExpiresAt! - Date.now()) + 150);
      await runtime.runs.reapExpired();
    }
    const restoredPool = createPgPool(isolatedDatabase);
    cleanup.push(() => restoredPool.close());
    const restored = createPostgresPeerExecutionStore(restoredPool, org);
    const recovery = createPeerExecution({
      repository: createPostgresCoordinationRepository(restoredPool, org),
      sessions,
      runs: runtime.runs,
      store: restored,
    });
    await recovery.sweep();
    await recovery.sweep();
    const usage = await restored.usage(session.id, session.scopeId);
    assert.equal(usage.activeInScope, window === "executing" ? 1 : 0);
    assert.equal(usage.orphanedInScope, window === "executing" ? 1 : 0);
    assert.equal(await recovery.sourceReleased(run.id), window === "released");
    if (window === "executing") {
      assert.equal((await runtime.runs.get(run.id))?.status, "pending");
      assert.equal(await restored.admit({ ...active[0]!, token: randomUUID() }), "attempt_busy");
    } else {
      assert.equal((await runtime.runs.get(run.id))?.status, "done");
      assert.deepEqual(await restored.active(), []);
    }
  });
}
