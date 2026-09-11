import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { createPgPool } from "../src/persistence/pg-pool.ts";
import { createPeerExecution } from "../src/coordination/execution.ts";
import { createMemoryCoordinationRepository } from "../src/coordination/repository.ts";
import { createMemorySessionStore } from "../src/sessions/memory-session-store.ts";
import { createMemoryRunStore } from "../src/runs/memory-run-store.ts";
import {
  createMemoryPeerExecutionStore,
  createPostgresPeerExecutionStore,
} from "../src/coordination/execution-store.ts";

const database = process.env.COORDINATION_TEST_DATABASE_URL;
for (const backend of ["memory", "postgres"] as const) {
  test(
    `lowered execution limits survive concurrent admission and reconstruction: ${backend}`,
    { skip: backend === "postgres" && !database },
    async (t) => {
      const pool = createPgPool(database ?? "postgres://unused");
      t.after(() => pool.close());
      const org = randomUUID();
      const first =
        backend === "memory" ? createMemoryPeerExecutionStore() : createPostgresPeerExecutionStore(pool, org);
      const second = backend === "memory" ? first : createPostgresPeerExecutionStore(pool, org);
      await first.lowerLimits("root", "scope", 2, 3);
      await assert.rejects(second.lowerLimits("root", "scope", 3, 2), /only lower/);
      assert.equal((await first.usage("root", "scope")).turnLimit, 3);
      const input = { rootId: "root", scopeId: "scope", token: "token", autonomous: true };
      const results = await Promise.all(
        Array.from({ length: 10 }, (_, i) => (i % 2 ? first : second).admit({ ...input, runId: String(i) })),
      );
      assert.equal(results.filter((result) => result === "admitted").length, 2);
      await assert.rejects(first.lowerLimits("root", "scope", 1, 3), /current usage/);
      await assert.rejects(first.lowerLimits("root", "scope", 2, 1), /current usage/);
      for (const row of await first.active()) await first.release(row.runId, row.token);
      const changes = await Promise.allSettled([
        first.lowerLimits("root", "scope", 1, 3),
        second.lowerLimits("root", "scope", 2, 3),
      ]);
      assert.ok(changes.some((result) => result.status === "fulfilled"));
      assert.equal((await second.usage("root", "scope")).concurrencyLimit, 1);
      assert.equal(await second.admit({ ...input, runId: "last" }), "admitted");
      assert.equal(await first.admit({ ...input, rootId: "other", runId: "other-root" }), "concurrency_limit");
      await first.release("last", "token");
      assert.equal(await first.admit({ ...input, runId: "over" }), "turn_limit");
      assert.equal((await first.usage("other", "scope")).turnLimit, 200);
      assert.equal((await first.usage("root", "other-scope")).concurrencyLimit, 4);
      const raced = await Promise.allSettled([
        first.lowerLimits("race-root", "race-scope", 1, 1),
        ...Array.from({ length: 6 }, (_, i) =>
          second.admit({
            ...input,
            rootId: "race-root",
            scopeId: "race-scope",
            runId: `race-${i}`,
          }),
        ),
      ]);
      const raceUsage = await second.usage("race-root", "race-scope");
      assert.ok(raceUsage.activeInScope <= raceUsage.concurrencyLimit);
      assert.ok(raceUsage.autonomousTurns <= raceUsage.turnLimit);
      if (raced[0]!.status === "fulfilled") {
        assert.equal(raceUsage.concurrencyLimit, 1);
        assert.equal(raceUsage.turnLimit, 1);
        assert.equal(raceUsage.activeInScope, 1);
      } else {
        assert.match(String(raced[0]!.reason), /current usage/);
        assert.equal(raceUsage.concurrencyLimit, 4);
        assert.equal(raceUsage.turnLimit, 200);
      }
      if (backend === "postgres") {
        const restored = createPostgresPeerExecutionStore(pool, org, 8, 400);
        assert.equal((await restored.usage("root", "scope")).concurrencyLimit, 1);
        assert.equal((await restored.usage("root", "scope")).turnLimit, 3);
        const tightened = createPostgresPeerExecutionStore(pool, org, 1, 2);
        assert.equal((await tightened.usage("root", "scope")).turnLimit, 2);
      }
    },
  );
}
for (const backend of ["memory", "postgres"] as const) {
  test(
    `execution admission: ${backend} scope concurrency, root budgets and fenced retries`,
    { skip: backend === "postgres" && !database },
    async (t) => {
      const pools = [createPgPool(database ?? "postgres://unused"), createPgPool(database ?? "postgres://unused")];
      t.after(async () => {
        await Promise.all(pools.map((pool) => pool.close()));
      });
      const org = randomUUID();
      const first =
        backend === "memory"
          ? createMemoryPeerExecutionStore(2, 3)
          : createPostgresPeerExecutionStore(pools[0]!, org, 2, 3);
      const second = backend === "memory" ? first : createPostgresPeerExecutionStore(pools[1]!, org, 2, 3);
      const stores = [first, second];
      const input = { runId: "one", rootId: "root", scopeId: "scope", token: "attempt-one", autonomous: true };
      assert.equal(await first.wasReleased("release-test", 1), false);
      await first.recordRelease("release-test", 1, "first");
      await second.recordRelease("release-test", 1, "first");
      assert.equal(await second.wasTokenReleased("release-test", "first"), true);
      assert.equal(await second.wasTokenReleased("release-test", "second"), false);
      assert.equal(await second.wasTokenReleased("other-run", "first"), false);
      assert.equal(await second.wasReleased("release-test", 1), true);
      assert.equal(await second.wasReleased("release-test", 2), false);
      await first.recordStart("overlapping", 1);
      await second.recordStart("overlapping", 2);
      await second.recordRelease("overlapping", 2, "second");
      assert.equal(await first.wasReleased("overlapping", 2), false);
      await first.recordRelease("overlapping", 1, "first");
      assert.equal(await second.wasReleased("overlapping", 2), true);
      const terminated = {
        runId: "terminated",
        rootId: "recovery-root",
        scopeId: "recovery-scope",
        token: "terminated-token",
        autonomous: false,
      };
      assert.equal(await first.admit(terminated), "admitted");
      await first.recordStart(terminated.runId, 1);
      await first.recordRelease(terminated.runId, 1, terminated.token);
      assert.equal((await second.active()).length, 1);
      const recovery = createPeerExecution({
        store: second,
        repository: createMemoryCoordinationRepository(),
        sessions: createMemorySessionStore(),
        runs: createMemoryRunStore().runs,
      });
      await recovery.sweep();
      assert.deepEqual(await first.active(), []);
      assert.equal(await first.admit({ ...terminated, token: "replacement-token" }), "admitted");
      await recovery.sweep();
      assert.equal((await first.active())[0]?.token, "replacement-token");
      assert.equal((await first.active())[0]?.orphaned, true);
      await first.release(terminated.runId, "replacement-token");
      const results = await Promise.all(
        Array.from({ length: 20 }, (_, i) => stores[i % 2]!.admit({ ...input, runId: `run-${i}` })),
      );
      assert.equal(results.filter((result) => result === "admitted").length, 2);
      const active = await first.active();
      assert.equal(active.length, 2);
      const current = active[0]!;
      assert.equal(await second.admit(current), "admitted");
      assert.equal(await second.admit({ ...current, token: "different" }), "attempt_busy");
      await second.release(current.runId, "wrong-token");
      assert.equal((await first.active()).length, 2);
      await second.orphan(current.runId, current.token);
      assert.equal((await first.active()).find((row) => row.runId === current.runId)?.orphaned, true);
      assert.equal(await first.admit(current), "attempt_busy");
      assert.equal(await first.admit(input), "concurrency_limit");
      assert.deepEqual(await second.usage("root", "scope"), {
        activeInScope: 2,
        orphanedInScope: 1,
        autonomousTurns: 2,
        concurrencyLimit: 2,
        turnLimit: 3,
      });
      await second.release(current.runId, current.token);
      assert.equal(await first.admit(input), "admitted");
      await Promise.all((await first.active()).map((row) => first.release(row.runId, row.token)));
      assert.equal(await second.admit({ ...input, runId: "over-budget" }), "turn_limit");
      const human = { ...input, runId: "human-follow-up", autonomous: false };
      assert.equal(await second.admit(human), "admitted");
      await assert.rejects(first.admit({ ...human, autonomous: true }), /binding cannot change/);
      await first.release(human.runId, human.token);
      assert.equal(await second.admit({ ...current, token: "retry" }), "admitted");
      await first.orphan(current.runId, current.token);
      assert.equal((await second.active())[0]?.orphaned, false);
      await first.release(current.runId, current.token);
      assert.equal((await second.active())[0]?.token, "retry");
      await assert.rejects(first.admit({ ...current, rootId: "other-root" }), /binding cannot change/);
      assert.equal(await second.admit({ ...input, runId: "other-root", rootId: "other-root" }), "admitted");
      assert.equal(await first.admit({ ...input, runId: "third-root", rootId: "third-root" }), "concurrency_limit");
      assert.equal(
        await first.admit({ ...input, runId: "other-scope", rootId: "different", scopeId: "elsewhere" }),
        "admitted",
      );
    },
  );
}

test("default autonomous turn budget is two hundred distinct runs", async () => {
  const store = createMemoryPeerExecutionStore();
  for (let i = 0; i < 201; i++) {
    const input = { runId: String(i), rootId: "root", scopeId: "scope", token: "token", autonomous: true };
    assert.equal(await store.admit(input), i < 200 ? "admitted" : "turn_limit");
    await store.release(input.runId, input.token);
  }
});
