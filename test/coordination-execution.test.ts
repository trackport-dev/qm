import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { createPgPool } from "../src/persistence/pg-pool.ts";
import {
  createMemoryPeerExecutionStore,
  createPostgresPeerExecutionStore,
} from "../src/coordination/execution-store.ts";

const database = process.env.COORDINATION_TEST_DATABASE_URL;
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
