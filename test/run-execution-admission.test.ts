import assert from "node:assert/strict";
import { test } from "node:test";
import { createMemoryRunStore } from "../src/runs/memory-run-store.ts";
import { processRun } from "../src/runs/worker.ts";

for (const scenario of [
  "admitted",
  "concurrency_limit",
  "attempt_busy",
  "turn_limit",
  "release_failure",
  "pending_execution",
] as const) {
  test(`run execution admission: ${scenario}`, async () => {
    const outcome = scenario === "release_failure" || scenario === "pending_execution" ? "admitted" : scenario;
    const { runs } = createMemoryRunStore();
    const { run } = await runs.enqueue({
      sessionId: "thread",
      dedupKey: "task",
      request: {
        actor: { id: "owner", type: "internal" },
        conversation: { kind: "dm", threadRef: "thread", audience: [] },
        text: "Work",
        origin: { kind: "human" },
      },
    });
    const claimed = (await runs.claimById(run.id, "worker", 60_000))!;
    let executed = 0;
    let released = 0;
    let finish!: () => void;
    const operation = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const pending = processRun(
      {
        runs,
        leaseTtlMs: 60_000,
        orchestrator: {
          async handleTurn(input) {
            executed++;
            if (scenario === "pending_execution") input.onExecutionPending?.(operation);
            return { status: "ok" };
          },
        },
        execution: {
          async admit(input) {
            assert.equal(input.leaseToken, claimed.leaseToken);
            return outcome;
          },
          async release(input) {
            assert.equal(input.leaseToken, claimed.leaseToken);
            released++;
            if (scenario === "release_failure") throw new Error("ledger unavailable");
          },
        },
      },
      claimed,
    );
    if (outcome === "turn_limit") await assert.rejects(pending, /Autonomous turn limit reached/);
    else assert.equal((await pending).status, outcome === "admitted" ? "ok" : "queued");
    assert.equal(executed, outcome === "admitted" ? 1 : 0);
    if (scenario === "pending_execution") {
      assert.equal(released, 0);
      finish();
      await new Promise<void>((resolve) => setImmediate(resolve));
    }
    assert.equal(released, executed);
    const saved = (await runs.get(run.id))!;
    if (outcome === "concurrency_limit" || outcome === "attempt_busy") {
      assert.equal(saved.status, "pending");
      assert.equal(saved.attempts, 0);
      assert.equal(saved.errorAttempts, 0);
      assert.equal(await runs.claimById(run.id, "next", 60_000), null);
    } else assert.equal(saved.status, outcome === "admitted" ? "done" : "failed");
  });
}
