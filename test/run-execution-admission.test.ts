import assert from "node:assert/strict";
import { test } from "node:test";
import { createMemoryRunStore } from "../src/runs/memory-run-store.ts";
import { processRun } from "../src/runs/worker.ts";

test("tool admission checks current lease expiry and closes when the attempt finishes", async (t) => {
  const { runs } = createMemoryRunStore();
  const { run } = await runs.enqueue({
    sessionId: "tool-fence",
    request: {
      actor: { id: "owner", type: "internal" },
      conversation: { kind: "dm", threadRef: "tool-fence", audience: [] },
      text: "Work",
      origin: { kind: "human" },
    },
  });
  const claimed = (await runs.claimById(run.id, "worker", 60_000))!;
  let authorize: (() => Promise<void>) | undefined;
  await processRun(
    {
      runs,
      leaseTtlMs: 60_000,
      orchestrator: {
        async handleTurn(input) {
          authorize = input.authorizeToolExecution;
          assert.ok(authorize);
          await authorize();
          const clock = t.mock.method(Date, "now", () => claimed.leaseExpiresAt! + 1);
          await assert.rejects(authorize(), /no longer accepts tool execution/);
          clock.mock.restore();
          await authorize();
          return { status: "ok" };
        },
      },
    },
    claimed,
  );
  assert.ok(authorize);
  await assert.rejects(authorize(), /no longer accepts tool execution/);
});

test("tool admission closes before run-result persistence completes", async (t) => {
  const { runs } = createMemoryRunStore();
  const { run } = await runs.enqueue({
    sessionId: "persisting-tool-check",
    request: {
      actor: { id: "owner", type: "internal" },
      conversation: { kind: "dm", threadRef: "persisting-tool-check", audience: [] },
      text: "Work",
      origin: { kind: "human" },
    },
  });
  const claimed = (await runs.claimById(run.id, "worker", 60_000))!;
  let authorize: (() => Promise<void>) | undefined;
  const complete = runs.complete.bind(runs);
  t.mock.method(runs, "complete", async (...args: Parameters<typeof complete>) => {
    assert.equal((await runs.get(run.id))?.status, "running");
    assert.ok(authorize);
    await assert.rejects(authorize(), /no longer accepts tool execution/);
    return complete(...args);
  });
  await processRun(
    {
      runs,
      leaseTtlMs: 60_000,
      orchestrator: {
        async handleTurn(input) {
          authorize = input.authorizeToolExecution;
          await authorize!();
          return { status: "ok" };
        },
      },
    },
    claimed,
  );
});

test("tool admission cannot outlive completion while its lease lookup is pending", async (t) => {
  const { runs } = createMemoryRunStore();
  const { run } = await runs.enqueue({
    sessionId: "pending-tool-check",
    request: {
      actor: { id: "owner", type: "internal" },
      conversation: { kind: "dm", threadRef: "pending-tool-check", audience: [] },
      text: "Work",
      origin: { kind: "human" },
    },
  });
  const claimed = (await runs.claimById(run.id, "worker", 60_000))!;
  const result = Promise.withResolvers<typeof claimed>();
  let checking: Promise<void> | undefined;
  await processRun(
    {
      runs,
      leaseTtlMs: 60_000,
      orchestrator: {
        async handleTurn(input) {
          t.mock.method(runs, "get", () => result.promise);
          checking = input.authorizeToolExecution!();
          return { status: "ok" };
        },
      },
    },
    claimed,
  );
  assert.ok(checking);
  const refused = assert.rejects(checking, /no longer accepts tool execution/);
  result.resolve(claimed);
  await refused;
});

test("tool admission rejects a replaced lease without disturbing its new owner", async () => {
  const { runs } = createMemoryRunStore();
  const { run } = await runs.enqueue({
    sessionId: "replaced-tool-check",
    request: {
      actor: { id: "owner", type: "internal" },
      conversation: { kind: "dm", threadRef: "replaced-tool-check", audience: [] },
      text: "Work",
      origin: { kind: "human" },
    },
  });
  const claimed = (await runs.claimById(run.id, "old-worker", 60_000))!;
  let replacementToken: string | null = null;
  await assert.rejects(
    processRun(
      {
        runs,
        leaseTtlMs: 60_000,
        orchestrator: {
          async handleTurn(input) {
            await input.authorizeToolExecution!();
            await runs.releaseLease(run.id, claimed.leaseToken!);
            const replacement = await runs.claimById(run.id, "new-worker", 60_000);
            assert.ok(replacement);
            replacementToken = replacement.leaseToken;
            await assert.rejects(input.authorizeToolExecution!(), /no longer accepts tool execution/);
            return { status: "ok" };
          },
        },
      },
      claimed,
    ),
    /lost its lease before completion/,
  );
  assert.equal((await runs.get(run.id))?.leaseToken, replacementToken);
  assert.equal((await runs.get(run.id))?.status, "running");
});

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
