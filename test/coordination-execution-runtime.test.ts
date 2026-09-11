import assert from "node:assert/strict";
import { test } from "node:test";
import { createPeerExecution, createParkedPeerExecution } from "../src/coordination/execution.ts";
import { createPeerSpawning } from "../src/coordination/spawning.ts";
import { createMemoryPeerExecutionStore } from "../src/coordination/execution-store.ts";
import { createPeerIdentity } from "../src/coordination/identity.ts";
import { createMemoryCoordinationRepository } from "../src/coordination/repository.ts";
import { createMemorySessionStore } from "../src/sessions/memory-session-store.ts";
import { createMemoryRunStore } from "../src/runs/memory-run-store.ts";
import { processRun } from "../src/runs/worker.ts";

test("disabled coordination parks peer wakes and child turns without spending attempts", async () => {
  const repository = createMemoryCoordinationRepository();
  const sessions = createMemorySessionStore();
  const { runs } = createMemoryRunStore();
  const parent = await sessions.getOrCreateByThread("parking-parent", "dm", "personal:owner");
  const authority = {
    actor: { id: "owner", type: "internal" as const },
    conversation: { kind: "dm" as const, threadRef: parent.threadRef, audience: [] },
    surface: "web",
  };
  await createPeerIdentity(repository).ensure({ id: parent.id, scopeId: parent.scopeId, authority });
  const spawning = createPeerSpawning(repository);
  const spawn = await spawning.reserve({
    parentId: parent.id,
    parentRunId: "parent-run",
    backend: "local",
    idempotencyKey: "parking",
    name: "Child",
    task: "Work",
  });
  const child = await sessions.getOrCreateByThread(
    "parking-child",
    "dm",
    parent.scopeId,
    undefined,
    "web",
    spawn.childId,
  );
  const execution = createParkedPeerExecution({ repository, sessions });
  for (const scenario of ["peer", "child", "root"] as const) {
    const session = scenario === "child" ? child : parent;
    const { run } = await runs.enqueue({
      sessionId: session.threadRef,
      request: {
        ...authority,
        conversation: { ...authority.conversation, threadRef: session.threadRef },
        text: "Work",
        origin:
          scenario === "peer"
            ? {
                kind: "peer",
                messageId: "message",
                deliveryId: "delivery",
                senderSessionId: parent.id,
                senderName: "Parent",
                recipientSessionId: session.id,
              }
            : { kind: "human" },
      },
    });
    const claimed = (await runs.claimById(run.id, "worker", 60_000))!;
    let entered = false;
    const result = await processRun(
      {
        runs,
        execution,
        leaseTtlMs: 60_000,
        orchestrator: {
          async handleTurn() {
            entered = true;
            return { status: "ok" };
          },
        },
      },
      claimed,
    );
    assert.equal(entered, scenario === "root");
    assert.equal(result.status, scenario === "root" ? "ok" : "queued");
    if (scenario !== "root") {
      assert.equal(result.reason, "coordination_paused");
      const saved = (await runs.get(run.id))!;
      assert.equal(saved.status, "pending");
      assert.equal(saved.attempts, 0);
      assert.equal(saved.errorAttempts, 0);
      assert.deepEqual(saved.request, run.request);
    }
  }
  assert.equal((await spawning.inspect(parent.id)).count, 1);

  const { run: boundRun } = await runs.enqueue({
    sessionId: child.threadRef,
    request: {
      ...authority,
      conversation: { ...authority.conversation, threadRef: child.threadRef },
      text: "Resume child",
      origin: { kind: "human" },
    },
  });
  const boundClaim = (await runs.claimById(boundRun.id, "worker", 60_000))!;
  assert.equal(await runs.bindSession(boundRun.id, boundClaim.leaseToken!, child.id), true);
  const bound = (await runs.get(boundRun.id))!;
  await sessions.deleteSession(child.id);
  for (const recreated of [false, true]) {
    if (recreated) {
      const replacement = await sessions.getOrCreateByThread(child.threadRef, "dm", parent.scopeId);
      assert.notEqual(replacement.id, child.id);
      await createPeerIdentity(repository).ensure({ id: replacement.id, scopeId: replacement.scopeId });
    }
    assert.equal(await execution.admit(bound), "coordination_paused");
    assert.equal(
      await execution.admit({ ...bound, sessionRecordId: null, result: { status: "ok", sessionId: child.id } }),
      "coordination_paused",
    );
  }
  const parked = await processRun(
    {
      runs,
      execution,
      leaseTtlMs: 60_000,
      orchestrator: {
        async handleTurn() {
          assert.fail("A deleted child's bound run must remain parked");
        },
      },
    },
    bound,
  );
  assert.equal(parked.reason, "coordination_paused");
  const savedBound = (await runs.get(bound.id))!;
  assert.equal(savedBound.status, "pending");
  assert.equal(savedBound.attempts, 0);
  assert.equal(savedBound.errorAttempts, 0);
  assert.equal(savedBound.sessionRecordId, child.id);
  assert.deepEqual(savedBound.request, bound.request);
});

test("execution admission rejects deleted or replaced session bindings before recording a start", async () => {
  const repository = createMemoryCoordinationRepository();
  const sessions = createMemorySessionStore();
  const { runs } = createMemoryRunStore();
  const store = createMemoryPeerExecutionStore();
  const execution = createPeerExecution({ repository, sessions, runs, store });
  const session = await sessions.getOrCreateByThread("bound-admission", "dm", "personal:owner");
  await createPeerIdentity(repository).ensure({ id: session.id, scopeId: session.scopeId });
  const { run } = await runs.enqueue({
    sessionId: session.threadRef,
    request: {
      actor: { id: "owner", type: "internal" },
      conversation: { kind: "dm", threadRef: session.threadRef, audience: [] },
      text: "Work",
      origin: { kind: "human" },
    },
  });
  const claimed = (await runs.claimById(run.id, "worker", 60_000))!;
  assert.equal(await runs.bindSession(run.id, claimed.leaseToken!, session.id), true);
  const bound = (await runs.get(run.id))!;
  await sessions.deleteSession(session.id);
  let starts = 0;
  const guarded = createPeerExecution({
    repository,
    sessions,
    runs,
    store: {
      ...store,
      async recordStart() {
        starts++;
        assert.fail("Mismatched sessions must not record execution start");
      },
    },
  });
  for (const recreated of [false, true]) {
    if (recreated) {
      const replacement = await sessions.getOrCreateByThread(session.threadRef, "dm", session.scopeId);
      assert.notEqual(replacement.id, session.id);
      await createPeerIdentity(repository).ensure({ id: replacement.id, scopeId: replacement.scopeId });
    }
    for (const candidate of [
      bound,
      { ...bound, sessionRecordId: null, result: { status: "ok" as const, sessionId: session.id } },
    ])
      await assert.rejects(guarded.admit(candidate), /session no longer matches its run binding/);
  }
  await assert.rejects(
    processRun(
      {
        runs,
        execution: guarded,
        leaseTtlMs: 60_000,
        orchestrator: {
          async handleTurn() {
            assert.fail("A replaced session must not execute a bound run");
          },
        },
      },
      bound,
    ),
    /session no longer matches its run binding/,
  );
  assert.equal((await runs.get(run.id))?.status, "failed");
  assert.equal(starts, 0);
  assert.deepEqual(await store.active(), []);
  assert.equal(await execution.sourceReleased(run.id), false);
});

test("human root release receipts wait for underlying execution without consuming subagent capacity", async () => {
  const repository = createMemoryCoordinationRepository();
  const sessions = createMemorySessionStore();
  const { runs } = createMemoryRunStore();
  const store = createMemoryPeerExecutionStore();
  const execution = createPeerExecution({ repository, sessions, runs, store });
  const session = await sessions.getOrCreateByThread("root-thread", "dm", "personal:owner");
  await createPeerIdentity(repository).ensure({ id: session.id, scopeId: session.scopeId });
  const { run } = await runs.enqueue({
    sessionId: session.threadRef,
    request: {
      actor: { id: "owner", type: "internal" },
      conversation: { kind: "dm", threadRef: session.threadRef, audience: [] },
      text: "Work",
      origin: { kind: "human" },
    },
  });
  const claimed = (await runs.claimById(run.id, "worker", 60_000))!;
  let finish!: () => void;
  const pending = new Promise<void>((resolve) => {
    finish = resolve;
  });
  await processRun(
    {
      runs,
      execution,
      leaseTtlMs: 60_000,
      orchestrator: {
        async handleTurn(input) {
          input.onExecutionPending?.(pending);
          return { status: "silent" };
        },
      },
    },
    claimed,
  );
  assert.equal((await runs.get(run.id))?.status, "done");
  assert.equal(await execution.sourceReleased(run.id), false);
  assert.equal((await store.usage(session.id, session.scopeId)).activeInScope, 0);
  finish();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(await createPeerExecution({ repository, sessions, runs, store }).sourceReleased(run.id), true);
});

test("expired execution leases are visible as orphaned before run reaping without freeing capacity", async (t) => {
  const now = Date.now();
  t.mock.timers.enable({ apis: ["Date"], now });
  const repository = createMemoryCoordinationRepository();
  const sessions = createMemorySessionStore();
  const { runs } = createMemoryRunStore();
  const store = createMemoryPeerExecutionStore(1, 10);
  const execution = createPeerExecution({ repository, sessions, runs, store });
  const { run } = await runs.enqueue({
    sessionId: "orphan-visibility",
    request: {
      actor: { id: "owner", type: "internal" },
      conversation: { kind: "dm", threadRef: "orphan-visibility", audience: [] },
      text: "Work",
      origin: { kind: "human" },
    },
  });
  const claimed = (await runs.claimById(run.id, "worker", 60_000))!;
  const input = {
    runId: claimed.id,
    token: claimed.leaseToken!,
    rootId: "root",
    scopeId: "personal:owner",
    autonomous: true,
  };
  assert.equal(await store.admit(input), "admitted");
  await execution.sweep();
  assert.equal((await store.usage(input.rootId, input.scopeId)).orphanedInScope, 0);
  t.mock.timers.setTime(now + 60_000);
  await execution.sweep();
  assert.equal((await runs.get(run.id))?.status, "running");
  assert.equal((await runs.get(run.id))?.leaseToken, input.token);
  const usage = await store.usage(input.rootId, input.scopeId);
  assert.equal(usage.orphanedInScope, 1);
  assert.equal(usage.activeInScope, 1);
  assert.equal(await store.admit({ ...input, runId: "other" }), "concurrency_limit");
  assert.equal(await store.admit({ ...input, token: "replacement" }), "attempt_busy");
  assert.equal(await store.wasTokenReleased(input.runId, input.token), false);
  assert.equal(await runs.heartbeat(run.id, input.token, 60_000), true);
  await execution.sweep();
  const renewedUsage = await store.usage(input.rootId, input.scopeId);
  assert.equal(renewedUsage.orphanedInScope, 1);
  assert.equal(renewedUsage.activeInScope, 1);
  assert.equal(await store.wasTokenReleased(input.runId, input.token), false);
  await execution.release(claimed);
  assert.equal((await store.usage(input.rootId, input.scopeId)).activeInScope, 0);
});

test("retired attempts retain capacity until the owning execution releases it", async () => {
  const repository = createMemoryCoordinationRepository();
  const sessions = createMemorySessionStore();
  const { runs } = createMemoryRunStore();
  const store = createMemoryPeerExecutionStore(1, 1);
  const deps = { repository, sessions, runs, store };
  const execution = createPeerExecution(deps);
  const session = await sessions.getOrCreateByThread("thread", "dm", "personal:owner");
  await createPeerIdentity(repository).ensure({ id: session.id, scopeId: session.scopeId });
  const enqueue = async (human = false) => {
    const { run } = await runs.enqueue({
      sessionId: session.threadRef,
      request: {
        actor: { id: "owner", type: "internal" },
        conversation: { kind: "dm", threadRef: session.threadRef, audience: [] },
        text: "Work",
        origin: human
          ? { kind: "human" }
          : {
              kind: "peer",
              messageId: "message",
              deliveryId: "delivery",
              senderSessionId: "sender",
              senderName: "Sender",
              recipientSessionId: session.id,
            },
      },
    });
    return (await runs.claimById(run.id, "worker", 60_000))!;
  };
  const first = await enqueue();
  assert.equal(await execution.admit(first), "admitted");
  await createPeerExecution(deps).sweep();
  assert.equal((await store.active()).length, 1);
  await runs.releaseLease(first.id, first.leaseToken!);
  const retry = (await runs.claimById(first.id, "replacement", 60_000))!;
  assert.equal(await execution.admit(retry), "attempt_busy");
  await createPeerExecution(deps).sweep();
  assert.equal(await execution.admit(retry), "attempt_busy");
  assert.equal((await store.active())[0]?.orphaned, true);
  const release = store.release;
  store.release = async () => {
    throw new Error("capacity release interrupted after termination receipt");
  };
  await assert.rejects(execution.release(first), /capacity release interrupted/);
  assert.equal((await store.active()).length, 1);
  assert.equal(await store.wasTokenReleased(first.id, first.leaseToken!), true);
  store.release = release;
  await createPeerExecution(deps).sweep();
  assert.deepEqual(await store.active(), []);
  assert.equal(await execution.admit(retry), "admitted");
  await createPeerExecution(deps).sweep();
  assert.equal((await store.active())[0]?.token, retry.leaseToken);
  await execution.release(first);
  assert.equal((await store.active())[0]?.token, retry.leaseToken);
  await assert.rejects(execution.admit(first), /lost its run lease/);
  await runs.complete(retry.id, retry.leaseToken!, { status: "ok" });
  assert.equal(await execution.sourceReleased(retry.id), false);
  await createPeerExecution(deps).sweep();
  assert.equal((await store.active()).length, 1);
  await execution.release(retry);
  assert.equal(await execution.sourceReleased(retry.id), true);
  assert.deepEqual(await store.active(), []);
  const second = await enqueue();
  assert.equal(await execution.admit(second), "turn_limit");
  await runs.complete(second.id, second.leaseToken!, { status: "refused" });
  await repository.transaction([`peer:${session.id}`], async (tx) => {
    const peer = (await tx.get("peer", session.id))!;
    await tx.put("peer", { ...peer, parentId: "parent", sandboxId: "dedicated" });
  });
  const human = await enqueue(true);
  assert.equal(await execution.admit(human), "admitted");
  assert.equal((await store.active())[0]?.autonomous, false);
  await execution.release(human);
  await repository.transaction([`peer:${session.id}`], async (tx) => {
    const peer = (await tx.get("peer", session.id))!;
    await tx.put("peer", { ...peer, state: "paused" });
  });
  await assert.rejects(execution.admit(second), /not ready/);
});

test("a finished human-root retry cannot hide an older unreleased execution", async () => {
  const repository = createMemoryCoordinationRepository();
  const sessions = createMemorySessionStore();
  const { runs } = createMemoryRunStore();
  const store = createMemoryPeerExecutionStore();
  const execution = createPeerExecution({ repository, sessions, runs, store });
  const session = await sessions.getOrCreateByThread("overlap", "dm", "personal:owner");
  await createPeerIdentity(repository).ensure({ id: session.id, scopeId: session.scopeId });
  const { run } = await runs.enqueue({
    sessionId: session.threadRef,
    request: {
      actor: { id: "owner", type: "internal" },
      conversation: { kind: "dm", threadRef: session.threadRef, audience: [] },
      text: "Work",
      origin: { kind: "human" },
    },
  });
  const first = (await runs.claimById(run.id, "first", 60_000))!;
  assert.equal(await execution.admit(first), "admitted");
  await runs.releaseLease(run.id, first.leaseToken!);
  const second = (await runs.claimById(run.id, "second", 60_000))!;
  assert.equal(await execution.admit(second), "admitted");
  await runs.complete(run.id, second.leaseToken!, { status: "silent" });
  await execution.release(second);
  assert.equal(await execution.sourceReleased(run.id), false);
  await execution.release(first);
  assert.equal(await execution.sourceReleased(run.id), true);
});

for (const admitted of [false, true]) {
  test(`expired leases cannot enter execution; existing permit=${admitted}`, async (t) => {
    t.mock.timers.enable({ apis: ["Date"], now: 1_000 });
    const repository = createMemoryCoordinationRepository();
    const sessions = createMemorySessionStore();
    const { runs } = createMemoryRunStore();
    const store = createMemoryPeerExecutionStore();
    const execution = createPeerExecution({ repository, sessions, runs, store });
    const session = await sessions.getOrCreateByThread("expired", "dm", "personal:owner");
    await createPeerIdentity(repository).ensure({ id: session.id, scopeId: session.scopeId });
    const { run } = await runs.enqueue({
      sessionId: session.threadRef,
      request: {
        actor: { id: "owner", type: "internal" },
        conversation: { kind: "dm", threadRef: session.threadRef, audience: [] },
        text: "Work",
        origin: {
          kind: "peer",
          senderSessionId: "sender",
          senderName: "Sender",
          recipientSessionId: session.id,
          messageId: "message",
          deliveryId: "delivery",
        },
      },
    });
    const claim = (await runs.claimById(run.id, "worker", 100))!;
    if (admitted) assert.equal(await execution.admit(claim), "admitted");
    t.mock.timers.tick(100);
    assert.equal((await runs.get(run.id))?.status, "running");
    await assert.rejects(execution.admit(claim), /lost its run lease/);
    assert.equal((await store.active()).length, admitted ? 1 : 0);
    assert.equal(await store.wasTokenReleased(run.id, claim.leaseToken!), false);
  });
}

for (const fault of [
  "start",
  "start_ack",
  "lease_before_start",
  "lease_after_start",
  "expiry_before_start",
  "expiry_after_start",
] as const) {
  test(`unstarted execution releases acquired capacity after ${fault} failure`, async () => {
    const repository = createMemoryCoordinationRepository();
    const sessions = createMemorySessionStore();
    const { runs } = createMemoryRunStore();
    const store = createMemoryPeerExecutionStore();
    const session = await sessions.getOrCreateByThread("failure", "dm", "personal:owner");
    await createPeerIdentity(repository).ensure({ id: session.id, scopeId: session.scopeId });
    const { run } = await runs.enqueue({
      sessionId: session.threadRef,
      request: {
        actor: { id: "owner", type: "internal" },
        conversation: { kind: "dm", threadRef: session.threadRef, audience: [] },
        text: "Work",
        origin: {
          kind: "peer",
          senderSessionId: "sender",
          senderName: "Sender",
          recipientSessionId: session.id,
          messageId: "message",
          deliveryId: "delivery",
        },
      },
    });
    const claimed = (await runs.claimById(run.id, "worker", 60_000))!;
    let reads = 0;
    const execution = createPeerExecution({
      repository,
      sessions,
      runs: {
        ...runs,
        get: async (id) => {
          reads++;
          if ((fault === "lease_before_start" && reads === 2) || (fault === "lease_after_start" && reads === 3))
            throw new Error("lease query failed");
          if ((fault === "expiry_before_start" && reads === 2) || (fault === "expiry_after_start" && reads === 3))
            return { ...(await runs.get(id))!, leaseExpiresAt: Date.now() - 1 };
          return runs.get(id);
        },
      },
      store: {
        ...store,
        recordStart: async (id, attempt) => {
          if (fault === "start") throw new Error("start write failed");
          await store.recordStart(id, attempt);
          if (fault === "start_ack") throw new Error("start acknowledgement lost");
        },
      },
    });
    await assert.rejects(execution.admit(claimed), /failed|lost/);
    assert.deepEqual(await store.active(), []);
    assert.equal(await store.wasReleased(run.id, claimed.attempts), true);
  });
}
