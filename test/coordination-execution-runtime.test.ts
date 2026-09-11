import assert from "node:assert/strict";
import { test } from "node:test";
import { createPeerExecution } from "../src/coordination/execution.ts";
import { createMemoryPeerExecutionStore } from "../src/coordination/execution-store.ts";
import { createPeerIdentity } from "../src/coordination/identity.ts";
import { createMemoryCoordinationRepository } from "../src/coordination/repository.ts";
import { createMemorySessionStore } from "../src/sessions/memory-session-store.ts";
import { createMemoryRunStore } from "../src/runs/memory-run-store.ts";
import { processRun } from "../src/runs/worker.ts";

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

for (const fault of ["start", "start_ack", "lease_before_start", "lease_after_start"] as const) {
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
