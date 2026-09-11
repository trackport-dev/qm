import assert from "node:assert/strict";
import { test } from "node:test";
import { createPeerExecution } from "../src/coordination/execution.ts";
import { createMemoryPeerExecutionStore } from "../src/coordination/execution-store.ts";
import { createPeerIdentity } from "../src/coordination/identity.ts";
import { createMemoryCoordinationRepository } from "../src/coordination/repository.ts";
import { createMemorySessionStore } from "../src/sessions/memory-session-store.ts";
import { createMemoryRunStore } from "../src/runs/memory-run-store.ts";

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
  await execution.release(first);
  assert.equal(await execution.admit(retry), "admitted");
  await execution.release(first);
  assert.equal((await store.active())[0]?.token, retry.leaseToken);
  await assert.rejects(execution.admit(first), /lost its run lease/);
  await runs.complete(retry.id, retry.leaseToken!, { status: "ok" });
  await createPeerExecution(deps).sweep();
  assert.equal((await store.active()).length, 1);
  await execution.release(retry);
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
