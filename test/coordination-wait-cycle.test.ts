import assert from "node:assert/strict";
import { test } from "node:test";
import { createMemoryCoordinationRepository } from "../src/coordination/repository.ts";
import { createPeerIdentity } from "../src/coordination/identity.ts";
import { createPeerBoard } from "../src/coordination/board.ts";
import { createPeerWaits } from "../src/coordination/waits.ts";
import { createPeerWaitWorker } from "../src/coordination/wait-worker.ts";
import { createPeerDispatcher } from "../src/coordination/dispatcher.ts";
import { createPeerDeliveryLedger } from "../src/coordination/delivery.ts";
import { createPeerExecution } from "../src/coordination/execution.ts";
import { createMemoryPeerExecutionStore } from "../src/coordination/execution-store.ts";
import { createMemorySessionStore } from "../src/sessions/memory-session-store.ts";
import { createMemoryRunStore } from "../src/runs/memory-run-store.ts";
import { createMemoryRunSignalStore } from "../src/runs/run-signal-store.ts";
import { processRun } from "../src/runs/worker.ts";
import { createAgentTools, type ToolContextRef } from "../src/harness/agent-tools.ts";
import type { Orchestrator } from "../src/core/orchestrator.ts";

test("one worker resolves reciprocal peer questions while suspended sessions release their leases", async () => {
  const repository = createMemoryCoordinationRepository();
  const sessions = createMemorySessionStore();
  const { runs } = createMemoryRunStore();
  const identity = createPeerIdentity(repository);
  const board = createPeerBoard(repository);
  const waits = createPeerWaits(repository);
  const ledger = createPeerDeliveryLedger(repository);
  const store = createMemoryPeerExecutionStore(1);
  const execution = createPeerExecution({ repository, sessions, runs, store });
  const parent = await sessions.getOrCreateByThread("parent-thread", "dm", "personal:owner");
  const child = await sessions.getOrCreateByThread("child-thread", "dm", "personal:owner");
  for (const session of [parent, child])
    await identity.ensure({
      id: session.id,
      scopeId: session.scopeId,
      authority: {
        actor: { id: "owner", type: "internal" },
        conversation: { kind: "dm", threadRef: session.threadRef, audience: [] },
        surface: "web",
      },
    });
  await repository.transaction([`peer:${child.id}`], async (tx) => {
    await tx.put("peer", {
      ...(await tx.get("peer", child.id))!,
      parentId: parent.id,
      rootId: parent.id,
      ancestors: [parent.id],
      sandboxId: "dedicated-child",
    });
  });
  const dispatcher = createPeerDispatcher({
    repository,
    identity,
    sessions,
    runs,
    signals: createMemoryRunSignalStore(),
    authorize: async (peer) => peer.authority,
    blocked: async () => false,
    prepareSteer: async () => "followup",
    enqueue: async (request) => {
      const session = await sessions.getByThread(request.conversation.threadRef);
      const peer = (await repository.get("peer", session!.id))!;
      const { run } = await runs.enqueue({
        sessionId: session!.threadRef,
        dedupKey: request.idempotencyKey,
        request: {
          ...peer.authority!,
          text: request.text,
          origin: request.origin!,
        },
      });
      return { status: "queued", runId: run.id };
    },
  });
  const continuation = createPeerWaitWorker({
    repository,
    waits,
    board,
    dispatcher,
    sourceReleased: (wait) => execution.sourceReleased(wait.runId),
  });
  const visited: string[] = [];
  let workId = "";
  let questionId = "";
  let parentWaitId = "";
  let childWaitId = "";
  const orchestrator: Pick<Orchestrator, "handleTurn"> = {
    async handleTurn(input) {
      const session = (await sessions.getByThread(input.conversation.threadRef))!;
      const { lease } = await sessions.acquireLease(session.id);
      assert.ok(lease);
      const origin = input.origin;
      if (origin.kind === "peer") assert.equal(await ledger.consume(origin.deliveryId, session.id, input.runId!), true);
      const publish = (text: string, recipient?: string, replyTo?: string) =>
        board.publish({
          senderId: session.id,
          senderRunId: input.runId!,
          idempotencyKey: text,
          text,
          audience: recipient ? `.[] | select(._qm.id == ${JSON.stringify(recipient)})` : "empty",
          replyTo,
        });
      const suspend = async (messageId: string) => {
        const wait = await waits.begin({
          sessionId: session.id,
          runId: input.runId!,
          messageId,
          idempotencyKey: messageId,
          timeoutMs: 60_000,
        });
        const ref: ToolContextRef = {
          current: null,
          onExecutionPending: input.onExecutionPending,
          suspendPeerWait: async (id) => {
            await waits.suspend(id, session.id, input.runId!);
          },
        };
        const tool = createAgentTools(ref).find((tool) => tool.name === "wait_for_peer")!;
        const result = await (
          tool.execute as unknown as (id: string, params: unknown) => Promise<{ terminate?: boolean }>
        )("wait", { wait_id: wait.id });
        assert.equal(result.terminate, true);
        return wait.id;
      };
      try {
        if (origin.kind === "human") {
          visited.push("parent requests work");
          workId = (await publish("Do work", child.id)).id;
          parentWaitId = await suspend(workId);
        } else if (origin.kind === "peer" && origin.messageId === workId) {
          visited.push("child asks question");
          questionId = (await publish("Which format?", parent.id)).id;
          childWaitId = await suspend(questionId);
        } else if (origin.kind === "peer" && origin.messageId === questionId) {
          visited.push("parent answers while waiting");
          assert.equal((await waits.get(parentWaitId)).state, "waiting");
          await publish("Use JSON", undefined, questionId);
        } else if (origin.kind === "peer" && origin.continuation?.waitId === childWaitId) {
          visited.push("child resumes");
          await publish("Work complete", undefined, workId);
        } else if (origin.kind === "peer" && origin.continuation?.waitId === parentWaitId) {
          visited.push("parent resumes");
        } else assert.fail("unexpected run");
        return { status: "silent" };
      } finally {
        await sessions.releaseLease(lease);
      }
    },
  };
  await runs.enqueue({
    sessionId: parent.threadRef,
    request: {
      actor: { id: "owner", type: "internal" },
      conversation: { kind: "dm", threadRef: parent.threadRef, audience: [] },
      text: "Start",
      origin: { kind: "human" },
    },
  });
  for (let round = 0; round < 10; round++) {
    await dispatcher.sweep();
    await continuation.sweep();
    const run = await runs.claim("only-worker", 60_000);
    if (!run) break;
    await processRun({ runs, execution, orchestrator, leaseTtlMs: 60_000 }, run);
    assert.equal((await store.active()).length, 0);
    for (const session of [parent, child]) {
      const probe = await sessions.acquireLease(session.id);
      assert.ok(probe.lease);
      await sessions.releaseLease(probe.lease);
    }
  }
  await continuation.sweep();
  assert.deepEqual(visited, [
    "parent requests work",
    "child asks question",
    "parent answers while waiting",
    "child resumes",
    "parent resumes",
  ]);
  assert.equal((await waits.get(parentWaitId)).state, "resumed");
  assert.equal((await waits.get(childWaitId)).state, "resumed");
  assert.equal((await runs.list()).length, 5);
});
