import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { createPgPool } from "../src/persistence/pg-pool.ts";
import {
  createMemoryCoordinationRepository,
  createPostgresCoordinationRepository,
} from "../src/coordination/repository.ts";
import { createPeerIdentity } from "../src/coordination/identity.ts";
import { createPeerBoard } from "../src/coordination/board.ts";
import { createPeerWaits } from "../src/coordination/waits.ts";
import { createPeerWaitWorker } from "../src/coordination/wait-worker.ts";
import { createPeerDeliveryLedger } from "../src/coordination/delivery.ts";

const database = process.env.COORDINATION_TEST_DATABASE_URL;
for (const backend of ["memory", "postgres"] as const) {
  test(
    `wait continuation: ${backend} release gating, lost publication acknowledgement and consumption`,
    { skip: backend === "postgres" && !database },
    async (t) => {
      const pool = createPgPool(database ?? "postgres://unused");
      t.after(() => pool.close());
      const repository =
        backend === "memory"
          ? createMemoryCoordinationRepository()
          : createPostgresCoordinationRepository(pool, randomUUID());
      const identity = createPeerIdentity(repository);
      for (const id of ["parent", "worker"]) await identity.ensure({ id, scopeId: "personal:owner" });
      const board = createPeerBoard(repository);
      const waits = createPeerWaits(repository);
      const message = await board.publish({
        senderId: "parent",
        senderRunId: "source-run",
        text: "Work",
        audience: '.[] | select(._qm.id == "worker")',
        idempotencyKey: "request",
      });
      const wait = await waits.begin({
        sessionId: "parent",
        runId: "source-run",
        messageId: message.id,
        idempotencyKey: "wait",
        timeoutMs: 10_000,
      });
      let released = false;
      let loseAcknowledgement = true;
      let dispatches = 0;
      const makeWorker = () =>
        createPeerWaitWorker({
          repository,
          waits,
          board: {
            publish: async (input) => {
              const saved = await board.publish(input);
              if (loseAcknowledgement) {
                loseAcknowledgement = false;
                throw new Error("lost publication acknowledgement");
              }
              return saved;
            },
          },
          sourceReleased: async () => released,
          dispatcher: {
            dispatch: async () => {
              dispatches++;
            },
          },
        });
      const worker = makeWorker();
      assert.deepEqual(await repository.pendingWaitIds("", 100), [wait.id]);
      await board.publish({
        senderId: "worker",
        senderRunId: "reply-run",
        text: "Done",
        audience: "empty",
        replyTo: message.id,
        idempotencyKey: "reply",
      });
      await worker.sweep();
      assert.equal((await waits.get(wait.id)).state, "ready");
      assert.equal(dispatches, 0);
      assert.deepEqual(await repository.pendingWaitIds("", 100), []);
      await waits.suspend(wait.id, "parent", "source-run");
      assert.deepEqual(await repository.pendingWaitIds("", 100), [wait.id]);
      await worker.advance(wait.id);
      assert.equal((await repository.list("message")).length, 2);
      released = true;
      await repository.transaction(["peer:parent"], async (tx) => {
        await tx.put("peer", { ...(await tx.get("peer", "parent"))!, state: "paused" });
      });
      await worker.advance(wait.id);
      assert.equal((await repository.list("message")).length, 2);
      await repository.transaction(["peer:parent"], async (tx) => {
        await tx.put("peer", { ...(await tx.get("peer", "parent"))!, state: "active" });
      });
      await assert.rejects(worker.advance(wait.id), /lost publication/);
      assert.equal((await repository.list("message")).length, 3);
      assert.equal((await waits.get(wait.id)).resumeMessageId, null);
      await Promise.all([makeWorker().advance(wait.id), makeWorker().advance(wait.id)]);
      const queued = await waits.get(wait.id);
      assert.ok(queued.resumeMessageId);
      assert.equal(queued.state, "ready");
      const continuation = (await board.get(queued.resumeMessageId)).message;
      assert.deepEqual(continuation.continuation, { kind: "reply_wait", waitId: wait.id });
      assert.equal(continuation.replyTo, null);
      assert.deepEqual(continuation.recipientIds, ["parent"]);
      assert.equal((await repository.list("message")).length, 3);
      assert.ok(!continuation.text.includes("source-run"));
      assert.equal(JSON.parse(continuation.text).instruction, undefined);
      const ledger = createPeerDeliveryLedger(repository);
      const delivery = await ledger.claim(`${continuation.id}:parent`);
      assert.ok(delivery);
      await ledger.bind(delivery.id, delivery.leaseToken!, "resume-run");
      await ledger.consume(delivery.id, "parent", "resume-run");
      await worker.advance(wait.id);
      const resumed = await waits.get(wait.id);
      assert.equal(resumed.state, "resumed");
      assert.equal(resumed.outcome, "ready");
      assert.equal(resumed.resumeRunId, "resume-run");
      assert.deepEqual(await repository.pendingWaitIds("", 100), []);
      const before = dispatches;
      await makeWorker().sweep();
      assert.equal(dispatches, before);
      const unanswered = await board.publish({
        senderId: "parent",
        senderRunId: "timeout-source",
        text: "Unanswered",
        audience: '.[] | select(._qm.id == "worker")',
        idempotencyKey: "timeout-request",
      });
      const timeout = await waits.begin({
        sessionId: "parent",
        runId: "timeout-source",
        messageId: unanswered.id,
        idempotencyKey: "timeout-wait",
        timeoutMs: 1000,
      });
      await waits.suspend(timeout.id, "parent", "timeout-source");
      t.mock.method(Date, "now", () => timeout.deadline + 1);
      await makeWorker().sweep();
      const timedOut = await waits.get(timeout.id);
      assert.equal(timedOut.state, "timed_out");
      assert.equal(timedOut.outcome, "timed_out");
      assert.ok(timedOut.resumeMessageId);
      const notification = (await board.get(timedOut.resumeMessageId)).message;
      assert.equal(JSON.parse(notification.text).outcome, "timed_out");
      assert.deepEqual(JSON.parse(notification.text).replyIds, []);
      assert.deepEqual(await repository.pendingWaitIds(timeout.id, 100), []);
      await assert.rejects(repository.pendingWaitIds("", 0), /invalid/);
    },
  );
}
