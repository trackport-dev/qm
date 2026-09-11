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

const database = process.env.COORDINATION_TEST_DATABASE_URL;
for (const backend of ["memory", "postgres"] as const) {
  test(
    `reply waits: ${backend} frozen audience, first/all, retries and deadlines`,
    { skip: backend === "postgres" && !database },
    async (t) => {
      const pool = createPgPool(database ?? "postgres://unused");
      t.after(() => pool.close());
      const repository =
        backend === "memory"
          ? createMemoryCoordinationRepository()
          : createPostgresCoordinationRepository(pool, randomUUID());
      for (const id of ["parent", "a", "b", "observer"])
        await createPeerIdentity(repository).ensure({ id, scopeId: "personal:owner" });
      const board = createPeerBoard(repository);
      const waits = createPeerWaits(repository);
      const message = await board.publish({
        senderId: "parent",
        senderRunId: "run",
        text: "Work",
        audience: '.[] | select(._qm.id == "a" or ._qm.id == "b")',
        idempotencyKey: "request",
      });
      const input = {
        sessionId: "parent",
        runId: "run",
        messageId: message.id,
        idempotencyKey: "all",
        mode: "all" as const,
        timeoutMs: 10_000,
      };
      const duplicates = await Promise.all(Array.from({ length: 10 }, () => createPeerWaits(repository).begin(input)));
      assert.ok(duplicates.every((wait) => wait.id === duplicates[0]!.id && wait.deadline === duplicates[0]!.deadline));
      const all = duplicates[0]!;
      assert.equal(all.suspendedAt, null);
      await assert.rejects(waits.suspend(all.id, "observer", input.runId), /registering turn/);
      await assert.rejects(waits.suspend(all.id, input.sessionId, "different-run"), /registering turn/);
      const suspended = await waits.suspend(all.id, input.sessionId, input.runId);
      assert.equal(typeof suspended.suspendedAt, "number");
      assert.deepEqual(await waits.suspend(all.id, input.sessionId, input.runId), suspended);
      assert.equal((await waits.begin(input)).suspendedAt, suspended.suspendedAt);
      const first = await waits.begin({ ...input, mode: "first", idempotencyKey: "first" });
      const expected = [all.id, first.id].sort();
      const page = await waits.list(message.id, "", 1);
      assert.deepEqual(
        page.waits.map((wait) => wait.id),
        expected.slice(0, 1),
      );
      assert.equal(page.hasMore, true);
      const next = await waits.list(message.id, page.nextCursor, 1);
      assert.deepEqual(
        next.waits.map((wait) => wait.id),
        expected.slice(1),
      );
      assert.equal(next.hasMore, false);
      assert.deepEqual((await waits.list("another-message")).waits, []);
      const reply = (senderId: string, key: string) =>
        board.publish({
          senderId,
          senderRunId: "reply-run",
          text: key,
          audience: "empty",
          replyTo: message.id,
          idempotencyKey: key,
        });
      await reply("observer", "not-selected");
      assert.equal((await waits.refresh(first.id)).state, "waiting");
      const a = await reply("a", "a-done");
      assert.equal((await waits.refresh(first.id)).state, "ready");
      assert.deepEqual((await waits.refresh(first.id)).replyIds, [a.id]);
      assert.equal((await waits.refresh(all.id)).state, "waiting");
      await reply("a", "duplicate-sender");
      assert.equal((await waits.refresh(all.id)).state, "waiting");
      const b = await reply("b", "b-done");
      assert.deepEqual((await waits.refresh(all.id)).replyIds, [a.id, b.id]);
      assert.equal((await waits.refresh(all.id)).state, "ready");
      const afterReplies = await waits.begin({ ...input, idempotencyKey: "already-answered" });
      assert.equal(afterReplies.state, "ready");
      assert.deepEqual(afterReplies.replyIds, [a.id, b.id]);
      assert.deepEqual(await repository.firstReplies(message.id, ["a", "a", "b"], Date.now()), [
        { id: a.id, senderId: "a" },
        { id: b.id, senderId: "b" },
      ]);
      assert.deepEqual(await repository.firstReplies(message.id, [], Date.now()), []);
      assert.deepEqual(await repository.firstReplies(message.id, ["a", "b"], 0), []);
      await assert.rejects(waits.begin({ ...input, mode: "first" }), /different wait/);
      await assert.rejects(waits.begin({ ...input, sessionId: "observer" }), /their own messages/);
      const unanswered = await board.publish({
        senderId: "parent",
        senderRunId: "run",
        text: "Unanswered",
        audience: '.[] | select(._qm.id == "b")',
        idempotencyKey: "unanswered",
      });
      const timeout = await waits.begin({ ...input, messageId: unanswered.id, idempotencyKey: "timeout" });
      t.mock.method(Date, "now", () => timeout.deadline + 1);
      await board.publish({
        senderId: "b",
        senderRunId: "late",
        text: "late",
        audience: "empty",
        replyTo: unanswered.id,
        idempotencyKey: "late",
      });
      assert.equal((await waits.refresh(timeout.id)).state, "timed_out");
      assert.deepEqual((await waits.refresh(timeout.id)).replyIds, []);
      assert.equal(
        (await waits.begin({ ...input, messageId: unanswered.id, idempotencyKey: "timeout" })).deadline,
        timeout.deadline,
      );
    },
  );
}
