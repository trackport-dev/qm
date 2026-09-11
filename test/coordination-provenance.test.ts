import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { createMemorySessionStore } from "../src/sessions/memory-session-store.ts";
import { createPostgresSessionStore } from "../src/sessions/postgres-session-store.ts";
import { createMemoryRunSignalStore, startSignalPoll } from "../src/runs/run-signal-store.ts";
import { projectTapeEntries } from "../src/harness/tape-projection.ts";
import { scopeId, type PeerOrigin } from "../src/types.ts";

const origin: PeerOrigin = {
  kind: "peer",
  messageId: "message",
  senderSessionId: "sender",
  senderName: "Builder",
  recipientSessionId: "recipient",
  deliveryId: "message:recipient",
};
const database = process.env.COORDINATION_TEST_DATABASE_URL;

test("peer steering carries provenance and does not count as human participation", async () => {
  const store = createMemoryRunSignalStore();
  await store.send("run", {
    kind: "steer",
    text: "A peer request",
    peerOrigin: origin,
    request: {
      actor: { externalId: "owner" },
      conversation: { kind: "dm", threadRef: "dm:owner" },
      surface: "web",
      text: "A peer request",
      origin,
    },
  });
  assert.deepEqual(await store.steerAuthors("run"), []);
  let observed: PeerOrigin | undefined;
  const stop = startSignalPoll(
    store,
    "run",
    {
      onSteer: async (_text, _ts, peer) => {
        observed = peer;
      },
      onAbort: async () => {},
    },
    { drainOnStop: true },
  );
  await stop();
  assert.deepEqual(observed, origin);
});

for (const backend of ["memory", "postgres"] as const) {
  test(
    `peer provenance: ${backend} survives persisted tape projection`,
    { skip: backend === "postgres" && !database },
    async () => {
      const store = backend === "memory" ? createMemorySessionStore() : createPostgresSessionStore(database!);
      const scope = scopeId("personal", "alice");
      const session = await store.getOrCreateByThread(`web:alice:${randomUUID()}`, "dm", scope);
      const { lease } = await store.acquireLease(session.id);
      assert.ok(lease);
      await store.appendTape(lease, {
        kind: "message",
        payload: { role: "user", content: [{ type: "text", text: "A peer request" }] },
        scopeLabel: scope,
        meta: { bareText: "A peer request", peerOrigin: origin },
      });
      await store.appendTape(lease, {
        kind: "annotation",
        payload: { turnEnd: true, render: 1, spanStart: 0 },
        scopeLabel: scope,
        entrySeq: 0,
      });
      const rows = await store.getTape(session.id);
      assert.deepEqual(rows[0]?.meta?.peerOrigin, origin);
      const projection = projectTapeEntries(session.id, rows);
      assert.ok(projection);
      assert.ok(projection.entries[0]);
      assert.deepEqual((projection.entries[0].payload as { peerOrigin?: PeerOrigin }).peerOrigin, origin);
      await store.releaseLease(lease);
    },
  );
}
