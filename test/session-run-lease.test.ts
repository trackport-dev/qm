import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { createMemorySessionStore } from "../src/sessions/memory-session-store.ts";
import { createPostgresSessionStore } from "../src/sessions/postgres-session-store.ts";

const database = process.env.COORDINATION_TEST_DATABASE_URL;

for (const backend of ["memory", "postgres"] as const) {
  test(
    `${backend} session cleanup only releases its owning run attempt`,
    { skip: backend === "postgres" && !database },
    async () => {
      let now = Date.now();
      const options = { now: () => now, leaseTtlMs: 100 };
      const sessions =
        backend === "memory" ? createMemorySessionStore(options) : createPostgresSessionStore(database!, options);
      const session = await sessions.getOrCreateByThread(randomUUID(), "dm", "personal:lease-test");
      const owner = { runId: randomUUID(), runLeaseToken: randomUUID() };
      const first = (await sessions.acquireLease(session.id, "turn", owner)).lease;
      assert.ok(first);
      for (const wrong of [
        { ...owner, runId: randomUUID() },
        { ...owner, runLeaseToken: randomUUID() },
      ]) {
        await sessions.forceReleaseLease(session.id, wrong);
        assert.equal(await sessions.renewLease(first), true);
      }
      now += 101;
      const replacementOwner = { ...owner, runLeaseToken: randomUUID() };
      const replacement = (await sessions.acquireLease(session.id, "turn", replacementOwner)).lease;
      assert.ok(replacement);
      await sessions.forceReleaseLease(session.id, owner);
      assert.equal(await sessions.renewLease(replacement), true);
      await sessions.forceReleaseLease(session.id, replacementOwner);
      assert.equal(await sessions.renewLease(replacement), false);
      const unowned = (await sessions.acquireLease(session.id, "compaction")).lease;
      assert.ok(unowned);
      await sessions.forceReleaseLease(session.id, replacementOwner);
      assert.equal(await sessions.renewLease(unowned), true);
      await sessions.releaseLease(unowned);
      const reacquired = (await sessions.acquireLease(session.id, "turn", owner)).lease;
      assert.ok(reacquired);
      await sessions.forceReleaseLease(session.id, owner);
      assert.equal(await sessions.renewLease(reacquired), false);
      await sessions.deleteSession(session.id);
    },
  );
}
