import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { createPeerIdentity } from "../src/coordination/identity.ts";
import {
  createMemoryCoordinationRepository,
  createPostgresCoordinationRepository,
} from "../src/coordination/repository.ts";
import { createMemorySessionStore } from "../src/sessions/memory-session-store.ts";
import { createPostgresSessionStore } from "../src/sessions/postgres-session-store.ts";
import { createMemoryRunStore } from "../src/runs/memory-run-store.ts";
import { createPostgresRunStore } from "../src/runs/postgres-run-store.ts";
import { createPgPool } from "../src/persistence/pg-pool.ts";
import { scopeId } from "../src/types.ts";
import type { OrchestratorInput } from "../src/core/orchestrator.ts";

const database = process.env.COORDINATION_TEST_DATABASE_URL;

for (const backend of ["memory", "postgres"] as const) {
  test(
    `historical authority: ${backend} backfill uses earliest executed internal owner without replaying privileges`,
    { skip: backend === "postgres" && !database },
    async (t) => {
      const pool = createPgPool(database ?? "postgres://unused");
      const repository =
        backend === "memory"
          ? createMemoryCoordinationRepository()
          : createPostgresCoordinationRepository(pool, randomUUID());
      const runtime = backend === "memory" ? createMemoryRunStore() : createPostgresRunStore(database!);
      const { runs } = runtime;
      t.after(async () => {
        await runs.close?.();
        await pool.close();
      });
      const sessions = createMemorySessionStore();
      const thread = `authority:${randomUUID()}`;
      const scope = scopeId("group", thread);
      const session = await sessions.getOrCreateByThread(thread, "group", scope);
      const request: OrchestratorInput = {
        actor: { id: "alice", type: "internal" },
        conversation: { kind: "group", threadRef: thread, audience: [] },
        surface: "web",
        text: "Original task",
        origin: { kind: "automation", useOwnerKeychain: true },
        botActor: true,
        unattendedGrants: ["private-grant"],
      };
      const guest = await runs.enqueue({
        sessionId: thread,
        request: { ...request, actor: { id: "guest", type: "guest" } },
      });
      const guestClaim = await runs.claimById(guest.run.id, "worker", 30_000);
      assert.ok(guestClaim);
      await runs.complete(guest.run.id, guestClaim.leaseToken!, { status: "silent" });
      const pending = await runs.enqueue({
        sessionId: thread,
        request: { ...request, actor: { id: "never-ran", type: "internal" } },
      });
      assert.equal(await runs.firstExecutedForThread(thread), null);
      const original = await runs.enqueue({ sessionId: thread, request });
      const claim = await runs.claimById(original.run.id, "worker", 30_000);
      assert.ok(claim);
      await runs.complete(original.run.id, claim.leaseToken!, { status: "ok", sessionId: session.id });
      const later = await runs.enqueue({
        sessionId: thread,
        request: { ...request, actor: { id: "bob", type: "internal" } },
      });
      const laterClaim = await runs.claimById(later.run.id, "worker", 30_000);
      assert.ok(laterClaim);
      await runs.complete(later.run.id, laterClaim.leaseToken!, { status: "ok", sessionId: session.id });
      for (let i = 0; i < 205; i++) await runs.enqueue({ sessionId: `unrelated:${randomUUID()}`, request });
      await createPeerIdentity(repository).ensure({ id: session.id, scopeId: scope });
      const identity = createPeerIdentity(repository, { sessions, runs });
      runs.list = async () => {
        throw new Error("backfill must use a targeted lookup");
      };
      const proposedAuthority = { actor: later.run.request.actor, conversation: request.conversation, surface: "web" };
      const peers = await Promise.all([
        identity.ensure({ id: session.id, scopeId: scope }),
        identity.ensure({ id: session.id, scopeId: scope, authority: proposedAuthority }),
      ]);
      for (const peer of peers) {
        assert.equal(peer.authority?.actor.id, "alice");
        assert.deepEqual(Object.keys(peer.authority!).sort(), ["actor", "conversation", "surface"]);
      }
      assert.equal((await runs.firstExecutedForThread(thread))?.id, original.run.id);
      assert.equal((await runs.get(pending.run.id))?.status, "pending");
      assert.equal((await repository.get("peer", session.id))?.authority?.actor.id, "alice");
    },
  );
}

for (const mismatch of ["thread", "kind", "scope", "unexecuted"] as const) {
  test(`historical authority rejects ${mismatch} evidence instead of accepting a new sender`, async () => {
    const repository = createMemoryCoordinationRepository();
    const sessions = createMemorySessionStore();
    const { runs } = createMemoryRunStore();
    const scope = scopeId("personal", "alice");
    const session = await sessions.getOrCreateByThread("original", "dm", scope);
    const request: OrchestratorInput = {
      actor: { id: mismatch === "scope" ? "bob" : "alice", type: "internal" },
      conversation: {
        kind: mismatch === "kind" ? "group" : "dm",
        threadRef: mismatch === "thread" ? "wrong" : "original",
        audience: [],
      },
      origin: { kind: "human" },
      text: "Old task",
    };
    const { run } = await runs.enqueue({ sessionId: session.threadRef, request });
    if (mismatch !== "unexecuted") assert.ok(await runs.claimById(run.id, "worker", 30_000));
    const identity = createPeerIdentity(repository, { sessions, runs });
    const peer = await identity.ensure({ id: session.id, scopeId: scope, authority: { ...request, surface: "web" } });
    assert.equal(peer.authority, null);
  });
}

test(
  "Postgres historical hydration above pool capacity does not hold connections across store reads",
  { skip: !database, timeout: 30_000 },
  async (t) => {
    const pool = createPgPool(database!);
    const repository = createPostgresCoordinationRepository(pool, randomUUID());
    const sessions = createPostgresSessionStore(database!);
    const runtime = createPostgresRunStore(database!);
    t.after(async () => {
      await runtime.close();
      await pool.close();
    });
    const identity = createPeerIdentity(repository, { sessions, runs: runtime.runs });
    const fixtures = await Promise.all(
      Array.from({ length: 30 }, async () => {
        const threadRef = `concurrent-authority:${randomUUID()}`;
        const session = await sessions.getOrCreateByThread(threadRef, "dm", scopeId("personal", "owner"));
        const { run } = await runtime.runs.enqueue({
          sessionId: threadRef,
          request: {
            actor: { id: "owner", type: "internal" },
            conversation: { kind: "dm", threadRef, audience: [] },
            text: "Original",
            origin: { kind: "human" },
          },
        });
        const claim = await runtime.runs.claimById(run.id, "worker", 30_000);
        assert.ok(claim);
        await runtime.runs.complete(run.id, claim.leaseToken!, { status: "ok", sessionId: session.id });
        return session;
      }),
    );
    const peers = await Promise.all(
      fixtures.map((session) => identity.ensure({ id: session.id, scopeId: session.scopeId })),
    );
    assert.equal(peers.length, 30);
    assert.ok(peers.every((peer) => peer.authority?.actor.id === "owner"));
  },
);
