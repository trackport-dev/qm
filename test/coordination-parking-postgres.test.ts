import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { setTimeout } from "node:timers/promises";
import { test } from "node:test";
import { createParkedPeerExecution, createPeerExecution } from "../src/coordination/execution.ts";
import { createPostgresPeerExecutionStore } from "../src/coordination/execution-store.ts";
import { createPeerIdentity } from "../src/coordination/identity.ts";
import { createPeerBoard } from "../src/coordination/board.ts";
import { createPeerDispatcher } from "../src/coordination/dispatcher.ts";
import { createPostgresCoordinationRepository } from "../src/coordination/repository.ts";
import { createPeerSpawning } from "../src/coordination/spawning.ts";
import { createPgPool } from "../src/persistence/pg-pool.ts";
import { createPostgresRunStore } from "../src/runs/postgres-run-store.ts";
import { createPostgresRunSignalStore } from "../src/runs/postgres-run-signal-store.ts";
import { processRun } from "../src/runs/worker.ts";
import { createPostgresSessionStore } from "../src/sessions/postgres-session-store.ts";
import type { CoordinationRepository } from "../src/coordination/repository.ts";
import type { RunStore } from "../src/runs/run-store.ts";
import type { SessionStore } from "../src/sessions/session-store.ts";

const database = process.env.COORDINATION_TEST_DATABASE_URL;

for (const origin of ["human", "peer"] as const) {
  test(`Postgres parked ${origin} child resumes after store reconstruction`, { skip: !database }, async (t) => {
    const org = randomUUID();
    const pool = createPgPool(database!);
    const runtime = createPostgresRunStore(database!);
    t.after(() => pool.close());
    t.after(() => runtime.close());
    const sessions = createPostgresSessionStore(database!);
    const repository = createPostgresCoordinationRepository(pool, org);
    const parent = await sessions.getOrCreateByThread(`parking:${org}`, "dm", `personal:${org}`);
    const authority = {
      actor: { id: org, type: "internal" as const },
      conversation: { kind: "dm" as const, threadRef: parent.threadRef, audience: [] },
      surface: "web",
    };
    await createPeerIdentity(repository).ensure({ id: parent.id, scopeId: parent.scopeId, authority });
    const spawn = await createPeerSpawning(repository).reserve({
      parentId: parent.id,
      parentRunId: randomUUID(),
      backend: "local",
      idempotencyKey: "child",
      name: "Child",
      task: "Work",
    });
    const child = await sessions.getOrCreateByThread(
      `parking-child:${org}`,
      "dm",
      parent.scopeId,
      undefined,
      "web",
      spawn.childId,
    );
    await repository.transaction([`peer:${child.id}`], async (tx) => {
      const peer = (await tx.get("peer", child.id))!;
      await tx.put("peer", { ...peer, state: "active", sandboxId: `sandbox:${org}` });
    });
    const signals = createPostgresRunSignalStore(database!);
    await createPeerIdentity(repository).ensure({
      id: child.id,
      scopeId: child.scopeId,
      authority: { ...authority, conversation: { ...authority.conversation, threadRef: child.threadRef } },
    });
    t.after(() => signals.close?.());
    let enqueues = 0;
    const dispatcherFor = (repo: CoordinationRepository, runs: RunStore, sessionStore: SessionStore) =>
      createPeerDispatcher({
        repository: repo,
        identity: createPeerIdentity(repo),
        runs,
        sessions: sessionStore,
        signals,
        authorize: async (peer) => peer.authority,
        blocked: async () => false,
        prepareSteer: async () => {
          assert.fail("A parked run must not be treated as an executing recipient");
        },
        enqueue: async (request) => {
          enqueues++;
          const { run } = await runs.enqueue({
            sessionId: child.threadRef,
            dedupKey: request.idempotencyKey,
            request: {
              ...authority,
              conversation: { ...authority.conversation, threadRef: child.threadRef },
              text: request.text,
              origin: request.origin!,
            },
          });
          return { status: "queued", runId: run.id };
        },
      });
    const message =
      origin === "peer"
        ? await createPeerBoard(repository).publish({
            senderId: parent.id,
            senderRunId: randomUUID(),
            idempotencyKey: "parked-message",
            text: "Continue working",
            audience: `.[] | select(._qm.id == ${JSON.stringify(child.id)})`,
          })
        : null;
    const deliveryId = message ? `${message.id}:${child.id}` : null;
    if (deliveryId) await dispatcherFor(repository, runtime.runs, sessions).dispatch(deliveryId);
    const run = deliveryId
      ? (await runtime.runs.get((await repository.get("delivery", deliveryId))!.runId!))!
      : (
          await runtime.runs.enqueue({
            sessionId: child.threadRef,
            request: {
              ...authority,
              conversation: { ...authority.conversation, threadRef: child.threadRef },
              text: "Continue working",
              origin: { kind: "human" },
            },
          })
        ).run;
    assert.ok(run, JSON.stringify(deliveryId ? await repository.get("delivery", deliveryId) : null));
    const claim = (await runtime.runs.claimById(run.id, "disabled", 60_000))!;
    assert.ok(claim);
    assert.equal(await runtime.runs.bindSession(run.id, claim.leaseToken!, child.id), true);
    const parked = await processRun(
      {
        runs: runtime.runs,
        execution: createParkedPeerExecution({ repository, sessions }),
        leaseTtlMs: 60_000,
        orchestrator: {
          async handleTurn() {
            assert.fail("Disabled child must not execute");
          },
        },
      },
      (await runtime.runs.get(run.id))!,
    );
    assert.equal(parked.reason, "coordination_paused");
    await runtime.close();
    await pool.close();

    const restoredPool = createPgPool(database!);
    const restoredRuntime = createPostgresRunStore(database!);
    t.after(() => restoredPool.close());
    t.after(() => restoredRuntime.close());
    const restoredRepository = createPostgresCoordinationRepository(restoredPool, org);
    const restoredSessions = createPostgresSessionStore(database!);
    const store = createPostgresPeerExecutionStore(restoredPool, org);
    const saved = (await restoredRuntime.runs.get(run.id))!;
    assert.equal(saved.status, "pending");
    assert.equal(saved.attempts, 0);
    assert.equal(saved.errorAttempts, 0);
    assert.equal(saved.sessionRecordId, child.id);
    assert.deepEqual(saved.request, run.request);
    assert.equal(
      await createParkedPeerExecution({
        repository: restoredRepository,
        sessions: restoredSessions,
      }).admit(saved),
      "coordination_paused",
    );
    assert.equal((await store.usage(parent.id, parent.scopeId)).autonomousTurns, 0);
    if (deliveryId) {
      const otherPool = createPgPool(database!);
      const otherRuntime = createPostgresRunStore(database!);
      t.after(() => otherPool.close());
      t.after(() => otherRuntime.close());
      await Promise.all([
        dispatcherFor(restoredRepository, restoredRuntime.runs, restoredSessions).dispatch(deliveryId),
        dispatcherFor(
          createPostgresCoordinationRepository(otherPool, org),
          otherRuntime.runs,
          createPostgresSessionStore(database!),
        ).dispatch(deliveryId),
      ]);
      const delivery = (await restoredRepository.get("delivery", deliveryId))!;
      assert.equal(delivery.state, "delivered");
      assert.equal(delivery.runId, run.id);
      assert.equal(enqueues, 1);
      assert.equal((await restoredRuntime.runs.get(run.id))?.attempts, 0);
      assert.deepEqual(await signals.takeLive(run.id), []);
    }
    await setTimeout(Math.max(0, (saved.availableAt ?? 0) - Date.now()) + 10);
    const resumed = (await restoredRuntime.runs.claimById(run.id, "enabled", 60_000))!;
    assert.ok(resumed);
    const execution = createPeerExecution({
      repository: restoredRepository,
      sessions: restoredSessions,
      runs: restoredRuntime.runs,
      store,
    });
    let entered = 0;
    await processRun(
      {
        runs: restoredRuntime.runs,
        execution,
        leaseTtlMs: 60_000,
        orchestrator: {
          async handleTurn(input) {
            entered++;
            assert.equal((await store.usage(parent.id, parent.scopeId)).activeInScope, 1);
            if (deliveryId) {
              const { lease } = await restoredSessions.acquireLease(child.id);
              assert.ok(lease);
              await restoredSessions.append(lease, {
                type: "user",
                payload: { text: input.text, peerOrigin: input.origin },
                scopeLabel: child.scopeId,
              });
              await restoredSessions.releaseLease(lease);
            }
            return { status: "ok", sessionId: child.id };
          },
        },
      },
      resumed,
    );
    assert.equal(entered, 1);
    assert.equal((await restoredRuntime.runs.get(run.id))?.status, "done");
    assert.equal((await restoredRuntime.runs.get(run.id))?.attempts, 1);
    assert.equal(await execution.sourceReleased(run.id), true);
    const usage = await store.usage(parent.id, parent.scopeId);
    assert.equal(usage.activeInScope, 0);
    assert.equal(usage.autonomousTurns, origin === "peer" ? 1 : 0);
    assert.equal((await createPeerSpawning(restoredRepository).inspect(parent.id)).count, 1);
    if (deliveryId) {
      const dispatcher = dispatcherFor(restoredRepository, restoredRuntime.runs, restoredSessions);
      await dispatcher.dispatch(deliveryId);
      await dispatcher.dispatch(deliveryId);
      assert.equal((await restoredRepository.get("delivery", deliveryId))?.state, "consumed");
      assert.equal(enqueues, 1);
    }
  });
}
