import assert from "node:assert/strict";
import { test } from "node:test";
import { createMemoryCoordinationRepository } from "../src/coordination/repository.ts";
import { createPeerIdentity } from "../src/coordination/identity.ts";
import { createPeerSpawning } from "../src/coordination/spawning.ts";
import { createPeerSpawnWorker } from "../src/coordination/spawn-worker.ts";
import { createPeerBoard } from "../src/coordination/board.ts";
import { createMemorySessionStore } from "../src/sessions/memory-session-store.ts";
import type { SandboxResource } from "../src/sandbox/sandbox-resources.ts";
import { conversationScope } from "../src/resolution/resolution-service.ts";
import { publicSpawn } from "../src/coordination/types.ts";

for (const kind of ["dm", "group", "channel"] as const) {
  for (const explicitRef of [false, true]) {
    test(`spawn recovery preserves one child, computer, task and scope (${kind}, explicitRef=${explicitRef})`, async () => {
      const repository = createMemoryCoordinationRepository();
      const identity = createPeerIdentity(repository);
      const authority = {
        actor: { id: "owner", type: "internal" as const },
        conversation: {
          kind,
          threadRef: "web:owner:parent",
          audience: [],
          ...(explicitRef ? { channelRef: "shared" } : {}),
        },
        surface: "web",
      };
      const scope = conversationScope(authority.conversation, "owner");
      await identity.ensure({ id: "parent", scopeId: scope, authority });
      const spawning = createPeerSpawning(repository);
      const spawn = await spawning.reserve({
        parentId: "parent",
        parentRunId: "parent-run",
        backend: "local",
        idempotencyKey: "spawn",
        name: "Builder",
        character: { role: "worker" },
        task: "Build the thing",
      });
      const sessions = createMemorySessionStore();
      const computers = new Map<string, SandboxResource>();
      let failProvision = true;
      let failDispatch = true;
      const deps: Parameters<typeof createPeerSpawnWorker>[0] = {
        repository,
        sessions,
        board: createPeerBoard(repository),
        authorize: async () => authority,
        participants: async () => ["owner"],
        resources: {
          async create(actor, scope, backend, name, id) {
            assert.ok(id);
            const resource: SandboxResource = {
              id,
              ownerScopeId: scope,
              backend: "local",
              backingScopeId: `sandbox-${id}`,
              name: name!,
              createdBy: actor,
              createdAt: new Date().toISOString(),
              legacy: false,
              state: "ready",
            };
            computers.set(id, computers.get(id) ?? resource);
            if (failProvision) {
              failProvision = false;
              throw new Error("lost provision response");
            }
            return computers.get(id)!;
          },
        },
        dispatcher: {
          async dispatch(id) {
            await repository.transaction([`delivery:${id}`], async (tx) => {
              const delivery = (await tx.get("delivery", id))!;
              const child = (await tx.get("peer", delivery.recipientId))!;
              assert.ok(child.authority);
              assert.equal(child.sandboxId, spawn.childId);
              await tx.put("delivery", { ...delivery, runId: "initial-run" });
            });
            if (failDispatch) {
              failDispatch = false;
              throw new Error("lost enqueue response");
            }
          },
        },
      };
      await assert.rejects(createPeerSpawnWorker(deps).advance(spawn.id), /lost provision response/);
      assert.equal((await repository.get("spawn", spawn.id))?.state, "failed");
      const failedNode = (await spawning.tree("parent")).find((node) => node.peer.id === spawn.childId)!;
      assert.equal(failedNode.spawn?.state, "failed");
      assert.equal(failedNode.spawn?.attempts, 1);
      assert.equal(failedNode.spawn?.reason, "spawn_provisioning_failed");
      assert.ok(!JSON.stringify(failedNode).includes("lost provision response"));
      assert.equal(
        publicSpawn({ ...spawn, reason: "private credential material" }).reason,
        "spawn_provisioning_failed",
      );
      assert.equal((await repository.list("message")).length, 0);
      await assert.rejects(createPeerSpawnWorker(deps).advance(spawn.id), /lost enqueue response/);
      const worker = createPeerSpawnWorker(deps);
      const retries = await Promise.all(Array.from({ length: 10 }, () => worker.advance(spawn.id)));
      assert.ok(retries.some((result) => result.state === "ready" && result.runId === "initial-run"));
      assert.equal((await repository.get("spawn", spawn.id))?.state, "ready");
      assert.equal(computers.size, 1);
      const readyNode = (await spawning.tree("parent")).find((node) => node.peer.id === spawn.childId)!;
      assert.equal(readyNode.spawn?.state, "ready");
      assert.equal(readyNode.spawn?.attempts, 3);
      assert.equal(readyNode.spawn?.reason, null);
      assert.ok(!Object.hasOwn(readyNode.spawn!, "task"));
      assert.ok(!Object.hasOwn(readyNode.spawn!, "leaseToken"));
      assert.equal((await repository.list("message")).length, 1);
      assert.equal((await repository.list("delivery")).length, 1);
      const child = (await repository.get("peer", spawn.childId))!;
      assert.equal(child.authority?.conversation.threadRef, (await sessions.get(child.id))?.threadRef);
      assert.equal(child.authority?.actor.id, "owner");
      assert.equal(conversationScope(child.authority!.conversation, "owner"), scope);
      assert.equal((await sessions.get(child.id))?.scopeId, scope);
      assert.equal((await sessions.getEntries(child.id)).length, 0);
      const message = (await repository.list("message"))[0]!;
      assert.equal(message.senderRunId, "parent-run");
      assert.equal(message.text, "Build the thing");
      assert.deepEqual(message.recipientIds, [child.id]);
      assert.equal((await spawning.inspect("parent")).count, 1);
    });
  }
}
