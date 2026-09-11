import { createHash, randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { characterObject, DEFAULT_DESCENDANT_LIMIT, peerName } from "./identity.ts";
import type { CoordinationRepository } from "./repository.ts";
import {
  CoordinationError,
  publicPeer,
  publicSpawn,
  type Peer,
  type PeerSpawn,
  type PublicPeer,
  type PublicSpawn,
} from "./types.ts";

function descendantCount(peers: Peer[], ancestorId: string): number {
  return peers.filter((peer) => peer.state !== "deleted" && peer.ancestors.includes(ancestorId)).length;
}

export function createPeerSpawning(repository: CoordinationRepository) {
  async function requiredPeer(id: string): Promise<Peer> {
    const peer = await repository.get("peer", id);
    if (!peer || peer.state === "deleted") throw new CoordinationError(404, "peer_not_found", "agent not found");
    return peer;
  }
  return {
    async reserve(input: {
      parentId: string;
      parentRunId: string;
      backend: PeerSpawn["backend"];
      idempotencyKey: string;
      task: string;
      name: string;
      character?: unknown;
    }): Promise<PeerSpawn> {
      if (!input.idempotencyKey || input.idempotencyKey.length > 200)
        throw new CoordinationError(400, "invalid_idempotency_key", "provide an idempotency key of 1–200 characters");
      if (!input.task.trim() || Buffer.byteLength(input.task) > 65_536 || input.task.includes("\u0000"))
        throw new CoordinationError(400, "invalid_task", "task must contain 1–65536 bytes of text");
      const name = peerName(input.name);
      const character = characterObject(input.character ?? {});
      const observed = await requiredPeer(input.parentId);
      const id = createHash("sha256")
        .update(JSON.stringify([input.parentId, input.idempotencyKey]))
        .digest("hex");
      return repository.transaction([`tree:${observed.rootId}`], async (tx) => {
        const existing = await tx.get("spawn", id);
        if (existing) {
          if (
            existing.task !== input.task ||
            existing.backend !== input.backend ||
            existing.initialName !== name ||
            !isDeepStrictEqual(existing.initialCharacter, character)
          )
            throw new CoordinationError(409, "spawn_conflict", "idempotency key already identifies a different spawn");
          return existing;
        }
        const parent = await tx.get("peer", input.parentId);
        if (!parent || parent.rootId !== observed.rootId || parent.state !== "active" || !parent.authority)
          throw new CoordinationError(
            409,
            "spawn_parent_unavailable",
            "parent must be active with execution authority",
          );
        const ancestors = [...parent.ancestors, parent.id];
        const peers = await tx.list("peer", { rootId: parent.rootId });
        for (const ancestorId of ancestors) {
          const ancestor = peers.find((peer) => peer.id === ancestorId);
          if (!ancestor) throw new CoordinationError(409, "invalid_ancestry", "spawn ancestry is incomplete");
          const count = descendantCount(peers, ancestorId);
          if (count >= ancestor.descendantLimit)
            throw new CoordinationError(409, "subtree_limit_exceeded", "subtree descendant limit reached", {
              ancestorId,
              count,
              cap: ancestor.descendantLimit,
            });
        }
        const now = Date.now();
        const child: Peer = {
          id: randomUUID(),
          name,
          character,
          version: 1,
          scopeId: parent.scopeId,
          parentId: parent.id,
          rootId: parent.rootId,
          ancestors,
          descendantLimit: DEFAULT_DESCENDANT_LIMIT,
          state: "active",
          authority: null,
          sandboxId: null,
          createdAt: now,
          updatedAt: now,
        };
        const spawn: PeerSpawn = {
          id,
          parentId: parent.id,
          parentRunId: input.parentRunId,
          backend: input.backend,
          childId: child.id,
          rootId: parent.rootId,
          task: input.task,
          initialName: name,
          initialCharacter: character,
          leaseToken: null,
          leaseUntil: 0,
          attempts: 0,
          state: "reserved",
          sandboxId: null,
          runId: null,
          reason: null,
          createdAt: now,
          updatedAt: now,
        };
        await tx.put("peer", child);
        await tx.put("spawn", spawn);
        await tx.event("peer", child.id, now);
        await tx.event("spawn", id, now);
        return spawn;
      });
    },
    async lowerLimit(id: string, limit: number): Promise<{ count: number; cap: number }> {
      if (!Number.isSafeInteger(limit) || limit < 0 || limit > DEFAULT_DESCENDANT_LIMIT)
        throw new CoordinationError(400, "invalid_subtree_limit", "limit must be an integer between zero and sixteen");
      const observed = await requiredPeer(id);
      return repository.transaction([`tree:${observed.rootId}`, `peer:${id}`], async (tx) => {
        const peer = await tx.get("peer", id);
        if (!peer || peer.state === "deleted") throw new CoordinationError(404, "peer_not_found", "agent not found");
        const count = descendantCount(await tx.list("peer", { rootId: peer.rootId }), id);
        if (limit > peer.descendantLimit || limit < count)
          throw new CoordinationError(
            409,
            "invalid_subtree_limit",
            "limits may only decrease and must fit existing descendants",
            {
              ancestorId: id,
              count,
              cap: peer.descendantLimit,
            },
          );
        const now = Date.now();
        await tx.put("peer", { ...peer, descendantLimit: limit, updatedAt: now });
        await tx.event("peer", id, now);
        return { count, cap: limit };
      });
    },
    async inspect(id: string): Promise<{ peer: Peer; count: number; cap: number }> {
      const peer = await requiredPeer(id);
      const count = descendantCount(await repository.list("peer", { rootId: peer.rootId }), id);
      return { peer, count, cap: peer.descendantLimit };
    },
    async tree(id: string): Promise<Array<{ peer: PublicPeer; count: number; cap: number; spawn?: PublicSpawn }>> {
      const root = await requiredPeer(id);
      const peers = await repository.list("peer", { rootId: root.rootId });
      const spawns = new Map(
        (await repository.list("spawn", { rootId: root.rootId })).map((spawn) => [spawn.childId, publicSpawn(spawn)]),
      );
      return peers
        .filter((peer) => peer.state !== "deleted" && (peer.id === id || peer.ancestors.includes(id)))
        .map((peer) => ({
          peer: publicPeer(peer),
          count: descendantCount(peers, peer.id),
          cap: peer.descendantLimit,
          ...(spawns.has(peer.id) ? { spawn: spawns.get(peer.id)! } : {}),
        }));
    },
    async transition(id: string, action: "pause" | "resume" | "stop", subtree = false): Promise<PublicPeer[]> {
      const states = { pause: "paused", resume: "active", stop: "stopped" } as const;
      if (!Object.hasOwn(states, action))
        throw new CoordinationError(400, "invalid_lifecycle_action", "unknown agent action");
      const state = states[action];
      const observed = await requiredPeer(id);
      for (;;) {
        const snapshot = subtree ? await repository.list("peer", { rootId: observed.rootId }) : [observed];
        const locked = new Set(snapshot.map((peer) => peer.id));
        const result = await repository.transaction(
          [`tree:${observed.rootId}`, ...snapshot.map((peer) => `peer:${peer.id}`)],
          async (tx) => {
            const root = await tx.get("peer", id);
            if (!root || root.state === "deleted")
              throw new CoordinationError(404, "peer_not_found", "agent not found");
            const peers = subtree ? await tx.list("peer", { rootId: root.rootId }) : [root];
            const targets = peers.filter((peer) => peer.id === id || (subtree && peer.ancestors.includes(id)));
            if (targets.some((peer) => !locked.has(peer.id))) return null;
            const now = Date.now();
            const changed: PublicPeer[] = [];
            for (const peer of targets) {
              if (peer.state === "deleted" || peer.state === "archived") continue;
              if (peer.state === state) continue;
              const updated: Peer = { ...peer, state, updatedAt: now };
              await tx.put("peer", updated);
              await tx.event("peer", peer.id, now);
              changed.push(publicPeer(updated));
            }
            return changed;
          },
        );
        if (result) return result;
      }
    },
  };
}

export type PeerSpawning = ReturnType<typeof createPeerSpawning>;
