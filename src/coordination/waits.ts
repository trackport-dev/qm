import { createHash } from "node:crypto";
import type { CoordinationRepository, CoordinationTransaction } from "./repository.ts";
import { CoordinationError, type PeerWait } from "./types.ts";

export function createPeerWaits(repository: CoordinationRepository) {
  async function evaluate(tx: CoordinationTransaction, wait: PeerWait, now: number): Promise<PeerWait> {
    if (wait.state !== "waiting") return wait;
    const replies = await tx.firstReplies(wait.messageId, wait.recipientIds, wait.deadline);
    const selected = new Map<string, string>();
    for (const reply of replies) {
      if (!selected.has(reply.senderId)) selected.set(reply.senderId, reply.id);
      if (wait.mode === "first") break;
    }
    const ready =
      wait.recipientIds.length === 0 ||
      (wait.mode === "first" ? selected.size > 0 : selected.size === wait.recipientIds.length);
    let state: PeerWait["state"] = "waiting";
    if (ready) state = "ready";
    else if (now >= wait.deadline) state = "timed_out";
    const updated: PeerWait = {
      ...wait,
      replyIds: [...selected.values()],
      state,
      outcome: state === "waiting" ? null : state,
    };
    if (updated.state !== wait.state || JSON.stringify(updated.replyIds) !== JSON.stringify(wait.replyIds)) {
      await tx.put("wait", updated);
      await tx.event("wait", wait.id, now);
    }
    return updated;
  }
  return {
    async list(messageId: string, after = "", limit = 50) {
      if (!messageId || !Number.isInteger(limit) || limit < 1 || limit > 200)
        throw new CoordinationError(400, "invalid_wait_query", "provide messageId and a limit between 1 and 200");
      return repository.waitPage(messageId, after, limit);
    },
    async suspend(id: string, sessionId: string, runId: string): Promise<PeerWait> {
      return repository.transaction([`wait:${id}`], async (tx) => {
        const wait = await tx.get("wait", id);
        if (!wait) throw new CoordinationError(404, "wait_not_found", "wait not found");
        if (wait.sessionId !== sessionId || wait.runId !== runId)
          throw new CoordinationError(
            403,
            "wait_owner_required",
            "only the registering turn may suspend for this wait",
          );
        if (wait.state === "cancelled" || wait.state === "resumed")
          throw new CoordinationError(409, "wait_finished", "this wait has already finished");
        if (wait.suspendedAt !== null) return wait;
        const now = Date.now();
        const updated = { ...wait, suspendedAt: now };
        await tx.put("wait", updated);
        await tx.event("wait", id, now);
        return updated;
      });
    },
    async get(id: string): Promise<PeerWait> {
      const wait = await repository.get("wait", id);
      if (!wait) throw new CoordinationError(404, "wait_not_found", "wait not found");
      return wait;
    },
    async begin(input: {
      sessionId: string;
      runId: string;
      messageId: string;
      idempotencyKey: string;
      mode?: "first" | "all";
      timeoutMs: number;
    }): Promise<PeerWait> {
      if (!input.idempotencyKey || input.idempotencyKey.length > 200)
        throw new CoordinationError(400, "invalid_idempotency_key", "provide an idempotency key of 1–200 characters");
      if (!Number.isSafeInteger(input.timeoutMs) || input.timeoutMs < 1 || input.timeoutMs > 86_400_000)
        throw new CoordinationError(400, "invalid_wait_timeout", "timeout must be 1–86400000 milliseconds");
      const mode = input.mode ?? "first";
      if (mode !== "first" && mode !== "all")
        throw new CoordinationError(400, "invalid_wait_mode", "wait mode must be first or all");
      const id = createHash("sha256")
        .update(JSON.stringify([input.sessionId, input.idempotencyKey]))
        .digest("hex");
      return repository.transaction([`wait:${id}`, `message:${input.messageId}`], async (tx) => {
        const existing = await tx.get("wait", id);
        if (existing) {
          if (
            existing.messageId !== input.messageId ||
            existing.mode !== mode ||
            existing.timeoutMs !== input.timeoutMs
          )
            throw new CoordinationError(409, "wait_conflict", "idempotency key already identifies a different wait");
          return evaluate(tx, existing, Date.now());
        }
        const message = await tx.get("message", input.messageId);
        if (!message) throw new CoordinationError(404, "message_not_found", "wait target not found");
        if (message.senderId !== input.sessionId)
          throw new CoordinationError(
            403,
            "wait_owner_required",
            "agents may wait only for replies to their own messages",
          );
        const now = Date.now();
        const wait: PeerWait = {
          id,
          sessionId: input.sessionId,
          runId: input.runId,
          messageId: message.id,
          mode,
          recipientIds: [...message.recipientIds],
          replyIds: [],
          state: "waiting",
          createdAt: now,
          deadline: now + input.timeoutMs,
          timeoutMs: input.timeoutMs,
          suspendedAt: null,
          outcome: null,
          resumeMessageId: null,
          resumeRunId: null,
        };
        await tx.put("wait", wait);
        await tx.event("wait", id, now);
        return evaluate(tx, wait, now);
      });
    },
    async refresh(id: string): Promise<PeerWait> {
      const observed = await repository.get("wait", id);
      if (!observed) throw new CoordinationError(404, "wait_not_found", "wait not found");
      return repository.transaction([`wait:${id}`, `message:${observed.messageId}`], async (tx) => {
        const wait = await tx.get("wait", id);
        if (!wait) throw new CoordinationError(404, "wait_not_found", "wait not found");
        return evaluate(tx, wait, Date.now());
      });
    },
  };
}

export type PeerWaits = ReturnType<typeof createPeerWaits>;
