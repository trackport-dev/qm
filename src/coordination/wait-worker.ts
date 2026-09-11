import { swallow } from "../util/errors.ts";
import type { PeerBoard } from "./board.ts";
import type { PeerDispatcher } from "./dispatcher.ts";
import type { CoordinationRepository } from "./repository.ts";
import type { PeerWait } from "./types.ts";
import type { PeerWaits } from "./waits.ts";

export function createPeerWaitWorker(deps: {
  repository: CoordinationRepository;
  waits: PeerWaits;
  board: Pick<PeerBoard, "publish">;
  dispatcher: Pick<PeerDispatcher, "dispatch">;
  sourceReleased(wait: PeerWait): Promise<boolean>;
}) {
  async function advance(id: string): Promise<void> {
    const wait = await deps.waits.refresh(id);
    if (wait.suspendedAt === null || (wait.state !== "ready" && wait.state !== "timed_out")) return;
    if (!(await deps.sourceReleased(wait))) return;
    const peer = await deps.repository.get("peer", wait.sessionId);
    if (peer?.state !== "active") return;
    const message = await deps.board.publish({
      senderId: wait.sessionId,
      senderRunId: wait.runId,
      idempotencyKey: `wait-continuation:${wait.id}`,
      audience: `.[] | select(._qm.id == ${JSON.stringify(wait.sessionId)})`,
      continuation: { kind: "reply_wait", waitId: wait.id },
      text: JSON.stringify({
        kind: "reply_wait_continuation",
        waitId: wait.id,
        messageId: wait.messageId,
        outcome: wait.state,
        replyIds: wait.replyIds,
      }),
    });
    await deps.dispatcher.dispatch(`${message.id}:${wait.sessionId}`);
    await deps.repository.transaction([`wait:${id}`], async (tx) => {
      const current = await tx.get("wait", id);
      if (!current || (current.state !== "ready" && current.state !== "timed_out")) return;
      const delivery = await tx.get("delivery", `${message.id}:${wait.sessionId}`);
      const resumed = delivery?.state === "consumed";
      if (current.resumeMessageId === message.id && current.resumeRunId === (delivery?.runId ?? null) && !resumed)
        return;
      await tx.put("wait", {
        ...current,
        resumeMessageId: message.id,
        resumeRunId: delivery?.runId ?? null,
        state: resumed ? "resumed" : current.state,
      });
      await tx.event("wait", id, Date.now());
    });
  }
  let cursor = "";
  return {
    advance,
    async sweep(): Promise<void> {
      const ids = await deps.repository.pendingWaitIds(cursor, 100);
      for (const id of ids) {
        try {
          await advance(id);
        } catch (error) {
          swallow("peer wait continuation", error);
        }
      }
      cursor = ids.length === 100 ? ids.at(-1)! : "";
    },
  };
}

export type PeerWaitWorker = ReturnType<typeof createPeerWaitWorker>;
