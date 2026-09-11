import { NonRetryableTurnError } from "../core/turn-error.ts";
import { resolveTurnOrigin } from "../core/turn-origin.ts";
import { conversationScope } from "../resolution/resolution-service.ts";
import type { Run, RunStore } from "../runs/run-store.ts";
import type { RunExecutionAdmission } from "../runs/worker.ts";
import type { SessionStore } from "../sessions/session-store.ts";
import type { PeerExecutionStore, ExecutionUsage } from "./execution-store.ts";
import type { CoordinationRepository } from "./repository.ts";

export function createPeerExecution(deps: {
  repository: CoordinationRepository;
  store: PeerExecutionStore;
  sessions: SessionStore;
  runs: RunStore;
}): RunExecutionAdmission & { sweep(): Promise<void>; usage(peerId: string): Promise<ExecutionUsage> } {
  async function ownsRun(run: Run): Promise<boolean> {
    const current = await deps.runs.get(run.id);
    return current?.status === "running" && current.leaseToken === run.leaseToken;
  }
  return {
    async usage(peerId) {
      const peer = await deps.repository.get("peer", peerId);
      if (!peer) throw new Error("Peer identity is unavailable.");
      return deps.store.usage(peer.rootId, peer.scopeId);
    },
    async admit(run) {
      const session = await deps.sessions.getByThread(run.sessionId);
      const peer = session ? await deps.repository.get("peer", session.id) : null;
      const origin = resolveTurnOrigin(run.request).kind;
      const fromPeer = origin === "peer";
      if (!peer) {
        if (fromPeer) throw new NonRetryableTurnError("Peer identity is unavailable.");
        return "admitted";
      }
      if (!peer.parentId && !fromPeer) return "admitted";
      if (peer.state !== "active" || (peer.parentId && !peer.sandboxId))
        throw new NonRetryableTurnError("Agent is not ready for autonomous execution.");
      if (conversationScope(run.request.conversation, run.request.actor.id) !== peer.scopeId)
        throw new NonRetryableTurnError("Agent execution scope does not match its identity.");
      if (!run.leaseToken || !(await ownsRun(run))) throw new Error("Execution admission lost its run lease.");
      const result = await deps.store.admit({
        runId: run.id,
        token: run.leaseToken,
        rootId: peer.rootId,
        scopeId: peer.scopeId,
        autonomous: origin !== "human" && origin !== "direct",
      });
      if (result === "admitted" && !(await ownsRun(run))) {
        await deps.store.release(run.id, run.leaseToken);
        throw new Error("Execution admission lost its run lease.");
      }
      return result;
    },
    async release(run) {
      if (run.leaseToken) await deps.store.release(run.id, run.leaseToken);
    },
    async sweep() {
      for (const execution of await deps.store.active()) {
        const run = await deps.runs.get(execution.runId);
        if (!run || run.status !== "running" || run.leaseToken !== execution.token)
          await deps.store.orphan(execution.runId, execution.token);
      }
    },
  };
}

export type PeerExecutionService = ReturnType<typeof createPeerExecution>;
