import { NonRetryableTurnError } from "../core/turn-error.ts";
import { swallow } from "../util/errors.ts";
import { resolveTurnOrigin } from "../core/turn-origin.ts";
import { conversationScope } from "../resolution/resolution-service.ts";
import type { Run, RunStore } from "../runs/run-store.ts";
import type { RunExecutionAdmission } from "../runs/worker.ts";
import type { SessionStore } from "../sessions/session-store.ts";
import type { PeerExecutionStore, ExecutionUsage } from "./execution-store.ts";
import type { CoordinationRepository, CoordinationRunFence } from "./repository.ts";

export function createParkedPeerExecution(deps: {
  repository: CoordinationRepository;
  sessions: SessionStore;
}): RunExecutionAdmission {
  return {
    async admit(run) {
      if (resolveTurnOrigin(run.request).kind === "peer") return "coordination_paused";
      const boundId = run.sessionRecordId ?? run.result?.sessionId;
      if (boundId && (await deps.repository.get("peer", boundId))?.parentId) return "coordination_paused";
      const session = await deps.sessions.getByThread(run.sessionId);
      const peer = session ? await deps.repository.get("peer", session.id) : null;
      return peer?.parentId ? "coordination_paused" : "admitted";
    },
    async release() {},
  };
}

export function createPeerExecution(deps: {
  repository: CoordinationRepository;
  store: PeerExecutionStore;
  sessions: SessionStore;
  runs: RunStore;
}): RunExecutionAdmission & {
  sweep(): Promise<void>;
  sourceReleased(runId: string): Promise<boolean>;
  usage(peerId: string): Promise<ExecutionUsage>;
  lowerLimits(
    peerId: string,
    concurrencyLimit: number,
    turnLimit: number,
    fence?: CoordinationRunFence,
  ): Promise<ExecutionUsage>;
} {
  async function ownsRun(run: Run): Promise<boolean> {
    const current = await deps.runs.get(run.id);
    return (
      current?.status === "running" &&
      run.leaseToken !== null &&
      current.leaseToken === run.leaseToken &&
      current.leaseExpiresAt !== null &&
      current.leaseExpiresAt > Date.now()
    );
  }
  async function recordAdmission(run: Run): Promise<"admitted"> {
    try {
      if (!(await ownsRun(run))) throw new Error("Execution admission lost its run lease.");
      await deps.store.recordStart(run.id, run.attempts);
      if (!(await ownsRun(run))) throw new Error("Execution admission lost its run lease.");
      return "admitted";
    } catch (error) {
      const cleanup = await Promise.allSettled([
        ...(run.leaseToken ? [deps.store.release(run.id, run.leaseToken)] : []),
        deps.store.recordRelease(run.id, run.attempts, run.leaseToken),
      ]);
      for (const result of cleanup)
        if (result.status === "rejected") swallow("unstarted execution cleanup", result.reason);
      throw error;
    }
  }
  return {
    async lowerLimits(peerId, concurrencyLimit, turnLimit, fence) {
      const peer = await deps.repository.get("peer", peerId);
      if (!peer) throw new Error("Peer identity is unavailable.");
      await deps.store.lowerLimits(peer.rootId, peer.scopeId, concurrencyLimit, turnLimit, fence);
      return deps.store.usage(peer.rootId, peer.scopeId);
    },
    async sourceReleased(runId) {
      const run = await deps.runs.get(runId);
      return (
        !!run &&
        (run.status === "done" || run.status === "failed") &&
        (await deps.store.wasReleased(runId, run.attempts))
      );
    },
    async usage(peerId) {
      const peer = await deps.repository.get("peer", peerId);
      if (!peer) throw new Error("Peer identity is unavailable.");
      return deps.store.usage(peer.rootId, peer.scopeId);
    },
    async admit(run) {
      const session = await deps.sessions.getByThread(run.sessionId);
      const boundId = run.sessionRecordId ?? run.result?.sessionId;
      if (boundId && session?.id !== boundId)
        throw new NonRetryableTurnError("Agent execution session no longer matches its run binding.");
      const peer = session ? await deps.repository.get("peer", session.id) : null;
      const origin = resolveTurnOrigin(run.request).kind;
      const fromPeer = origin === "peer";
      if (!peer) {
        if (fromPeer) throw new NonRetryableTurnError("Peer identity is unavailable.");
        return recordAdmission(run);
      }
      if (!peer.parentId && !fromPeer) return recordAdmission(run);
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
      return result === "admitted" ? recordAdmission(run) : result;
    },
    async release(run) {
      await deps.store.recordRelease(run.id, run.attempts, run.leaseToken);
      if (run.leaseToken) await deps.store.release(run.id, run.leaseToken);
    },
    async sweep() {
      for (const execution of await deps.store.active()) {
        if (await deps.store.wasTokenReleased(execution.runId, execution.token)) {
          await deps.store.release(execution.runId, execution.token);
          continue;
        }
        const run = await deps.runs.get(execution.runId);
        if (
          !run ||
          run.status !== "running" ||
          run.leaseToken !== execution.token ||
          run.leaseExpiresAt === null ||
          run.leaseExpiresAt <= Date.now()
        )
          await deps.store.orphan(execution.runId, execution.token);
      }
    },
  };
}
