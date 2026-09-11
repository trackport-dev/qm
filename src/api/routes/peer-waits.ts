import { CoordinationError, type PeerWait } from "../../coordination/types.ts";
import { ownPeerSession, peerRoute } from "./peers.ts";
import { isObj } from "./shared.ts";
import type { ApiCtx, Route } from "./route.ts";

function waits(ctx: ApiCtx) {
  if (!ctx.deps.peerWaits) throw new CoordinationError(404, "coordination_disabled", "coordination is disabled");
  return ctx.deps.peerWaits;
}

function publicWait(wait: PeerWait) {
  return {
    id: wait.id,
    sessionId: wait.sessionId,
    messageId: wait.messageId,
    mode: wait.mode,
    recipientIds: wait.recipientIds,
    replyIds: wait.replyIds,
    state: wait.state,
    outcome: wait.outcome,
    createdAt: wait.createdAt,
    deadline: wait.deadline,
    suspendedAt: wait.suspendedAt,
    resumeMessageId: wait.resumeMessageId,
  };
}

export const peerWaitRoutes: ReadonlyArray<Route<ApiCtx>> = [
  {
    method: "GET",
    path: "/v1/peer-waits",
    auth: "either",
    handle: peerRoute(async (ctx) => {
      const query = ctx.url.searchParams;
      const page = await waits(ctx).list(
        query.get("messageId") ?? "",
        query.get("after") ?? "",
        query.has("limit") ? Number(query.get("limit")) : 50,
      );
      return { ...page, waits: page.waits.map(publicWait) };
    }),
  },
  {
    method: "POST",
    path: "/v1/peer-waits",
    auth: "either",
    handle: peerRoute(async (ctx) => {
      const sessionId = await ownPeerSession(ctx);
      const body = isObj(ctx.body) ? ctx.body : {};
      if (
        typeof body.messageId !== "string" ||
        typeof body.idempotencyKey !== "string" ||
        typeof body.timeoutMs !== "number" ||
        (body.mode !== undefined && body.mode !== "first" && body.mode !== "all")
      )
        throw new CoordinationError(
          400,
          "bad_request",
          "provide messageId, idempotencyKey, timeoutMs and optional mode first or all",
        );
      return {
        wait: publicWait(
          await waits(ctx).begin({
            sessionId,
            runId: ctx.capability!.runId!,
            messageId: body.messageId,
            idempotencyKey: body.idempotencyKey,
            timeoutMs: body.timeoutMs,
            mode: body.mode,
          }),
        ),
      };
    }),
  },
  {
    method: "GET",
    path: "/v1/peer-waits/:id",
    auth: "either",
    handle: peerRoute(async (ctx) => ({ wait: publicWait(await waits(ctx).get(ctx.params.id!)) })),
  },
];
