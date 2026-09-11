import { CoordinationError } from "../../coordination/types.ts";
import { sendJson } from "../http.ts";
import { activePrincipal, isObj } from "./shared.ts";
import type { ApiCtx, Route } from "./route.ts";

export async function peerActor(ctx: ApiCtx): Promise<string> {
  const actorId = ctx.capability?.actorId ?? ctx.actor?.p ?? ctx.url.searchParams.get("principalId");
  if (!actorId || !ctx.deps.identity || !(await activePrincipal(ctx.deps, actorId)))
    throw new CoordinationError(403, "forbidden", "an active organization member is required");
  return actorId;
}

export async function ownPeerSession(ctx: ApiCtx): Promise<string> {
  const cap = ctx.capability;
  if (!cap?.sessionId || !cap.runId || !ctx.deps.sessions || !ctx.deps.runs)
    throw new CoordinationError(403, "session_capability_required", "use the current session's agent capability");
  const [session, run] = await Promise.all([ctx.deps.sessions.get(cap.sessionId), ctx.deps.runs.get(cap.runId)]);
  if (
    !session ||
    session.scopeId !== cap.scopeId ||
    session.threadRef !== cap.threadRef ||
    !run ||
    run.sessionId !== session.threadRef ||
    run.request.actor.id !== cap.actorId ||
    run.status !== "running"
  )
    throw new CoordinationError(403, "session_capability_mismatch", "capability does not identify a running session");
  return session.id;
}

export function peerRoute(action: (ctx: ApiCtx) => Promise<unknown>): Route<ApiCtx>["handle"] {
  return async (ctx) => {
    try {
      if (!ctx.deps.peerIdentity) return sendJson(ctx.res, 404, { error: "coordination_disabled" });
      await peerActor(ctx);
      const result = await action(ctx);
      sendJson(ctx.res, 200, result);
    } catch (error) {
      if (!(error instanceof CoordinationError)) throw error;
      sendJson(ctx.res, error.status, { error: error.code, message: error.message, ...error.details });
    }
  };
}

export const peerRoutes: ReadonlyArray<Route<ApiCtx>> = [
  {
    method: "GET",
    path: "/v1/peers",
    auth: "either",
    handle: peerRoute(async (ctx) => {
      for (const session of (await ctx.deps.sessions?.scanAll()) ?? []) {
        await ctx.deps.peerIdentity!.ensure({ id: session.id, scopeId: session.scopeId });
      }
      return { peers: await ctx.deps.peerIdentity!.list() };
    }),
  },
  {
    method: "GET",
    path: "/v1/peers/self",
    auth: "either",
    handle: peerRoute(async (ctx) => ({ peer: await ctx.deps.peerIdentity!.get(await ownPeerSession(ctx)) })),
  },
  {
    method: "GET",
    path: "/v1/peers/:id",
    auth: "either",
    handle: peerRoute(async (ctx) => {
      const peer = await ctx.deps.peerIdentity!.get(ctx.params.id!);
      if (!peer) throw new CoordinationError(404, "peer_not_found", "agent not found");
      return { peer };
    }),
  },
  {
    method: "PUT",
    path: "/v1/peers/:id/character",
    auth: "either",
    handle: peerRoute(async (ctx) => {
      const id = ctx.params.id!;
      if (ctx.capability) {
        if (id !== (await ownPeerSession(ctx)))
          throw new CoordinationError(403, "forbidden", "agents may edit only their own character");
      } else if (!(await ctx.app.getSessionForViewer(id, await peerActor(ctx)))) {
        throw new CoordinationError(403, "forbidden", "session access required");
      }
      const body = isObj(ctx.body) ? ctx.body : {};
      if (!Number.isInteger(body.version) || (body.name !== undefined && typeof body.name !== "string"))
        throw new CoordinationError(400, "bad_request", "provide the current character version and an optional name");
      return {
        peer: await ctx.deps.peerIdentity!.replace(id, body.version as number, {
          character: body.character,
          name: body.name as string | undefined,
        }),
      };
    }),
  },
];
