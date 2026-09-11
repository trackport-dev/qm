import "./support/auto-fake-sprites.ts";
import assert from "node:assert/strict";
import { test } from "node:test";
import type { AddressInfo } from "node:net";
import { buildApp, serverDeps } from "../src/wiring.ts";
import { createServer } from "../src/api/server.ts";
import { mintCapabilityToken } from "../src/auth/capability-token.ts";
import { signedHeaders } from "../plugins/chassis/src/core-client.ts";
import { testConfig, TEST_CAPABILITY_SECRET } from "./support/test-config.ts";
import { scopeId } from "../src/types.ts";

test("peer routes expose public character, enforce session attribution, and preserve transcript access", async (t) => {
  const config = testConfig({
    coordinationEnabled: true,
    signingSecret: "coordination-ingress-test-secret-00000000000",
  });
  const built = buildApp(config);
  const server = createServer(built.app, serverDeps(config, built));
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(async () => {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  const personal = scopeId("personal", "alice");
  const session = await built.sessions.getOrCreateByThread("web:alice:private", "dm", personal, undefined, "web");
  await built.sessions.updateTitle(session.id, "Private acquisition discussion");
  await built.sessions.addParticipant(session.id, "alice");
  const peer = await built.peerIdentity!.ensure({ id: session.id, scopeId: personal });
  const { run } = await built.runs.enqueue({
    sessionId: session.threadRef,
    request: {
      actor: { id: "alice", type: "internal" },
      conversation: { kind: "dm", threadRef: session.threadRef, audience: [] },
      text: "work",
      origin: { kind: "human" },
      surface: "web",
    },
  });
  const claimed = await built.runs.claimById(run.id, "route-test", 60_000);
  assert.ok(claimed);
  const token = (actorId: string, extra: Record<string, unknown> = {}) =>
    mintCapabilityToken(
      {
        actorId,
        scopeId: scopeId("personal", actorId),
        aud: "control-plane",
        exp: Date.now() + 60_000,
        ...extra,
      },
      TEST_CAPABILITY_SECRET,
    );
  const own = await token("alice", { sessionId: session.id, runId: run.id, threadRef: session.threadRef });
  const request = async (path: string, cap: string, body?: unknown) =>
    fetch(base + path, {
      method: body ? "PUT" : "GET",
      headers: { "x-agent-capability": cap, "content-type": "application/json" },
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
  const self = await request("/v1/peers/self", own);
  assert.equal(self.status, 200, await self.clone().text());
  assert.equal(((await self.json()) as { peer: { id: string } }).peer.id, session.id);
  const updated = await request(`/v1/peers/${peer.id}/character`, own, {
    version: 1,
    character: { role: "worker" },
    name: "Builder",
  });
  assert.equal(updated.status, 200, await updated.clone().text());
  const bob = await token("bob");
  const peers = await request("/v1/peers", bob);
  assert.equal(peers.status, 200, await peers.clone().text());
  const publicText = await peers.text();
  assert.ok(publicText.includes("Builder"));
  for (const privateValue of ["Private acquisition", "web:alice:private", "personal:alice", "authority", "sandboxId"])
    assert.ok(!publicText.includes(privateValue), privateValue);
  const refused = await request(`/v1/peers/${peer.id}/character`, bob, { version: 2, character: {} });
  assert.equal(refused.status, 403);
  assert.equal(await built.app.getSessionForViewer(session.id, "bob"), null);
  const stale = await request(`/v1/peers/${peer.id}/character`, own, { version: 1, character: {} });
  assert.equal(stale.status, 409);
  const forged = await token("alice", { sessionId: "other", runId: run.id, threadRef: session.threadRef });
  assert.equal((await request("/v1/peers/self", forged)).status, 403);
  const post = (cap: string, body: unknown, path = "/v1/peer-messages") =>
    fetch(base + path, {
      method: "POST",
      headers: { "x-agent-capability": cap, "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  const publication = {
    text: "Public progress update",
    audience: "empty",
    idempotencyKey: "progress",
    senderId: "forged",
    continuation: { kind: "reply_wait", waitId: "forged" },
  };
  const spawnRequest = {
    task: "Build independently",
    name: "Child",
    character: { role: "worker" },
    idempotencyKey: "child",
    parentId: "forged",
    parentRunId: "forged",
    backend: "forged",
  };
  assert.equal((await post(bob, spawnRequest, "/v1/peer-spawns")).status, 403);
  assert.equal((await post(forged, spawnRequest, "/v1/peer-spawns")).status, 403);
  const spawned = await post(own, spawnRequest, "/v1/peer-spawns");
  assert.equal(spawned.status, 200, await spawned.clone().text());
  const operation = (
    (await spawned.json()) as { spawn: { id: string; parentId: string; childId: string; state: string } }
  ).spawn;
  assert.equal(operation.parentId, session.id);
  assert.equal(operation.state, "reserved");
  const storedSpawn = await built.coordinationRepository.get("spawn", operation.id);
  assert.equal(storedSpawn?.parentRunId, run.id);
  assert.equal(storedSpawn?.backend, config.sandboxBackend);
  const duplicateSpawn = await post(own, spawnRequest, "/v1/peer-spawns");
  assert.deepEqual(await duplicateSpawn.json(), { spawn: operation });
  const subtree = await request(`/v1/peers/${session.id}/subtree`, bob);
  assert.equal(subtree.status, 200, await subtree.clone().text());
  const subtreeBody = (await subtree.json()) as { count: number; cap: number; peer: Record<string, unknown> };
  assert.equal(subtreeBody.count, 1);
  assert.equal(subtreeBody.cap, 16);
  assert.equal(subtreeBody.peer.authority, undefined);
  assert.equal(subtreeBody.peer.sandboxId, undefined);
  const lifecyclePath = `/v1/peers/${session.id}/lifecycle`;
  assert.equal((await post(own, { action: "stop" }, lifecyclePath)).status, 403);
  const humanAction = async (actor: string, action: string, subtree = false) => {
    const path = `${lifecyclePath}?principalId=${actor}`;
    const body = JSON.stringify({ action, subtree });
    return fetch(base + path, {
      method: "POST",
      body,
      headers: { ...signedHeaders(config.signingSecret, "POST", path, body), "content-type": "application/json" },
    });
  };
  assert.equal((await humanAction("bob", "stop")).status, 403);
  const paused = await humanAction("alice", "pause", true);
  assert.equal(paused.status, 200, await paused.clone().text());
  assert.equal((await built.coordinationRepository.get("peer", operation.childId))?.state, "paused");
  assert.equal((await humanAction("alice", "resume", true)).status, 200);
  assert.equal((await built.coordinationRepository.get("peer", operation.childId))?.state, "active");
  assert.equal((await request(`/v1/peers/${operation.childId}/subtree-limit`, own, { limit: 0 })).status, 403);
  assert.equal((await request(`/v1/peers/${session.id}/subtree-limit`, own, { limit: 0 })).status, 409);
  assert.equal((await request(`/v1/peers/${session.id}/subtree-limit`, own, { limit: 1 })).status, 200);
  assert.equal((await post(own, { ...spawnRequest, idempotencyKey: "overflow" }, "/v1/peer-spawns")).status, 409);
  const published = await post(own, publication);
  assert.equal(published.status, 200, await published.clone().text());
  const message = ((await published.json()) as { message: { id: string; senderId: string } }).message;
  assert.equal(message.senderId, session.id);
  assert.equal((await built.coordinationRepository.get("message", message.id))?.continuation, undefined);
  const detail = await request(`/v1/peer-messages/${message.id}`, bob);
  assert.equal(detail.status, 200, await detail.clone().text());
  assert.ok((await detail.text()).includes(publication.text));
  assert.equal((await post(bob, publication)).status, 403);
  assert.equal((await post(forged, publication)).status, 403);
  assert.equal((await post(bob, { audience: ".[]" }, "/v1/peer-messages/preview")).status, 200);
  assert.equal((await request("/v1/peer-messages?limit=-1", bob)).status, 400);
  const waitRequest = {
    messageId: message.id,
    idempotencyKey: "reply-wait",
    timeoutMs: 1000,
    sessionId: "forged",
    runId: "forged",
  };
  assert.equal((await post(bob, waitRequest, "/v1/peer-waits")).status, 403);
  assert.equal((await post(forged, waitRequest, "/v1/peer-waits")).status, 403);
  assert.equal((await post(own, { ...waitRequest, mode: "invalid" }, "/v1/peer-waits")).status, 400);
  const registered = await post(own, waitRequest, "/v1/peer-waits");
  assert.equal(registered.status, 200, await registered.clone().text());
  const registeredBody = (await registered.json()) as { wait: { id: string; sessionId: string; state: string } };
  assert.equal(registeredBody.wait.sessionId, session.id);
  assert.equal(registeredBody.wait.state, "ready");
  assert.equal((await built.coordinationRepository.get("wait", registeredBody.wait.id))?.runId, run.id);
  assert.deepEqual(await (await post(own, waitRequest, "/v1/peer-waits")).json(), registeredBody);
  assert.equal((await post(own, { ...waitRequest, timeoutMs: 2000 }, "/v1/peer-waits")).status, 409);
  const waitDetail = await request(`/v1/peer-waits/${registeredBody.wait.id}`, bob);
  assert.equal(waitDetail.status, 200, await waitDetail.clone().text());
  assert.deepEqual(await waitDetail.json(), registeredBody);
  const waitPage = await request(`/v1/peer-waits?messageId=${message.id}&limit=1`, bob);
  assert.equal(waitPage.status, 200);
  assert.deepEqual(await waitPage.json(), {
    waits: [registeredBody.wait],
    nextCursor: registeredBody.wait.id,
    hasMore: false,
  });
  assert.equal((await request("/v1/peer-waits", bob)).status, 400);
  assert.equal((await request(`/v1/peer-waits?messageId=${message.id}&limit=201`, bob)).status, 400);
  const publicWaitText = JSON.stringify(registeredBody);
  for (const privateValue of [run.id, "runId", "resumeRunId", session.threadRef, personal])
    assert.ok(!publicWaitText.includes(privateValue), privateValue);
  assert.equal((await request("/v1/peer-waits/missing", bob)).status, 404);
  await built.runs.complete(run.id, claimed.leaseToken!, { status: "silent" });
  assert.equal((await post(own, { ...waitRequest, idempotencyKey: "stale" }, "/v1/peer-waits")).status, 403);
  assert.equal((await post(own, { ...publication, idempotencyKey: "stale" })).status, 403);
  assert.equal((await request(`/v1/peers/${peer.id}/character`, own, { version: 2, character: {} })).status, 403);
});
