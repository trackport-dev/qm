import "./support/auto-fake-sprites.ts";
import assert from "node:assert/strict";
import { test } from "node:test";
import type { AddressInfo } from "node:net";
import { buildApp, serverDeps } from "../src/wiring.ts";
import { createServer } from "../src/api/server.ts";
import { mintCapabilityToken } from "../src/auth/capability-token.ts";
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
  };
  const published = await post(own, publication);
  assert.equal(published.status, 200, await published.clone().text());
  const message = ((await published.json()) as { message: { id: string; senderId: string } }).message;
  assert.equal(message.senderId, session.id);
  const detail = await request(`/v1/peer-messages/${message.id}`, bob);
  assert.equal(detail.status, 200, await detail.clone().text());
  assert.ok((await detail.text()).includes(publication.text));
  assert.equal((await post(bob, publication)).status, 403);
  assert.equal((await post(forged, publication)).status, 403);
  assert.equal((await post(bob, { audience: ".[]" }, "/v1/peer-messages/preview")).status, 200);
  assert.equal((await request("/v1/peer-messages?limit=-1", bob)).status, 400);
  await built.runs.complete(run.id, claimed.leaseToken!, { status: "silent" });
  assert.equal((await post(own, { ...publication, idempotencyKey: "stale" })).status, 403);
  assert.equal((await request(`/v1/peers/${peer.id}/character`, own, { version: 2, character: {} })).status, 403);
});
