import assert from "node:assert/strict";
import { test } from "node:test";
import { createPeerInputReceipts, hasUserInput } from "../src/harness/peer-input-receipts.ts";
import type { PeerOrigin } from "../src/types.ts";

const origin: PeerOrigin = {
  kind: "peer",
  messageId: "message",
  deliveryId: "delivery",
  senderSessionId: "sender",
  senderName: "Sender",
  recipientSessionId: "recipient",
};

test("peer receipts recognize exact user text across provider payloads, not assistant or tool output", () => {
  for (const payload of [
    { messages: [{ role: "user", content: "peer text" }] },
    { messages: [{ role: "user", content: [{ type: "text", text: "peer text" }] }] },
    { input: [{ role: "user", content: [{ type: "input_text", text: "peer text" }] }] },
    { contents: [{ role: "user", parts: [{ text: "peer text" }] }] },
  ])
    assert.equal(hasUserInput(payload, "peer text"), true);
  for (const payload of [
    { messages: [{ role: "assistant", content: "peer text" }] },
    { messages: [{ role: "tool", content: "peer text" }] },
    { messages: [{ role: "user", content: [{ type: "tool_result", text: "peer text" }] }] },
    { messages: [{ role: "user", content: "prefix peer text" }] },
    { system: "peer text" },
  ])
    assert.equal(hasUserInput(payload, "peer text"), false);
});

test("queueing and an earlier provider response never acknowledge an unsubmitted peer input", async () => {
  const accepted: PeerOrigin[] = [];
  const receipts = createPeerInputReceipts(async (input) => {
    accepted.push(input);
  });
  receipts.prepare({ messages: [{ role: "user", content: "initial" }] });
  receipts.add("wire-id", origin, "peer text");
  await receipts.response(200);
  assert.deepEqual(accepted, []);
  receipts.prepare({ messages: [{ role: "user", content: "peer text" }] });
  await receipts.response(500);
  assert.deepEqual(accepted, []);
  receipts.prepare({ messages: [{ role: "user", content: "trimmed" }] });
  await receipts.response(200);
  assert.deepEqual(accepted, []);
  receipts.prepare({ messages: [{ role: "user", content: "peer text" }] });
  await receipts.response(200);
  assert.deepEqual(accepted, [origin]);
  await receipts.accepted("wire-id");
  assert.deepEqual(accepted, [origin]);
});

test("failed durable receipt writes remain retryable", async () => {
  let attempts = 0;
  const receipts = createPeerInputReceipts(async () => {
    if (++attempts === 1) throw new Error("store unavailable");
  });
  receipts.add("wire-id", origin, "peer text");
  await assert.rejects(receipts.accepted("wire-id"), /store unavailable/);
  await receipts.accepted("wire-id");
  await receipts.accepted("wire-id");
  assert.equal(attempts, 2);
});
