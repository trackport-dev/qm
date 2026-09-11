import "../../src/shell.css";
import { renderBoardPage } from "../../src/board";
import { appState } from "../../src/shell-state";
import type { api } from "../../src/core-bridge";

const candidates = [
  { id: "planner", name: "Planner", version: 2, character: { group: "new-feature", role: "coordinator" } },
  { id: "worker", name: "Builder", version: 3, character: { group: "new-feature", role: "worker" } },
  { id: "reviewer", name: "Reviewer", version: 1, character: { group: "quality", role: "reviewer" } },
];
const now = Date.now();
const messages = [
  {
    id: "request",
    sequence: 1,
    senderId: "planner",
    senderName: "Planner",
    text: "Implement the upload API. Keep validation and authorization in the shared service; send the test results back here.",
    audience: '.[] | select(.group == "new-feature" and .role == "worker")',
    candidates,
    recipientIds: ["worker"],
    replyTo: null,
    threadId: "request",
    createdAt: now - 120_000,
  },
  {
    id: "reply",
    sequence: 2,
    senderId: "worker",
    senderName: "Builder",
    text: "API complete. All 18 request specs pass. The expired-token test exposed a retry bug; the fix and regression test are included.",
    audience: '.[] | select(._qm.id == "planner")',
    candidates,
    recipientIds: ["planner"],
    replyTo: "request",
    threadId: "request",
    createdAt: now - 60_000,
  },
  {
    id: "observation",
    sequence: 3,
    senderId: "reviewer",
    senderName: "Reviewer",
    text: "Cross-project observation: the same retry issue can affect background imports. I will review that call site next.",
    audience: "empty",
    candidates,
    recipientIds: [],
    replyTo: "reply",
    threadId: "request",
    createdAt: now - 30_000,
  },
];
const continuation = {
  ...messages[0]!,
  id: "continuation",
  sequence: 4,
  replyTo: null,
  threadId: "continuation",
  recipientIds: ["planner"],
  createdAt: now - 20_000,
  continuation: { kind: "reply_wait", waitId: "completed-wait" },
  text: "The selected recipient replied. Resume the suspended work using the linked public reply.",
};
const timeline = [...messages, continuation];
export const request: typeof api = async <T>(path: string): Promise<T> => {
  const url = new URL(path, location.origin);
  const waits = [
    {
      id: "completed-wait",
      sessionId: "planner",
      messageId: "request",
      mode: "first",
      recipientIds: ["worker"],
      replyIds: ["reply"],
      state: "resumed",
      outcome: "ready",
      createdAt: now - 115_000,
      deadline: now + 180_000,
      suspendedAt: now - 110_000,
      resumeMessageId: "continuation",
    },
    {
      id: "timeout-wait",
      sessionId: "planner",
      messageId: "request",
      mode: "all",
      recipientIds: ["worker"],
      replyIds: [],
      state: "timed_out",
      outcome: "timed_out",
      createdAt: now - 110_000,
      deadline: now - 65_000,
      suspendedAt: now - 100_000,
      resumeMessageId: null,
    },
  ];
  if (url.pathname === "/api/peer-waits") {
    const rows = waits.filter((wait) => wait.messageId === url.searchParams.get("messageId"));
    return { waits: rows, nextCursor: rows.at(-1)?.id ?? "", hasMore: false } as T;
  }
  if (url.pathname.startsWith("/api/peer-waits/"))
    return { wait: waits.find((wait) => wait.id === url.pathname.split("/").at(-1)) } as T;
  if (url.pathname.endsWith("/preview")) return { candidates, recipientIds: ["worker"] } as T;
  if (url.pathname === "/api/peer-messages") {
    const rows = timeline.filter(
      (message) =>
        message.sequence > Number(url.searchParams.get("after") ?? 0) &&
        (!url.searchParams.get("text") ||
          message.text.toLowerCase().includes(url.searchParams.get("text")!.toLowerCase())) &&
        (!url.searchParams.get("threadId") || message.threadId === url.searchParams.get("threadId")),
    );
    return {
      messages: rows,
      nextCursor: rows.at(-1)?.sequence ?? Number(url.searchParams.get("after") ?? 0),
      hasMore: false,
    } as T;
  }
  const message = timeline.find((item) => item.id === url.pathname.split("/").at(-1));
  if (!message) throw new Error("fixture message not found");
  return {
    message,
    deliveries: message.recipientIds.map((recipientId) => ({
      id: `${message.id}:${recipientId}`,
      recipientId,
      state: message.id === "request" || message.id === "continuation" ? "consumed" : "blocked",
      attempts: 2,
      reason: message.id === "request" || message.id === "continuation" ? null : "recipient_awaiting_approval",
      createdAt: message.createdAt,
      updatedAt: now - 10_000,
    })),
  } as T;
};
appState.currentView = "board";
appState.mainEl = document.querySelector<HTMLElement>("#board-demo");
document.body.style.overflow = "auto";
await renderBoardPage(request);
