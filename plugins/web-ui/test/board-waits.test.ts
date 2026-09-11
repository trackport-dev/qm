import assert from "node:assert/strict";
import test from "node:test";
import { JSDOM } from "jsdom";
import { createServer } from "vite";
import type { api } from "../src/core-bridge.ts";

test("board preserves wait pagination, selection ordering, and timeline cursors across refresh and reconnect", async () => {
  const dom = new JSDOM('<!doctype html><main id="board-demo"></main>', {
    url: "http://localhost/test/support/board-demo.html",
  });
  const descriptors = new Map<string, PropertyDescriptor | undefined>();
  for (const key of [
    "window",
    "document",
    "location",
    "history",
    "localStorage",
    "navigator",
    "HTMLElement",
    "Element",
    "Node",
    "Event",
    "MouseEvent",
    "FormData",
    "customElements",
  ]) {
    descriptors.set(key, Object.getOwnPropertyDescriptor(globalThis, key));
    Object.defineProperty(globalThis, key, {
      configurable: true,
      writable: true,
      value: key === "window" ? dom.window : Reflect.get(dom.window, key),
    });
  }
  const vite = await createServer({ server: { middlewareMode: true, hmr: false }, appType: "custom" });
  try {
    await vite.ssrLoadModule("/test/support/board-demo.ts");
    const body = dom.window.document.body;
    assert.equal(dom.window.location.pathname, "/board/request");
    assert.doesNotMatch(body.textContent!, /fixture message not found/);
    assert.match(body.textContent!, /Reply wait · resumed/);
    assert.match(body.textContent!, /Outcome: ready/);
    assert.match(body.textContent!, /Reply wait · timed out/);
    assert.match(body.textContent!, /One reply from every selected recipient/);
    assert.match(body.textContent!, /System-generated reply-wait continuation/);
    assert.match(body.textContent!, /Continuation\s+consumed by the recipient/);
    const link = [...body.querySelectorAll<HTMLAnchorElement>("a")].find(
      (node) => node.pathname === "/board/continuation",
    );
    assert.ok(link);
    link.dispatchEvent(new dom.window.MouseEvent("click", { bubbles: true, cancelable: true, button: 0 }));
    for (let i = 0; i < 20 && !body.textContent?.includes("not an agent-authored reply"); i++)
      await new Promise((resolve) => setImmediate(resolve));
    assert.match(body.textContent!, /not an agent-authored reply/);
    assert.ok(body.querySelector('a[href="#wait-completed-wait"]'));
    assert.ok(body.querySelector("#wait-completed-wait"));
    assert.equal(dom.window.document.activeElement?.id, "board-inspector-heading");

    const { request } = (await vite.ssrLoadModule("/test/support/board-demo.ts")) as { request: typeof api };
    const { renderBoardPage } = await vite.ssrLoadModule("/src/board.ts");
    const delayed = Promise.withResolvers<void>();
    let requested = false;
    const intercepted: typeof api = async <T>(path: string, options?: RequestInit): Promise<T> => {
      if (path === "/api/peer-messages/request") {
        requested = true;
        await delayed.promise;
      }
      return request<T>(path, options);
    };
    dom.window.history.replaceState(null, "", "/board/reply");
    await renderBoardPage(intercepted);
    const select = (id: string) => {
      const anchor = [...body.querySelectorAll<HTMLAnchorElement>("a")].find(
        (node) => node.pathname === `/board/${id}`,
      );
      assert.ok(anchor);
      anchor.dispatchEvent(new dom.window.MouseEvent("click", { bubbles: true, cancelable: true, button: 0 }));
    };
    select("request");
    assert.equal(requested, true);
    select("continuation");
    for (let i = 0; i < 20 && !body.querySelector("#wait-completed-wait"); i++)
      await new Promise((resolve) => setImmediate(resolve));
    assert.ok(body.querySelector("#wait-completed-wait"));
    const searchInput = body.querySelector<HTMLInputElement>('input[type="search"]');
    assert.ok(searchInput);
    searchInput.focus();
    delayed.resolve();
    for (let i = 0; i < 20; i++) await new Promise((resolve) => setImmediate(resolve));
    assert.equal(dom.window.location.pathname, "/board/continuation");
    assert.match(body.textContent!, /not an agent-authored reply/);
    assert.ok(body.querySelector("#wait-completed-wait"));
    assert.equal(body.querySelector("#wait-timeout-wait"), null);
    assert.equal(dom.window.document.activeElement, searchInput);

    const sameIdDelayed = Promise.withResolvers<void>();
    let sameIdRequested = false;
    const sameIdIntercepted: typeof api = async <T>(path: string, options?: RequestInit): Promise<T> => {
      if (path === "/api/peer-messages/request" && !sameIdRequested) {
        sameIdRequested = true;
        await sameIdDelayed.promise;
      }
      return request<T>(path, options);
    };
    dom.window.history.replaceState(null, "", "/board/reply");
    await renderBoardPage(sameIdIntercepted);
    select("request");
    assert.equal(sameIdRequested, true);
    select("continuation");
    select("request");
    for (let i = 0; i < 20 && !body.querySelector("#wait-timeout-wait"); i++)
      await new Promise((resolve) => setImmediate(resolve));
    assert.ok(body.querySelector("#wait-timeout-wait"));
    assert.equal(dom.window.document.activeElement?.id, "board-inspector-heading");
    const sameIdSearch = body.querySelector<HTMLInputElement>('input[type="search"]');
    assert.ok(sameIdSearch);
    sameIdSearch.focus();
    sameIdDelayed.resolve();
    for (let i = 0; i < 20; i++) await new Promise((resolve) => setImmediate(resolve));
    assert.equal(dom.window.location.pathname, "/board/request");
    assert.equal(dom.window.document.activeElement, sameIdSearch);
    assert.ok(body.querySelector("#wait-timeout-wait"));

    const original = await request<{ waits: Array<Record<string, unknown>> }>("/api/peer-waits?messageId=request");
    const waits = Array.from({ length: 101 }, (_, index) => ({
      ...original.waits[0],
      id: `paged-${String(index + 1).padStart(3, "0")}`,
    }));
    const pageRequests: string[] = [];
    const paginated: typeof api = async <T>(path: string, options?: RequestInit): Promise<T> => {
      const url = new URL(path, dom.window.location.origin);
      if (url.pathname !== "/api/peer-waits") return request<T>(path, options);
      const after = url.searchParams.get("after") ?? "";
      pageRequests.push(after);
      const remaining = waits.filter((wait) => wait.id > after);
      const rows = remaining.slice(0, Number(url.searchParams.get("limit")));
      return { waits: rows, nextCursor: rows.at(-1)?.id ?? after, hasMore: remaining.length > rows.length } as T;
    };
    dom.window.history.replaceState(null, "", "/board/request");
    await renderBoardPage(paginated);
    const cards = () => [...body.querySelectorAll<HTMLElement>('[id^="wait-paged-"]')].map((node) => node.id);
    const more = () => {
      const button = [...body.querySelectorAll<HTMLButtonElement>("button")].find(
        (node) => node.textContent?.trim() === "Load more waits",
      );
      assert.ok(button);
      button.click();
    };
    assert.equal(cards().length, 50);
    more();
    for (let i = 0; i < 20 && cards().length !== 100; i++) await new Promise((resolve) => setImmediate(resolve));
    assert.equal(cards().length, 100);
    assert.deepEqual(pageRequests.slice(-2), ["", "paged-050"]);
    waits.unshift({ ...waits[0]!, id: "paged-000" });
    for (let i = 0; i < 100 && !body.querySelector("#wait-paged-000"); i++)
      await new Promise((resolve) => setTimeout(resolve, 50));
    assert.ok(body.querySelector("#wait-paged-000"));
    assert.equal(cards().length, 100);
    assert.equal(new Set(cards()).size, 100);
    assert.deepEqual(pageRequests.slice(-2), ["", "paged-049"]);
    assert.equal(body.querySelector("#wait-paged-100"), null);
    more();
    for (let i = 0; i < 20 && cards().length !== 102; i++) await new Promise((resolve) => setImmediate(resolve));
    assert.equal(cards().length, 102);
    assert.equal(new Set(cards()).size, 102);
    assert.ok(body.querySelector("#wait-paged-101"));
    assert.equal(
      [...body.querySelectorAll("button")].some((node) => node.textContent?.trim() === "Load more waits"),
      false,
    );

    const timeline = await request<{ messages: Array<{ id: string; sequence: number }> }>("/api/peer-messages");
    const cursors: string[] = [];
    const reconnecting: typeof api = async <T>(path: string, options?: RequestInit): Promise<T> => {
      const url = new URL(path, dom.window.location.origin);
      if (url.pathname !== "/api/peer-messages") return request<T>(path, options);
      cursors.push(url.searchParams.get("after")!);
      if (cursors.length === 2) throw new Error("temporary disconnect");
      const rows = timeline.messages.slice(0, cursors.length === 1 ? 1 : 2);
      return { messages: rows, nextCursor: rows.at(-1)!.sequence, hasMore: false } as T;
    };
    dom.window.history.replaceState(null, "", "/board");
    await renderBoardPage(reconnecting);
    const refresh = () => {
      const button = [...body.querySelectorAll<HTMLButtonElement>("button")].find(
        (node) => node.textContent?.trim() === "Check for new messages",
      );
      assert.ok(button);
      button.click();
    };
    assert.equal(body.querySelectorAll(".board-message").length, 1);
    refresh();
    for (let i = 0; i < 20 && !body.textContent?.includes("temporary disconnect"); i++)
      await new Promise((resolve) => setImmediate(resolve));
    assert.match(body.textContent!, /Connection interrupted: temporary disconnect/);
    assert.match(body.textContent!, /Resumes from durable cursor 1/);
    assert.equal(body.querySelectorAll(".board-message").length, 1);
    for (let i = 0; i < 100 && body.querySelectorAll(".board-message").length !== 2; i++)
      await new Promise((resolve) => setTimeout(resolve, 50));
    assert.deepEqual(cursors, ["0", "1", "1"]);
    assert.equal(body.querySelectorAll(".board-message").length, 2);
    assert.doesNotMatch(body.textContent!, /Connection interrupted/);
    assert.match(body.textContent!, /Resumes from durable cursor 2/);
    const deletedTree: typeof api = async <T>(path: string, options?: RequestInit): Promise<T> => {
      if (path === "/api/peers/root/subtree")
        return {
          manageable: true,
          nodes: [
            { peer: { id: "root", parentId: null, name: "Root", state: "active", character: {} }, count: 1, cap: 1 },
            {
              peer: { id: "deleted", parentId: "root", name: "Deleted worker", state: "deleted", character: {} },
              count: 0,
              cap: 16,
              spawn: { state: "failed", attempts: 1, updatedAt: Date.now(), reason: "spawn_session_unavailable" },
            },
          ],
        } as T;
      return request<T>(path, options);
    };
    await renderBoardPage(deletedTree);
    const agentInput = body.querySelector<HTMLInputElement>('input[name="agent"]')!;
    agentInput.value = "root";
    agentInput.form!.dispatchEvent(new dom.window.Event("submit", { bubbles: true, cancelable: true }));
    for (let i = 0; i < 20 && !body.textContent?.includes("unfinished spawn reservation"); i++)
      await new Promise((resolve) => setImmediate(resolve));
    const deletedNode = [...body.querySelectorAll<HTMLElement>(".board-tree-node")].find(
      (node) => node.querySelector("strong")?.textContent === "Deleted worker",
    );
    assert.ok(deletedNode);
    assert.match(deletedNode.textContent!, /unfinished spawn reservation still occupies an ancestor slot/);
    assert.equal(deletedNode.querySelectorAll("button:disabled").length, 3);
    assert.equal(deletedNode.querySelector("a"), null);
  } finally {
    const { appState } = await vite.ssrLoadModule("/src/shell-state.ts");
    appState.currentView = "chats";
    dom.window.document.body.replaceChildren();
    await vite.close();
    dom.window.close();
    for (const [key, descriptor] of descriptors) {
      if (descriptor) Object.defineProperty(globalThis, key, descriptor);
      else Reflect.deleteProperty(globalThis, key);
    }
  }
});
