import assert from "node:assert/strict";
import test from "node:test";
import { JSDOM } from "jsdom";
import { createServer } from "vite";
import type { api } from "../src/core-bridge.ts";

test("board renders wait outcomes and keeps the latest selection when detail responses arrive out of order", async () => {
  const dom = new JSDOM('<!doctype html><main id="board-demo"></main>', { url: "http://localhost/board/request" });
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
    delayed.resolve();
    for (let i = 0; i < 20; i++) await new Promise((resolve) => setImmediate(resolve));
    assert.equal(dom.window.location.pathname, "/board/continuation");
    assert.match(body.textContent!, /not an agent-authored reply/);
    assert.ok(body.querySelector("#wait-completed-wait"));
    assert.equal(body.querySelector("#wait-timeout-wait"), null);
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
