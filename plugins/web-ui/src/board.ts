import { html, nothing, render } from "lit";
import { api, withBase } from "./core-bridge";
import { appState } from "./shell-state";
import { errMessage } from "../../chassis/src/errors";
import { deepLinkPath, parseDeepLink, UI_BASE } from "./deep-link";
import "./board.css";

interface Candidate {
  id: string;
  name: string;
  version: number;
  character: Record<string, unknown>;
}
interface Message {
  id: string;
  senderId: string;
  senderName: string;
  text: string;
  audience: string;
  recipientIds: string[];
  replyTo: string | null;
  threadId: string;
  createdAt: number;
  sequence: number;
}
interface Detail {
  message: Message & { candidates: Candidate[] };
  deliveries: Array<{
    id: string;
    recipientId: string;
    state: string;
    attempts: number;
    createdAt: number;
    updatedAt: number;
    reason?: string | null;
    sessionId?: string;
    runId?: string | null;
  }>;
}
interface Page {
  messages: Message[];
  nextCursor: number;
  hasMore: boolean;
}
interface Preview {
  candidates: Candidate[];
  recipientIds: string[];
}

const filterNames = [
  ["text", "Search messages"],
  ["senderId", "Sender ID"],
  ["recipientId", "Intended recipient ID"],
  ["threadId", "Reply thread ID"],
  ["rootId", "Spawn tree root ID"],
  ["group", "Sender group at publication"],
  ["role", "Sender role at publication"],
] as const;

export async function renderBoardPage(request: typeof api = api): Promise<void> {
  if (!appState.mainEl) return;
  const host = document.createElement("section");
  host.className = "pane agent-board";
  appState.mainEl.replaceChildren(host);
  const messages = new Map<string, Message>();
  let cursor = 0;
  let more = false;
  let busy = false;
  let generation = 0;
  let notice = "";
  let selected = parseDeepLink(UI_BASE, location.pathname, location.search).item;
  let detail: Detail | null = null;
  let expression = '.[] | select(.group == "new-feature" and .role == "worker")';
  let preview: Preview | null = null;
  const filters = new URLSearchParams();
  const active = () => host.isConnected && appState.currentView === "board";
  const json = (value: unknown) => JSON.stringify(value, null, 2);
  const stamp = (at: number) => new Date(at).toLocaleString();
  const permalink = (id: string) => deepLinkPath(UI_BASE, "board", null, null, id);

  async function select(id: string): Promise<void> {
    selected = id;
    detail = null;
    history.replaceState(history.state, "", permalink(id));
    paint();
    await loadDetail();
  }
  async function loadDetail(): Promise<void> {
    const id = selected;
    if (!id) return;
    try {
      const result = await request<Detail>(`/api/peer-messages/${encodeURIComponent(id)}`);
      if (selected === id && active()) detail = result;
    } catch (error) {
      if (selected === id) notice = errMessage(error);
    }
    paint();
  }
  async function refresh(reset = false): Promise<void> {
    if (!active()) return;
    if (reset) {
      generation++;
      messages.clear();
      cursor = 0;
      more = false;
    } else if (busy) return;
    const version = generation;
    busy = true;
    paint();
    const query = new URLSearchParams(filters);
    query.set("after", String(cursor));
    query.set("limit", "50");
    try {
      const page = await request<Page>(`/api/peer-messages?${query}`);
      if (!active() || version !== generation) return;
      for (const message of page.messages) messages.set(message.id, message);
      cursor = page.nextCursor;
      more = page.hasMore;
      notice = "";
    } catch (error) {
      if (version === generation)
        notice = `Connection interrupted: ${errMessage(error)}. Retrying from the last cursor.`;
    } finally {
      if (version === generation) {
        busy = false;
        paint();
      }
    }
  }
  async function evaluate(): Promise<void> {
    const input = expression;
    try {
      const result = await request<Preview>("/api/peer-messages/preview", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ audience: input }),
      });
      if (expression === input) preview = result;
      notice = "";
    } catch (error) {
      notice = errMessage(error);
    }
    paint();
  }
  function paint(): void {
    if (!active()) return;
    render(
      html` <header>
          <h1 class="pane-title">Agent board</h1>
          <p>
            Public within this organization. Intended audiences control notifications, not visibility. Reading never
            wakes agents.
          </p>
        </header>
        <form
          class="board-filters"
          @submit=${(event: SubmitEvent) => {
            event.preventDefault();
            const values = new FormData(event.currentTarget as HTMLFormElement);
            for (const [key] of filterNames) {
              const value = String(values.get(key) ?? "").trim();
              if (value) filters.set(key, value);
              else filters.delete(key);
            }
            void refresh(true);
          }}
        >
          ${filterNames.map(([key, label]) => html`<label>${label}<input name=${key} type=${key === "text" ? "search" : "text"} /></label>`)}
          <button class="btn" type="submit">Apply filters</button>
        </form>
        ${notice ? html`<p role="status">${notice}</p>` : nothing}
        <div class="board-columns">
          <section aria-label="Message timeline" aria-busy=${busy}>
            ${messages.size === 0 ? html`<p>${busy ? "Loading messages…" : "No matching messages."}</p>` : nothing}
            ${[...messages.values()]
              .sort((a, b) => a.sequence - b.sequence)
              .map(
                (message) =>
                  html` <article class="board-message ${selected === message.id ? "selected" : ""}">
                    <a
                      href=${permalink(message.id)}
                      @click=${(event: MouseEvent) => {
                        if (!event.metaKey && !event.ctrlKey && !event.shiftKey && event.button === 0) {
                          event.preventDefault();
                          void select(message.id);
                        }
                      }}
                      >${message.senderName} · ${stamp(message.createdAt)}</a
                    >
                    <p class="board-text">${message.text}</p>
                    <small
                      >${message.recipientIds.length ? `${message.recipientIds.length} intended recipients` : "No recipients — published without notification"}${message.replyTo ? " · Reply" : ""}</small
                    >
                  </article>`,
              )}
            <button class="btn" ?disabled=${busy} @click=${() => void refresh()}>
              ${more ? "Load more" : "Check for new messages"}
            </button>
            <small> Resumes from durable cursor ${cursor}</small>
          </section>
          <aside aria-label="Message inspector">
            ${
              detail
                ? html`
                    <h2>Message inspector</h2>
                    <a href=${permalink(detail.message.id)}>Permalink</a>
                    <p>Sender: ${detail.message.senderName} (${detail.message.senderId})</p>
                    <p class="board-text">${detail.message.text}</p>
                    ${detail.message.replyTo ? html`<button class="btn" @click=${() => void select(detail!.message.replyTo!)}>Inspect parent reply</button>` : nothing}
                    <button
                      class="btn"
                      @click=${() => {
                        filters.set("threadId", detail!.message.threadId);
                        void refresh(true);
                      }}
                    >
                      Show reply thread
                    </button>
                    <h3>Original audience expression</h3>
                    <pre>${detail.message.audience}</pre>
                    <p>Frozen recipients: ${detail.message.recipientIds.join(", ") || "None"}</p>
                    <details>
                      <summary>Historical candidate characters (${detail.message.candidates.length})</summary>
                      <pre>${json(detail.message.candidates)}</pre>
                    </details>
                    <h3>Delivery evidence</h3>
                    <p>Delivery is not a reply. Private transcript access remains separate.</p>
                    ${detail.deliveries.map(
                      (delivery) =>
                        html`<article class="board-delivery">
                          <strong>${delivery.recipientId}</strong>
                          <p>${delivery.state} · ${delivery.attempts} attempts</p>
                          ${delivery.reason ? html`<p>${delivery.reason.replaceAll("_", " ")}</p>` : nothing}
                          ${delivery.sessionId ? html`<a href=${withBase(`/s/${encodeURIComponent(delivery.sessionId)}`)}>Open session activity</a>${delivery.runId ? html`<p>Run: ${delivery.runId}</p>` : nothing}` : html`<p>Private session activity requires separate access.</p>`}
                          <small>Created ${stamp(delivery.createdAt)} · Updated ${stamp(delivery.updatedAt)}</small>
                        </article>`,
                    )}
                  `
                : html`<p>
                    ${selected ? "Loading message…" : "Select a message to inspect its audience and delivery."}
                  </p>`
            }
            <h2>Current audience preview</h2>
            <p>This is a new evaluation. It does not change past recipients or notify anyone.</p>
            <label
              >jq expression<textarea
                rows="4"
                .value=${expression}
                @input=${(event: Event) => {
                  expression = (event.currentTarget as HTMLTextAreaElement).value;
                  preview = null;
                }}
              ></textarea>
            </label>
            <button class="btn" @click=${() => void evaluate()}>Preview current matches</button>
            ${
              preview
                ? html`<p>${preview.recipientIds.length} current matches</p>
                    <pre>${json(preview)}</pre>`
                : nothing
            }
          </aside>
        </div>`,
      host,
    );
  }
  async function poll(): Promise<void> {
    if (!active()) return;
    await Promise.all([refresh(), loadDetail()]);
    if (active()) setTimeout(() => void poll(), 3_000);
  }
  await poll();
}
