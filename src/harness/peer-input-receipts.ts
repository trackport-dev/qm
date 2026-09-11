import type { PeerOrigin } from "../types.ts";
import { isObj } from "../util/objects.ts";

export function hasUserInput(payload: unknown, text: string): boolean {
  if (!isObj(payload)) return false;
  const messages = payload.messages ?? payload.input ?? payload.contents;
  if (!Array.isArray(messages)) return false;
  return messages.some((message) => {
    if (!isObj(message) || message.role !== "user") return false;
    const content = message.content ?? message.parts;
    if (typeof content === "string") return content === text;
    return (
      Array.isArray(content) &&
      content.some(
        (part) =>
          isObj(part) &&
          part.text === text &&
          (part.type === undefined || part.type === "text" || part.type === "input_text"),
      )
    );
  });
}

export function createPeerInputReceipts(acknowledge?: (origin: PeerOrigin) => Promise<void>) {
  const pending = new Map<string, { origin: PeerOrigin; text: string }>();
  let submitted: string[] = [];
  async function accepted(key: string): Promise<void> {
    const input = pending.get(key);
    if (!input || !acknowledge) return;
    await acknowledge(input.origin);
    pending.delete(key);
  }
  return {
    add(key: string, origin: PeerOrigin, text: string): void {
      pending.set(key, { origin, text });
    },
    accepted,
    prepare(payload: unknown): void {
      submitted = [...pending].filter(([, input]) => hasUserInput(payload, input.text)).map(([key]) => key);
    },
    async response(status: number): Promise<void> {
      const keys = submitted;
      submitted = [];
      if (status < 200 || status >= 300) return;
      for (const key of keys) await accepted(key);
    },
  };
}

export type PeerInputReceipts = ReturnType<typeof createPeerInputReceipts>;
