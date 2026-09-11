import type { Conversation, Principal, ScopeId } from "../types.ts";

export type Character = { [key: string]: JsonValue };
export type JsonValue = null | boolean | number | string | JsonValue[] | Character;

export interface PeerAuthority {
  actor: Principal;
  conversation: Conversation;
  surface: string;
  scopeVersion?: string;
}

export interface Peer {
  id: string;
  name: string;
  character: Character;
  version: number;
  scopeId: ScopeId;
  parentId: string | null;
  rootId: string;
  ancestors: string[];
  descendantLimit: number;
  state: "active" | "paused" | "stopped" | "archived" | "deleted";
  authority: PeerAuthority | null;
  sandboxId: string | null;
  createdAt: number;
  updatedAt: number;
}

export type PublicPeer = Pick<
  Peer,
  | "id"
  | "name"
  | "character"
  | "version"
  | "parentId"
  | "rootId"
  | "descendantLimit"
  | "state"
  | "createdAt"
  | "updatedAt"
>;

export interface AudienceCandidate {
  id: string;
  version: number;
  name: string;
  character: Character;
}

export interface PeerMessage {
  id: string;
  senderId: string;
  senderRunId: string;
  senderName: string;
  text: string;
  audience: string;
  candidates: AudienceCandidate[];
  recipientIds: string[];
  replyTo: string | null;
  threadId: string;
  createdAt: number;
  sequence: number;
}

export interface PeerDelivery {
  id: string;
  messageId: string;
  recipientId: string;
  state: "queued" | "delivered" | "consumed" | "blocked" | "failed";
  attempts: number;
  runId: string | null;
  reason: string | null;
  createdAt: number;
  updatedAt: number;
  leaseToken: string | null;
  leaseUntil: number;
}

export interface PeerSpawn {
  id: string;
  parentId: string;
  childId: string;
  rootId: string;
  task: string;
  state: "reserved" | "provisioning" | "ready" | "failed";
  sandboxId: string | null;
  runId: string | null;
  reason: string | null;
  createdAt: number;
  updatedAt: number;
}

export interface PeerWait {
  id: string;
  sessionId: string;
  runId: string;
  messageId: string;
  mode: "first" | "all";
  recipientIds: string[];
  replyIds: string[];
  state: "waiting" | "ready" | "timed_out" | "cancelled" | "resumed";
  deadline: number;
  createdAt: number;
  resumeRunId: string | null;
}

export interface CoordinationRows {
  peer: Peer;
  message: PeerMessage;
  delivery: PeerDelivery;
  spawn: PeerSpawn;
  wait: PeerWait;
}

export type CoordinationKind = keyof CoordinationRows;

export interface CoordinationEvent {
  sequence: number;
  kind: CoordinationKind;
  id: string;
  at: number;
}

export class CoordinationError extends Error {
  readonly status: number;
  readonly code: string;
  readonly details: Record<string, unknown>;

  constructor(status: number, code: string, message: string, details: Record<string, unknown> = {}) {
    super(message);
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

export function publicPeer(peer: Peer): PublicPeer {
  const { id, name, character, version, parentId, rootId, descendantLimit, state, createdAt, updatedAt } = peer;
  return structuredClone({
    id,
    name,
    character,
    version,
    parentId,
    rootId,
    descendantLimit,
    state,
    createdAt,
    updatedAt,
  });
}
