/** Wire types for SubState `/v1/sync` and `/v1/ingest`. */

import WebSocket from "ws";

export type DeltaOp = "add" | "update" | "remove";

export type ServerMessage =
  | { type: "subscribed"; subscription: string }
  | {
      type: "snapshot";
      subscription: string;
      entities: Array<{ id: string; fields: Record<string, unknown> }>;
    }
  | {
      type: "delta";
      subscription: string;
      seq: number;
      op: DeltaOp;
      entity: string;
      id: string;
      fields?: Record<string, unknown>;
    }
  | { type: "reset"; subscription: string; reason: string }
  | { type: "error"; message: string };

export type MessageHandler = (message: ServerMessage) => void;

export type IngestBody = {
  source: string;
  entity_type: string;
  id: string;
  fields?: Record<string, unknown>;
  versions?: Record<string, number>;
};

export type IngestResult = {
  accepted: boolean;
  changed_fields?: string[];
  error?: string;
};

export type ClientOptions = {
  /** Shared secret for `/v1/*` when the server has `SUBSTATE_API_KEY` set. */
  apiKey?: string;
  /** Auto-reconnect with resume after disconnect (default true). */
  autoReconnect?: boolean;
  /** Delay before reconnect attempt in ms (default 1000). */
  reconnectDelayMs?: number;
};

function authHeaders(apiKey?: string): Record<string, string> {
  if (!apiKey) {
    return {};
  }
  return {
    Authorization: `Bearer ${apiKey}`,
    "x-api-key": apiKey,
  };
}

type TrackedSub = {
  entityType: string;
  where: Record<string, unknown>;
  lastSeq: number;
};

export class SubStateClient {
  readonly url: string;
  readonly apiKey?: string;
  private ws: WebSocket | null = null;
  private handlers = new Set<MessageHandler>();
  private openPromise: Promise<void> | null = null;
  private autoReconnect: boolean;
  private reconnectDelayMs: number;
  private intentionalClose = false;
  private tracked = new Map<string, TrackedSub>();
  private pendingByFilter = new Map<string, TrackedSub>();

  constructor(url: string, options: ClientOptions = {}) {
    this.url = url;
    this.apiKey = options.apiKey;
    this.autoReconnect = options.autoReconnect !== false;
    this.reconnectDelayMs = options.reconnectDelayMs ?? 1000;
  }

  onMessage(handler: MessageHandler): () => void {
    this.handlers.add(handler);
    return () => {
      this.handlers.delete(handler);
    };
  }

  async connect(): Promise<void> {
    this.intentionalClose = false;
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      return;
    }
    if (this.openPromise) {
      return this.openPromise;
    }

    this.openPromise = new Promise<void>((resolve, reject) => {
      const headers = authHeaders(this.apiKey);
      const ws = new WebSocket(this.url, {
        headers: Object.keys(headers).length ? headers : undefined,
      });
      this.ws = ws;

      ws.once("open", () => resolve());
      ws.once("error", (err) => {
        reject(err instanceof Error ? err : new Error(String(err)));
      });
      ws.on("message", (data) => {
        const text = typeof data === "string" ? data : data.toString("utf8");
        let message: ServerMessage;
        try {
          message = JSON.parse(text) as ServerMessage;
        } catch {
          return;
        }
        this.noteServerMessage(message);
        for (const handler of this.handlers) {
          handler(message);
        }
      });
      ws.on("close", () => {
        this.ws = null;
        this.openPromise = null;
        if (!this.intentionalClose && this.autoReconnect) {
          setTimeout(() => {
            void this.reconnectAndResume();
          }, this.reconnectDelayMs);
        }
      });
    });

    return this.openPromise;
  }

  private async reconnectAndResume(): Promise<void> {
    try {
      await this.connect();
      for (const [subId, tracked] of this.tracked) {
        if (tracked.lastSeq > 0) {
          this.resume(subId, tracked.lastSeq);
        } else {
          this.subscribe(tracked.entityType, tracked.where);
        }
      }
      for (const tracked of this.pendingByFilter.values()) {
        this.subscribe(tracked.entityType, tracked.where);
      }
    } catch {
      if (!this.intentionalClose && this.autoReconnect) {
        setTimeout(() => {
          void this.reconnectAndResume();
        }, this.reconnectDelayMs);
      }
    }
  }

  private noteServerMessage(message: ServerMessage): void {
    if (message.type === "subscribed") {
      // Match the most recent pending subscribe for this connection.
      const pending = [...this.pendingByFilter.values()].pop();
      if (pending) {
        this.tracked.set(message.subscription, {
          ...pending,
          lastSeq: 0,
        });
      }
    }
    if (message.type === "delta") {
      const tracked = this.tracked.get(message.subscription);
      if (tracked) {
        tracked.lastSeq = Math.max(tracked.lastSeq, message.seq);
      }
    }
    if (message.type === "snapshot") {
      const tracked = this.tracked.get(message.subscription);
      if (tracked && tracked.lastSeq === 0) {
        // Keep lastSeq; snapshot has no seq.
      }
    }
    if (message.type === "reset") {
      const tracked = this.tracked.get(message.subscription);
      if (tracked) {
        tracked.lastSeq = 0;
      }
    }
  }

  close(): void {
    this.intentionalClose = true;
    this.ws?.close();
    this.ws = null;
    this.openPromise = null;
  }

  private send(payload: unknown): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      throw new Error("WebSocket is not connected");
    }
    this.ws.send(JSON.stringify(payload));
  }

  subscribe(entityType: string, where: Record<string, unknown> = {}): void {
    const key = `${entityType}:${JSON.stringify(where)}`;
    this.pendingByFilter.set(key, { entityType, where, lastSeq: 0 });
    this.send({
      type: "subscribe",
      entity_type: entityType,
      where,
    });
  }

  /**
   * Subscribe with an explicit filter document.
   * Supports equality, `{ gt|gte|lt|lte|ne }`, `$or`, and `$and`.
   */
  subscribeWhere(entityType: string, where: Record<string, unknown>): void {
    this.subscribe(entityType, where);
  }

  resume(subscription: string, resumeAfter: number): void {
    this.send({
      type: "resume",
      subscription,
      resume_after: resumeAfter,
    });
  }

  unsubscribe(subscription: string): void {
    this.tracked.delete(subscription);
    this.send({ type: "unsubscribe", subscription });
  }

  ack(subscription: string, seq: number): void {
    this.send({ type: "ack", subscription, seq });
  }

  /** Highest applied seq per subscription (for resume helpers). */
  lastSeq(subscription: string): number {
    return this.tracked.get(subscription)?.lastSeq ?? 0;
  }

  /** Wait for the next server message matching `predicate`, with timeout. */
  waitFor(
    predicate: (message: ServerMessage) => boolean,
    timeoutMs = 10_000,
  ): Promise<ServerMessage> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        off();
        reject(new Error(`Timed out after ${timeoutMs}ms waiting for message`));
      }, timeoutMs);

      const off = this.onMessage((message) => {
        if (!predicate(message)) {
          return;
        }
        clearTimeout(timer);
        off();
        resolve(message);
      });
    });
  }
}

/** Convenience: connect to a sync WebSocket URL. */
export async function connect(
  url: string,
  options: ClientOptions = {},
): Promise<SubStateClient> {
  const client = new SubStateClient(url, options);
  await client.connect();
  return client;
}

/** HTTP dump of the current CDS (merged snapshot). */
export async function cds(
  baseUrl: string,
  path: string = "",
  options: ClientOptions = {},
): Promise<unknown> {
  const suffix = path ? `/v1/cds/${path.replace(/^\//, "")}` : "/v1/cds";
  const url = `${baseUrl.replace(/\/$/, "")}${suffix}`;
  const response = await fetch(url, {
    headers: authHeaders(options.apiKey),
  });
  return response.json();
}

/** HTTP ingest against a SubState base URL (e.g. http://127.0.0.1:8080). */
export async function ingest(
  baseUrl: string,
  body: IngestBody,
  options: ClientOptions = {},
): Promise<IngestResult> {
  const url = `${baseUrl.replace(/\/$/, "")}/v1/ingest`;
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...authHeaders(options.apiKey),
    },
    body: JSON.stringify(body),
  });
  const json = (await response.json()) as IngestResult;
  if (!response.ok && !json.error) {
    return { accepted: false, error: `HTTP ${response.status}` };
  }
  return json;
}
