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

export class SubStateClient {
  readonly url: string;
  private ws: WebSocket | null = null;
  private handlers = new Set<MessageHandler>();
  private openPromise: Promise<void> | null = null;

  constructor(url: string) {
    this.url = url;
  }

  onMessage(handler: MessageHandler): () => void {
    this.handlers.add(handler);
    return () => {
      this.handlers.delete(handler);
    };
  }

  async connect(): Promise<void> {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      return;
    }
    if (this.openPromise) {
      return this.openPromise;
    }

    this.openPromise = new Promise<void>((resolve, reject) => {
      const ws = new WebSocket(this.url);
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
        for (const handler of this.handlers) {
          handler(message);
        }
      });
      ws.on("close", () => {
        this.ws = null;
        this.openPromise = null;
      });
    });

    return this.openPromise;
  }

  close(): void {
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
    this.send({
      type: "subscribe",
      entity_type: entityType,
      where,
    });
  }

  resume(subscription: string, resumeAfter: number): void {
    this.send({
      type: "resume",
      subscription,
      resume_after: resumeAfter,
    });
  }

  unsubscribe(subscription: string): void {
    this.send({ type: "unsubscribe", subscription });
  }

  ack(subscription: string, seq: number): void {
    this.send({ type: "ack", subscription, seq });
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
export async function connect(url: string): Promise<SubStateClient> {
  const client = new SubStateClient(url);
  await client.connect();
  return client;
}

/** HTTP ingest against a SubState base URL (e.g. http://127.0.0.1:8080). */
export async function ingest(
  baseUrl: string,
  body: IngestBody,
): Promise<IngestResult> {
  const url = `${baseUrl.replace(/\/$/, "")}/v1/ingest`;
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const json = (await response.json()) as IngestResult;
  if (!response.ok && !json.error) {
    return { accepted: false, error: `HTTP ${response.status}` };
  }
  return json;
}
