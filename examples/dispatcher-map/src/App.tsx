import { useEffect, useMemo, useRef, useState } from "react";

type Entity = {
  id: string;
  fields: Record<string, unknown>;
};

type ServerMessage =
  | { type: "subscribed"; subscription: string }
  | { type: "snapshot"; subscription: string; entities: Entity[] }
  | {
      type: "delta";
      subscription: string;
      seq: number;
      op: "add" | "update" | "remove";
      entity: string;
      id: string;
      fields?: Record<string, unknown>;
    }
  | { type: "reset"; subscription: string; reason: string }
  | { type: "error"; message: string };

function locationOf(fields: Record<string, unknown>): { lat: number; lng: number } | null {
  const loc = fields.location;
  if (loc && typeof loc === "object") {
    const obj = loc as Record<string, unknown>;
    const lat = Number(obj.lat);
    const lng = Number(obj.lng ?? obj.lon);
    if (Number.isFinite(lat) && Number.isFinite(lng)) {
      return { lat, lng };
    }
  }
  const lat = Number(fields.lat);
  const lng = Number(fields.lng ?? fields.lon);
  if (Number.isFinite(lat) && Number.isFinite(lng)) {
    return { lat, lng };
  }
  return null;
}

function project(
  lat: number,
  lng: number,
  bounds: { minLat: number; maxLat: number; minLng: number; maxLng: number },
  size: { w: number; h: number },
): { x: number; y: number } {
  const x =
    ((lng - bounds.minLng) / Math.max(bounds.maxLng - bounds.minLng, 1e-6)) * size.w;
  const y =
    (1 - (lat - bounds.minLat) / Math.max(bounds.maxLat - bounds.minLat, 1e-6)) * size.h;
  return { x, y };
}

export function App() {
  const [baseUrl, setBaseUrl] = useState("ws://127.0.0.1:8080/v1/sync");
  const [apiKey, setApiKey] = useState("");
  const [entityType, setEntityType] = useState("driver");
  const [region, setRegion] = useState("nashville");
  const [status, setStatus] = useState("disconnected");
  const [entities, setEntities] = useState<Record<string, Entity>>({});
  const wsRef = useRef<WebSocket | null>(null);
  const lastSeq = useRef(0);
  const subId = useRef<string | null>(null);

  const bounds = useMemo(
    () => ({ minLat: 36.0, maxLat: 36.3, minLng: -87.0, maxLng: -86.6 }),
    [],
  );

  useEffect(() => {
    return () => {
      wsRef.current?.close();
    };
  }, []);

  function connect() {
    wsRef.current?.close();
    setEntities({});
    lastSeq.current = 0;
    subId.current = null;
    setStatus("connecting");

    const headers: Record<string, string> = {};
    // Browser WebSocket cannot set custom headers; pass key as query when needed.
    const url = apiKey
      ? `${baseUrl}${baseUrl.includes("?") ? "&" : "?"}api_key=${encodeURIComponent(apiKey)}`
      : baseUrl;

    const ws = new WebSocket(url);
    wsRef.current = ws;

    ws.onopen = () => {
      setStatus("connected");
      const where: Record<string, unknown> = {};
      if (region) where.region = region;
      ws.send(
        JSON.stringify({
          type: "subscribe",
          entity_type: entityType,
          where,
        }),
      );
    };

    ws.onmessage = (event) => {
      const message = JSON.parse(String(event.data)) as ServerMessage;
      if (message.type === "subscribed") {
        subId.current = message.subscription;
        setStatus(`subscribed ${message.subscription}`);
      }
      if (message.type === "snapshot") {
        const next: Record<string, Entity> = {};
        for (const entity of message.entities) {
          next[entity.id] = entity;
        }
        setEntities(next);
      }
      if (message.type === "delta") {
        lastSeq.current = Math.max(lastSeq.current, message.seq);
        setEntities((prev) => {
          const next = { ...prev };
          if (message.op === "remove") {
            delete next[message.id];
          } else if (message.op === "add") {
            next[message.id] = {
              id: message.id,
              fields: message.fields ?? {},
            };
          } else {
            const existing = next[message.id] ?? { id: message.id, fields: {} };
            next[message.id] = {
              id: message.id,
              fields: { ...existing.fields, ...(message.fields ?? {}) },
            };
          }
          return next;
        });
        if (subId.current) {
          ws.send(
            JSON.stringify({
              type: "ack",
              subscription: subId.current,
              seq: message.seq,
            }),
          );
        }
      }
      if (message.type === "reset") {
        setStatus(`reset: ${message.reason}`);
      }
      if (message.type === "error") {
        setStatus(`error: ${message.message}`);
      }
    };

    ws.onclose = () => setStatus("disconnected");
    ws.onerror = () => setStatus("error");
  }

  const size = { w: 900, h: 700 };
  const dots = Object.values(entities)
    .map((entity) => {
      const loc = locationOf(entity.fields);
      if (!loc) return null;
      const { x, y } = project(loc.lat, loc.lng, bounds, size);
      const busy = String(entity.fields.status ?? "") === "busy";
      return { entity, x, y, busy };
    })
    .filter(Boolean) as Array<{
    entity: Entity;
    x: number;
    y: number;
    busy: boolean;
  }>;

  return (
    <div className="app">
      <aside className="panel">
        <h1>Dispatcher Map</h1>
        <p style={{ margin: 0, fontSize: "0.8rem", color: "#9bb0d0" }}>
          Reference UI for SubState live subscriptions.
        </p>
        <label>
          WebSocket URL
          <input value={baseUrl} onChange={(e) => setBaseUrl(e.target.value)} />
        </label>
        <label>
          API key (optional query)
          <input value={apiKey} onChange={(e) => setApiKey(e.target.value)} />
        </label>
        <label>
          Entity type
          <input value={entityType} onChange={(e) => setEntityType(e.target.value)} />
        </label>
        <label>
          Region filter
          <input value={region} onChange={(e) => setRegion(e.target.value)} />
        </label>
        <button type="button" onClick={connect}>
          Connect &amp; subscribe
        </button>
        <div className="status">{status}</div>
        <div className="list">
          {Object.values(entities).map((entity) => (
            <div key={entity.id}>
              <strong>{entity.id}</strong>{" "}
              {String(entity.fields.status ?? "")}{" "}
              {String(entity.fields.name ?? "")}
            </div>
          ))}
        </div>
      </aside>
      <main className="map" style={{ width: size.w, height: size.h }}>
        {dots.map(({ entity, x, y, busy }) => (
          <div
            key={entity.id}
            className={busy ? "dot busy" : "dot"}
            style={{ left: x, top: y }}
            title={entity.id}
          >
            <span className="tooltip">{entity.id}</span>
          </div>
        ))}
      </main>
    </div>
  );
}
