# `@substate/client`

TypeScript client for SubState (`0.1.0-alpha` — `/v1` may break):

- WebSocket `/v1/sync` — subscribe, resume, ack, unsubscribe
- Optional auto-reconnect with `resume_after`
- HTTP `GET /v1/cds` — current merged state
- HTTP `POST /v1/ingest`
- `apiKey` on `connect`, `cds`, and `ingest`

```bash
cd clients/typescript && npm install && npm run build
```

```ts
import { cds, connect, ingest } from "@substate/client";

const opts = {
  apiKey: process.env.SUBSTATE_API_KEY, // optional
  autoReconnect: true,
};

const snapshot = await cds("http://127.0.0.1:8080", "", opts);
const client = await connect("ws://127.0.0.1:8080/v1/sync", opts);

client.subscribe("<entity>", { "<field>": "value" });
// or richer filters:
client.subscribeWhere("<entity>", {
  $or: [{ status: "available" }, { status: { eq: "busy" } }],
  updated_at: { gt: "2026-01-01T00:00:00Z" },
});

await client.waitFor((m) => m.type === "subscribed");

await ingest(
  "http://127.0.0.1:8080",
  {
    source: "<http_source>",
    entity_type: "<entity>",
    id: "1",
    fields: { "<live_field>": { lat: 36.16, lng: -86.78 } },
    versions: { "<live_field>": 1 },
  },
  opts,
);
```

When the server sets `SUBSTATE_API_KEY`, pass the same value via `apiKey`
(`Authorization: Bearer` and `x-api-key`). `/health` stays open without a key.

Protocol details: [docs/api.md](../../docs/api.md).
