# `@substate/client`

Minimal TypeScript client for SubState:

- WebSocket `/v1/sync` — subscribe, resume, ack, unsubscribe
- HTTP `GET /v1/cds` — current merged state
- HTTP `POST /v1/ingest`

```bash
cd clients/typescript && npm install && npm run build
```

Replace `<entity>`, `<http_source>`, and field names with the values from your
`schema.yaml`.

```ts
import { cds, connect, ingest } from "@substate/client";

const opts = { apiKey: process.env.SUBSTATE_API_KEY }; // optional

const snapshot = await cds("http://127.0.0.1:8080", "", opts);
const row = await cds("http://127.0.0.1:8080", "<entity>/1", opts);

const client = await connect("ws://127.0.0.1:8080/v1/sync", opts);
client.subscribe("<entity>", { "<field>": "value" });
const subscribed = await client.waitFor((m) => m.type === "subscribed");

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

When the server sets `SUBSTATE_API_KEY`, pass the same value via `apiKey` (sent as
`Authorization: Bearer` and `x-api-key`). `/health` stays open without a key.
