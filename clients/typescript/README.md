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

const snapshot = await cds("http://127.0.0.1:8080");
const row = await cds("http://127.0.0.1:8080", "<entity>/1");

const client = await connect("ws://127.0.0.1:8080/v1/sync");
client.subscribe("<entity>", { "<field>": "value" });
const subscribed = await client.waitFor((m) => m.type === "subscribed");

await ingest("http://127.0.0.1:8080", {
  source: "<http_source>",
  entity_type: "<entity>",
  id: "1",
  fields: { "<live_field>": { lat: 36.16, lng: -86.78 } },
  versions: { "<live_field>": 1 },
});
```
