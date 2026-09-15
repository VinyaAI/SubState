# `@substate/client`

Minimal TypeScript client for SubState:

- WebSocket `/v1/sync` — subscribe, resume, ack, unsubscribe
- HTTP `POST /v1/ingest`

```bash
cd clients/typescript && npm install && npm run build
```

```ts
import { connect, ingest } from "@substate/client";

const client = await connect("ws://127.0.0.1:8080/v1/sync");
client.subscribe("driver", { status: "available" });
const subscribed = await client.waitFor((m) => m.type === "subscribed");

await ingest("http://127.0.0.1:8080", {
  source: "http",
  entity_type: "driver",
  id: "1",
  fields: { location: { lat: 36.16, lng: -86.78 } },
  versions: { location: 1 },
});
```

See `../../scripts/smoke.mjs` for an end-to-end example against Docker Compose.
