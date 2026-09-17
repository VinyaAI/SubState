# Dispatcher Map (reference UI)

Minimal React demo that subscribes to SubState over WebSocket and plots
entities with `location` / `lat`+`lng` on a simple map.

```bash
# terminal 1 — SubState
cargo run -p substate-cli -- serve

# terminal 2 — UI
cd examples/dispatcher-map
npm install
npm run dev
```

Open the Vite URL (default http://127.0.0.1:5173), set the WebSocket URL to
`ws://127.0.0.1:8080/v1/sync`, and click **Connect & subscribe**.

Note: browsers cannot send `Authorization` headers on WebSocket; if you use
`SUBSTATE_API_KEY`, prefer the TypeScript Node client, or temporarily leave
the key unset for local demos.
