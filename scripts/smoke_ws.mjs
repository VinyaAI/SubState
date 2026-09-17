#!/usr/bin/env node
/**
 * WebSocket helper for scripts/smoke.sh.
 *
 * Env:
 *   SMOKE_WS_URL       ws://127.0.0.1:18080/v1/sync
 *   SMOKE_API_KEY      shared secret (optional)
 *   SMOKE_EXPECT       "delta" (default) — wait for subscribed + snapshot + delta
 *   SMOKE_TIMEOUT_MS   overall timeout (default 30000)
 *   SMOKE_READY_FILE   touched after subscribed + snapshot (smoke.sh waits before ingest)
 *   SMOKE_WS_MODULE    optional absolute path to ws package
 */
import { createRequire } from "node:module";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");
const require = createRequire(import.meta.url);

function loadWs() {
  const override = process.env.SMOKE_WS_MODULE;
  if (override) {
    return require(override);
  }
  try {
    return require(path.join(root, "clients/typescript/node_modules/ws"));
  } catch {
    return require("ws");
  }
}

const WebSocket = loadWs();

const url = process.env.SMOKE_WS_URL || "ws://127.0.0.1:18080/v1/sync";
const apiKey = process.env.SMOKE_API_KEY || "";
const expect = process.env.SMOKE_EXPECT || "delta";
const timeoutMs = Number(process.env.SMOKE_TIMEOUT_MS || "30000");
const readyFile = process.env.SMOKE_READY_FILE || "";

const headers = {};
if (apiKey) {
  headers.Authorization = `Bearer ${apiKey}`;
  headers["x-api-key"] = apiKey;
}

let gotSubscribed = false;
let gotSnapshot = false;
let gotDelta = false;
let readyMarked = false;
let settled = false;

function markReady() {
  if (readyMarked || !gotSubscribed || !gotSnapshot) return;
  readyMarked = true;
  if (readyFile) {
    fs.writeFileSync(readyFile, "ready\n");
  }
}

function fail(message) {
  if (settled) return;
  settled = true;
  clearTimeout(timer);
  console.error(`smoke_ws: ${message}`);
  try {
    ws.close();
  } catch {
    /* ignore */
  }
  process.exit(1);
}

function succeed(message) {
  if (settled) return;
  settled = true;
  clearTimeout(timer);
  console.log(`smoke_ws: ${message}`);
  try {
    ws.close();
  } catch {
    /* ignore */
  }
  process.exit(0);
}

const timer = setTimeout(() => {
  fail(
    `timeout after ${timeoutMs}ms (subscribed=${gotSubscribed} snapshot=${gotSnapshot} delta=${gotDelta})`,
  );
}, timeoutMs);

const ws = new WebSocket(url, { headers });

ws.on("open", () => {
  ws.send(
    JSON.stringify({
      type: "subscribe",
      entity_type: "driver",
      where: { status: "available" },
    }),
  );
});

ws.on("message", (data) => {
  let msg;
  try {
    msg = JSON.parse(String(data));
  } catch (err) {
    fail(`non-JSON message: ${data} (${err})`);
    return;
  }

  if (msg.type === "error") {
    fail(`server error: ${msg.message || JSON.stringify(msg)}`);
    return;
  }
  if (msg.type === "subscribed") {
    gotSubscribed = true;
    markReady();
    return;
  }
  if (msg.type === "snapshot") {
    gotSnapshot = true;
    markReady();
    if (expect === "snapshot" && gotSubscribed) {
      succeed("got subscribed + snapshot");
    }
    return;
  }
  if (msg.type === "delta") {
    gotDelta = true;
    if (expect === "delta" && gotSubscribed && gotSnapshot) {
      succeed(`got delta op=${msg.op} id=${msg.id}`);
    }
  }
});

ws.on("unexpected-response", (_req, res) => {
  let body = "";
  res.on("data", (chunk) => {
    body += chunk;
  });
  res.on("end", () => {
    fail(`unexpected HTTP ${res.statusCode} during WS upgrade: ${body}`);
  });
});

ws.on("error", (err) => {
  fail(`socket error: ${err.message || err}`);
});

ws.on("close", () => {
  if (!settled) {
    fail(
      `socket closed early (subscribed=${gotSubscribed} snapshot=${gotSnapshot} delta=${gotDelta})`,
    );
  }
});
