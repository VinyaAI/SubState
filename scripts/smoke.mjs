#!/usr/bin/env node
/**
 * End-to-end smoke against a live SubState server.
 * Compose demo: subscribe, Postgres UPDATE (CDC), Kafka location produce.
 * HTTP fallback: POST /v1/ingest when SMOKE_PG_CMD / SMOKE_KAFKA_CMD are unset.
 */
import { exec } from "node:child_process";
import { promisify } from "node:util";
import path from "node:path";
import { fileURLToPath } from "node:url";

const execAsync = promisify(exec);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const clientDist = path.resolve(__dirname, "../clients/typescript/dist/index.js");
const { connect, ingest } = await import(clientDist);

const baseUrl = process.env.SUBSTATE_URL ?? "http://127.0.0.1:8080";
const wsUrl = process.env.SUBSTATE_WS_URL ?? "ws://127.0.0.1:8080/v1/sync";
const pgCmd = process.env.SMOKE_PG_CMD;
const kafkaCmd = process.env.SMOKE_KAFKA_CMD;

const client = await connect(wsUrl);

const subscribedP = client.waitFor((m) => m.type === "subscribed", 15_000);
const snapshotP = client.waitFor((m) => m.type === "snapshot", 15_000);

client.subscribe("driver", { region: "nashville" });

const subscribed = await subscribedP;
const snapshot = await snapshotP;

if (subscribed.type !== "subscribed") {
  throw new Error("expected subscribed");
}
if (snapshot.type !== "snapshot") {
  throw new Error("expected snapshot");
}
if (!Array.isArray(snapshot.entities) || snapshot.entities.length < 1) {
  throw new Error(`expected snapshot entities, got ${JSON.stringify(snapshot)}`);
}

console.log(
  `subscribed ${subscribed.subscription}; snapshot entities=${snapshot.entities.length}`,
);

const name = `Alicia-${Date.now()}`;
const seq = Date.now();

const pgDeltaP = client.waitFor(
  (m) =>
    m.type === "delta" &&
    m.op === "update" &&
    String(m.id) === "1" &&
    m.changes &&
    Object.prototype.hasOwnProperty.call(m.changes, "name"),
  20_000,
);

if (pgCmd) {
  console.log("==> Postgres update via SMOKE_PG_CMD");
  await execAsync(pgCmd.replaceAll("__NAME__", name), { timeout: 15_000 });
} else {
  console.log("==> HTTP ingest fallback (no SMOKE_PG_CMD)");
}

const locDeltaP = client.waitFor(
  (m) =>
    m.type === "delta" &&
    m.op === "update" &&
    String(m.id) === "1" &&
    m.changes &&
    Object.prototype.hasOwnProperty.call(m.changes, "location"),
  20_000,
);

if (kafkaCmd) {
  console.log("==> Kafka produce via SMOKE_KAFKA_CMD");
  await execAsync(kafkaCmd.replaceAll("__SEQ__", String(seq)), { timeout: 15_000 });
} else {
  const result = await ingest(baseUrl, {
    source: "http",
    entity_type: "driver",
    id: "1",
    fields: { location: { lat: 36.16, lng: -86.78 } },
    versions: { location: seq },
  });
  if (!result.accepted) {
    throw new Error(`ingest not accepted: ${JSON.stringify(result)}`);
  }
  console.log(`ingest ok changed_fields=${JSON.stringify(result.changed_fields)}`);
}

if (pgCmd) {
  const pgDelta = await pgDeltaP;
  if (pgDelta.type !== "delta") {
    throw new Error("expected postgres name delta");
  }
  console.log(`postgres delta ok seq=${pgDelta.seq} name=${JSON.stringify(pgDelta.changes.name)}`);
}

const locDelta = await locDeltaP;
if (locDelta.type !== "delta") {
  throw new Error("expected location delta");
}
console.log(`location delta ok seq=${locDelta.seq}`);

client.close();
