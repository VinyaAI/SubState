#!/usr/bin/env node
/**
 * End-to-end smoke using @substate/client against a live SubState server.
 * Expects Compose demo schema (entity `driver`, seed id `1` = Alice).
 */
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const clientDist = path.resolve(__dirname, "../clients/typescript/dist/index.js");
const { connect, ingest } = await import(clientDist);

const baseUrl = process.env.SUBSTATE_URL ?? "http://127.0.0.1:8080";
const wsUrl = process.env.SUBSTATE_WS_URL ?? "ws://127.0.0.1:8080/v1/sync";

const client = await connect(wsUrl);

const subscribedP = client.waitFor((m) => m.type === "subscribed", 15_000);
const snapshotP = client.waitFor((m) => m.type === "snapshot", 15_000);

client.subscribe("driver", { status: "available" });

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

const deltaP = client.waitFor(
  (m) => m.type === "delta" && m.op === "update" && m.id === "1",
  20_000,
);

const seq = Date.now();
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

const delta = await deltaP;
if (delta.type !== "delta") {
  throw new Error("expected delta");
}
console.log(`delta ok seq=${delta.seq} op=${delta.op} id=${delta.id}`);

client.close();
