import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { appendFileSync, closeSync, mkdirSync, openSync, readFileSync, writeFileSync } from "node:fs";
import { access, mkdtemp, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { connect } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { Kafka, logLevel } from "kafkajs";
import { Miniflare } from "miniflare";

const example = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const binary = resolve(example, process.env.NISSHI_BINARY ?? "../../target/debug/nisshi");
const output = resolve(example, ".e2e");
const kafkaPort = Number(process.env.NISSHI_KAFKA_PORT ?? 29092);
const workerPort = Number(process.env.NISSHI_WORKER_PORT ?? 28787);
const runId = randomUUID();
const topic = `ws-e2e-${runId}`;
const groupId = `ws-e2e-${runId}`;
const secret = randomUUID();
const events = [];
const require = createRequire(import.meta.url);
const versions = Object.fromEntries(["miniflare", "workerd", "kafkajs", "esbuild"].map((name) => [name, require(`${name}/package.json`).version]));
versions.node = process.version;
const sourceRevision = execFileSync("git", ["rev-parse", "HEAD"], { cwd: example, encoding: "utf8" }).trim();
const sourceHashes = Object.fromEntries(["test/e2e.mjs", "test/consumer.ts", "src/index.ts"].map((path) => [path, createHash("sha256").update(readFileSync(join(example, path))).digest("hex")]));
const report = { run_id: runId, scope: "local workerd automatic idle hibernation; no Cloudflare deployment", status: "running", source_revision: sourceRevision, source_sha256: sourceHashes, versions, events };
let runtime, broker, producer, admin, brokerLog, temporary;

async function until(description, check, milliseconds = 30_000) {
  const deadline = Date.now() + milliseconds;
  while (Date.now() < deadline) {
    if (report.broker_error) throw new Error(report.broker_error);
    if (broker && (broker.exitCode !== null || broker.signalCode !== null)) throw new Error(`Broker exited ${broker.exitCode}; see .e2e/broker.log`);
    const answer = await check();
    if (answer) return answer;
    await sleep(50);
  }
  throw new Error(`Timed out: ${description}`);
}

async function committed(expected) {
  return until(`committed offset ${expected}`, async () => {
    const offsets = await admin.fetchOffsets({ groupId, topics: [topic] });
    const offset = offsets.find((entry) => entry.topic === topic)?.partitions.find((entry) => entry.partition === 0)?.offset;
    return offset === expected ? offset : undefined;
  });
}

function assertRecord(delivery, phase, offset) {
  assert.equal(delivery.group_id, groupId);
  assert.equal(delivery.topic, topic);
  assert.equal(delivery.partition, 0);
  assert.equal(delivery.next_offset, String(Number(offset) + 1));
  assert.equal(delivery.records.length, 1);
  const record = delivery.records[0];
  assert.equal(record.offset, offset);
  assert.equal(Buffer.from(record.key, "base64").toString(), `${runId}:${phase}`);
  assert.deepEqual(JSON.parse(Buffer.from(record.value, "base64").toString()), { run_id: runId, phase });
  assert.deepEqual(record.headers.map(({ key, value }) => ({ key: Buffer.from(key, "base64").toString(), value: value === null ? null : [...Buffer.from(value, "base64")] })), [
    { key: "phase", value: [...Buffer.from(phase)] },
    { key: "binary", value: [0, 255] },
    { key: "duplicate", value: [...Buffer.from("one")] },
    { key: "duplicate", value: [...Buffer.from("two")] },
  ]);
}

async function produce(phase) {
  return producer.send({ topic, messages: [{ partition: 0, key: `${runId}:${phase}`,
    value: JSON.stringify({ run_id: runId, phase }),
    headers: { phase, binary: Buffer.from([0, 255]), duplicate: ["one", "two"] },
  }] });
}

async function state() {
  const response = await fetch(`http://127.0.0.1:${workerPort}/state`, { headers: { Authorization: `Bearer ${secret}` } });
  assert.equal(response.status, 200);
  return (await response.json())[`progress:${JSON.stringify([groupId, topic, 0])}`];
}

try {
  await access(binary);
  mkdirSync(output, { recursive: true });
  writeFileSync(join(output, "runtime.log"), "");
  temporary = await mkdtemp(join(tmpdir(), "nisshi-ws-e2e-"));
  const bundled = await build({ entryPoints: [join(example, "test/consumer.ts")], bundle: true, format: "esm", platform: "browser", target: "es2022", external: ["cloudflare:workers"], write: false });
  runtime = new Miniflare({
    host: "127.0.0.1", port: workerPort, cf: false, telemetry: { enabled: false },
    handleStructuredLogs(log) {
      appendFileSync(join(output, "runtime.log"), JSON.stringify(log) + "\n");
      const marker = log.message.indexOf("NISSHI_E2E ");
      if (marker >= 0) {
        const event = JSON.parse(log.message.slice(marker + "NISSHI_E2E ".length));
        events.push({ observed_at: new Date().toISOString(), ...event });
      }
    },
    workers: [{ config: {
      name: "nisshi-ws-e2e", compatibilityDate: "2026-10-15",
      manifest: { mainModule: "index.js", modulesRoot: example, modules: { "index.js": { type: "esm", contents: bundled.outputFiles[0].text } } },
      env: { PUSH_TOKEN: { type: "text", value: secret }, CONSUMERS: { type: "durable-object", worker: "nisshi-ws-e2e", exportName: "ObservedConsumer" } },
      exports: { ObservedConsumer: { type: "durable-object", storage: "sqlite" } },
    } }],
  });
  await runtime.ready;
  const configuration = join(temporary, "subscriptions.json");
  writeFileSync(configuration, JSON.stringify({ subscriptions: [{ group_id: groupId, topic, partition: 0, endpoint: `ws://127.0.0.1:${workerPort}/consumer`, bearer_token_env: "NISSHI_E2E_TOKEN" }] }));
  brokerLog = openSync(join(output, "broker.log"), "w");
  broker = spawn(binary, ["broker", "--silent", "--listener-url", `tcp://127.0.0.1:${kafkaPort}`, "--advertised-listener-url", `tcp://127.0.0.1:${kafkaPort}`, "--storage-engine", "memory://", "--websocket-push-config", configuration], {
    cwd: temporary, env: { PATH: process.env.PATH, HOME: process.env.HOME, RUST_LOG: "nisshi_broker=info", NISSHI_E2E_TOKEN: secret }, stdio: ["ignore", brokerLog, brokerLog],
  });
  broker.on("error", (error) => { report.broker_error = error.message; });
  process.on("exit", () => { if (broker?.exitCode === null) broker.kill("SIGTERM"); });
  await until("Kafka listener", () => new Promise((ready) => {
    const socket = connect({ host: "127.0.0.1", port: kafkaPort });
    socket.once("connect", () => { socket.destroy(); ready(true); });
    socket.once("error", () => { socket.destroy(); ready(false); });
  }));
  const kafka = new Kafka({ clientId: "nisshi-ws-e2e", brokers: [`127.0.0.1:${kafkaPort}`], logLevel: logLevel.ERROR });
  admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({ topics: [{ topic, numPartitions: 1, replicationFactor: 1 }], waitForLeaders: true });
  producer = kafka.producer({ idempotent: false });
  await producer.connect();
  const upgrade = await until("initial websocket upgrade", () => events.find((entry) => entry.event === "upgrade"));
  const firstProduce = await produce("before-eviction");
  const first = await until("first record processed", () => events.find((entry) => entry.event === "processed" && entry.next_offset === "1"));
  const firstMessage = events.find((entry) => entry.event === "message" && entry.delivery.next_offset === "1");
  assertRecord(firstMessage.delivery, "before-eviction", "0");
  assert.deepEqual(await state(), { next_offset: "1", count: "1" });
  const beforeOffset = await committed("1");
  assert.equal(first.connection_id, upgrade.connection_id);

  const idleStartedAt = new Date().toISOString();
  const idleEvents = events.length;
  const idleStarted = Date.now();
  console.log("Observing 35 seconds of idle time without actor requests or forced eviction...");
  // Only the harness waits. The broker's 30-second native Ping must not execute the actor.
  await sleep(35_000);
  const idleMilliseconds = Date.now() - idleStarted;
  const callbacksDuringIdle = events.length - idleEvents;
  assert.equal(callbacksDuringIdle, 0, "Actor ran or socket reconnected during the idle period");
  const secondProduce = await produce("after-eviction");
  const second = await until("second record processed", () => events.find((entry) => entry.event === "processed" && entry.next_offset === "2"));
  const secondMessage = events.find((entry) => entry.event === "message" && entry.delivery.next_offset === "2");
  assertRecord(secondMessage.delivery, "after-eviction", "1");
  assert.notEqual(second.instance_id, first.instance_id, "Automatic idle hibernation did not replace the actor instance");
  assert.equal(second.connection_id, first.connection_id, "Wake used a different websocket");
  assert.equal(events.filter((entry) => entry.event === "upgrade").length, 1, "Broker reconnected instead of waking the hibernated socket");
  assert.equal(events.filter((entry) => entry.event === "message").length, 2);
  const afterOffset = await committed("2");
  const finalState = await state();
  assert.deepEqual(finalState, { next_offset: "2", count: "2" });
  Object.assign(report, { status: "passed", topic, group_id: groupId, kafka_produce: [firstProduce, secondProduce], committed_offsets: [beforeOffset, afterOffset], durable_state: finalState,
    hibernation: { method: "automatic local workerd idle eviction", forced: false, idle_started_at: idleStartedAt, idle_ms: idleMilliseconds, actor_callbacks_during_idle: callbacksDuringIdle },
    before_instance_id: first.instance_id, after_instance_id: second.instance_id, connection_id: first.connection_id, websocket_upgrades: 1,
  });
  console.log(JSON.stringify({ status: report.status, committed_offsets: report.committed_offsets, connection_id: report.connection_id, websocket_upgrades: report.websocket_upgrades, report: join(output, "report.json") }, null, 2));
} catch (error) {
  Object.assign(report, { status: "failed", error: error.stack });
  process.exitCode = 1;
  console.error(error);
} finally {
  await producer?.disconnect().catch(() => {});
  await admin?.disconnect().catch(() => {});
  if (broker?.pid && broker.exitCode === null && broker.signalCode === null) {
    broker.kill("SIGTERM");
    await Promise.race([new Promise((done) => broker.once("exit", done)), sleep(6_000, undefined, { ref: false })]);
    if (broker.exitCode === null && broker.signalCode === null) broker.kill("SIGKILL");
  }
  await runtime?.dispose().catch((error) => {
    Object.assign(report, { status: "failed", cleanup_error: error.message });
    process.exitCode = 1;
  });
  if (brokerLog !== undefined) closeSync(brokerLog);
  if (temporary) await rm(temporary, { recursive: true, force: true });
  mkdirSync(output, { recursive: true });
  writeFileSync(join(output, "report.json"), JSON.stringify(report, null, 2) + "\n");
}
