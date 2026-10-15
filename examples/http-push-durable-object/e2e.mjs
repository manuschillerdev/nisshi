import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { once } from "node:events";
import { createWriteStream } from "node:fs";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { connect } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as sleep } from "node:timers/promises";
import { Kafka, Partitioners, logLevel } from "kafkajs";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";

const directory = dirname(fileURLToPath(import.meta.url));
const root = resolve(directory, "../..");
const artifacts = join(directory, ".e2e");
const temporary = await mkdtemp(join(tmpdir(), "nisshi-http-e2e-"));
const brokerPort = Number(process.env.KAFKA_PORT ?? 19092);
const workerPort = Number(process.env.DO_PORT ?? 18787);
const run = crypto.randomUUID();
const topic = `http-e2e-${run}`;
const group = `http-push-${run}`;
const token = crypto.randomUUID();
const observations = [];
const checkpoints = [];
let runtime;
let broker;
let releaseRetry;
let failedAcknowledgement = false;
const retryGate = new Promise((resolve) => { releaseRetry = resolve; });
const kafka = new Kafka({
  clientId: "nisshi-http-e2e", brokers: [`127.0.0.1:${brokerPort}`],
  logLevel: logLevel.ERROR, retry: { retries: 10, initialRetryTime: 100 },
});
const admin = kafka.admin();
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
await mkdir(artifacts, { recursive: true });
const brokerLog = createWriteStream(join(artifacts, "broker.log"));
const started = Date.now();
const revision = execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim();
const versions = { node: process.version };
for (const name of ["miniflare", "kafkajs"]) {
  versions[name] = JSON.parse(await readFile(join(directory, "node_modules", name, "package.json"), "utf8")).version;
}
const record = (name, detail = {}) => {
  const checkpoint = { name, elapsed_ms: Date.now() - started, ...detail };
  checkpoints.push(checkpoint);
  console.log(JSON.stringify(checkpoint));
};

async function until(description, predicate) {
  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) {
    if (broker && (broker.exitCode !== null || broker.signalCode !== null)) {
      throw new Error(`Broker exited with ${broker.exitCode}; see .e2e/broker.log`);
    }
    const result = await predicate();
    if (result) return result;
    await sleep(50);
  }
  throw new Error(`Timed out waiting for ${description}`);
}

async function listening() {
  return new Promise((resolve) => {
    const socket = connect({ host: "127.0.0.1", port: brokerPort });
    socket.once("connect", () => { socket.destroy(); resolve(true); });
    socket.once("error", () => resolve(false));
    socket.setTimeout(500, () => { socket.destroy(); resolve(false); });
  });
}

async function committed() {
  const offsets = await admin.fetchOffsets({ groupId: group, topics: [topic] });
  return offsets[0]?.partitions[0]?.offset;
}

async function produce(phase, offset) {
  const key = `${run}:${phase}`;
  const value = JSON.stringify({ run, phase });
  const response = await producer.send({ topic, messages: [{ partition: 0, key, value }] });
  assert.equal(response[0].baseOffset, String(offset));
  record("produced", { phase, offset: response[0].baseOffset });
  return { key: Buffer.from(key).toString("base64"), value: Buffer.from(value).toString("base64") };
}

function assertDelivery(observation, expected, offset, processed) {
  assert.equal(observation.batch.group_id, group);
  assert.equal(observation.batch.topic, topic);
  assert.equal(observation.batch.partition, 0);
  assert.equal(observation.batch.records.length, 1);
  assert.equal(observation.batch.records[0].offset, String(offset));
  assert.equal(observation.batch.records[0].key, expected.key);
  assert.equal(observation.batch.records[0].value, expected.value);
  assert.equal(observation.streams[0].processed, String(processed));
  assert.equal(observation.streams[0].next_offset, String(offset + 1));
}

try {
  runtime = new Miniflare(convertV4MiniflareOptions({
    name: "http-e2e", modulesRoot: directory,
    modules: await Promise.all(["e2e-worker.js", "worker.js"].map(async (name) => ({
      type: "ESModule", path: join(directory, name), contents: await readFile(join(directory, name), "utf8"),
    }))),
    cf: false, telemetry: { enabled: false },
    compatibilityDate: "2026-10-15", host: "127.0.0.1", port: workerPort,
    durableObjects: { CONSUMERS: { className: "ObservedConsumer", useSQLite: true } },
    durableObjectsPersist: join(temporary, "durable-objects"),
    bindings: { PUSH_TOKEN: token },
    serviceBindings: {
      EVIDENCE: async (request) => {
        const observation = await request.json();
        observations.push(observation);
        if (observation.batch?.next_offset === "3") {
          if (!failedAcknowledgement) {
            failedAcknowledgement = true;
            observation.injected_status = 503;
            return new Response(null, { status: 503 });
          }
          await retryGate;
        }
        return new Response(null, { status: 204 });
      },
    },
  }));
  await runtime.ready;
  const endpoint = `http://127.0.0.1:${workerPort}/consumers/example`;
  assert.equal((await fetch(endpoint)).status, 401);
  assert.equal(observations.length, 0, "unauthorized request must not activate DO");
  const configuration = join(temporary, "subscriptions.json");
  await writeFile(configuration, JSON.stringify({
    poll_interval_ms: 50, request_timeout_ms: 10_000,
    subscriptions: [{ group_id: group, topic, partition: 0, endpoint, bearer_token_env: "PUSH_TOKEN" }],
  }));
  broker = spawn(process.env.NISSHI_BIN ?? join(root, "target/debug/nisshi"), [
    "broker", "--silent", "--storage-engine", "memory://http-e2e/",
    "--listener-url", `tcp://127.0.0.1:${brokerPort}`,
    "--advertised-listener-url", `tcp://127.0.0.1:${brokerPort}`,
    "--http-push-config", configuration,
  ], { cwd: root, env: { ...process.env, PUSH_TOKEN: token, RUST_LOG: "warn" }, stdio: ["ignore", "pipe", "pipe"] });
  broker.stdout.pipe(brokerLog, { end: false });
  broker.stderr.pipe(brokerLog, { end: false });
  broker.on("error", (error) => { console.error(error); });
  await until("Kafka listener", listening);
  await admin.connect();
  await admin.createTopics({ topics: [{ topic, numPartitions: 1, replicationFactor: 1 }] });
  await producer.connect();
  await until("initial offset", async () => (await committed()) === "0");
  assert.equal(observations.length, 0, "empty subscription must not wake DO");
  record("idle_before_first_delivery", { committed_offset: "0", do_requests: 0 });

  const first = await produce("before-eviction", 0);
  await until("first delivery commit", async () => (await committed()) === "1");
  assertDelivery(observations[0], first, 0, 1);
  const originalInstance = observations[0].instance;
  record("first_delivery", { instance: originalInstance, processed: "1", committed_offset: "1" });

  const idleRequests = observations.length;
  await sleep(15_000);
  assert.equal(observations.length, idleRequests, "DO must stay untouched during the idle window");
  record("idle_without_do_requests", { idle_ms: 15000, do_requests: idleRequests });

  const second = await produce("after-eviction", 1);
  await until("post-eviction delivery commit", async () => (await committed()) === "2");
  assertDelivery(observations[1], second, 1, 2);
  assert.notEqual(observations[1].instance, originalInstance);
  record("woken_by_delivery", { instance: observations[1].instance, processed: "2", committed_offset: "2" });

  const third = await produce("lost-acknowledgement", 2);
  await until("retry after injected 503", () => observations.filter((entry) => entry.batch?.next_offset === "3").length === 2);
  const attempts = observations.filter((entry) => entry.batch?.next_offset === "3");
  assert.equal(attempts[0].injected_status, 503);
  assert.deepEqual(attempts[0].batch, attempts[1].batch);
  for (const attempt of attempts) assertDelivery(attempt, third, 2, 3);
  assert.equal(await committed(), "2", "failed/held ack must not advance broker offset");
  record("retry_deduplicated", { attempts: 2, processed: "3", committed_offset_before_ack: "2" });
  releaseRetry();
  await until("retry acknowledgement commit", async () => (await committed()) === "3");
  record("passed", { processed: "3", committed_offset: "3", deliveries: observations.length });
} catch (error) {
  record("failed", { error: String(error.stack ?? error) });
  process.exitCode = 1;
} finally {
  releaseRetry();
  await Promise.allSettled([producer.disconnect(), admin.disconnect()]);
  if (broker?.pid && broker.exitCode === null && broker.signalCode === null) {
    broker.kill("SIGTERM");
    await Promise.race([once(broker, "exit"), sleep(6_000, undefined, { ref: false })]);
    if (broker.exitCode === null && broker.signalCode === null) { broker.kill("SIGKILL"); await once(broker, "exit"); }
  }
  await runtime?.dispose().catch((error) => {
    record("cleanup_failed", { error: error.message });
    process.exitCode = 1;
  });
  brokerLog.end();
  await writeFile(join(artifacts, "evidence.json"), JSON.stringify({
    run, started_at: new Date(started).toISOString(), revision, versions,
    transport: "http", broker_port: brokerPort, do_port: workerPort,
    limitation: "Automatic local workerd idle eviction; does not establish production idle timing or billing.",
    checkpoints, observations,
  }, null, 2) + "\n");
  await rm(temporary, { recursive: true, force: true });
}
