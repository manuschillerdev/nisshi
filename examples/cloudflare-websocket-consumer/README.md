# Hibernating Durable Object consumer

The broker connects to this Worker, which forwards the connection to one named
Durable Object. `ctx.acceptWebSocket()` keeps the connection alive while the
object hibernates. New records invoke `webSocketMessage()`; there are no timers,
fetch loops or outgoing connections in the actor.

```sh
npm ci
# Use the same random secret here and in the broker's NISSHI_PUSH_TOKEN variable.
printf 'PUSH_TOKEN=replace-with-your-secret\n' > .dev.vars
npm run check
npm run dev
```

From the repository root, in another terminal:

```sh
export NISSHI_PUSH_TOKEN=replace-with-your-secret
cargo run -p nisshi --no-default-features --features dynostore -- broker --websocket-push-config examples/cloudflare-websocket-consumer/subscriptions.json
```

Create topic `events` with one partition and produce records using your usual
Kafka client. The broker can start before the topic exists; delivery retries.
Read the counter with `curl -H "Authorization: Bearer $NISSHI_PUSH_TOKEN"
http://127.0.0.1:8787/state`. A lost acknowledgement or reconnect replays records;
the durable offset prevents the counter increasing twice. Both the counter and
offset are updated in one storage transaction. Replace this counter with local
application effects in that transaction. For external effects, use an idempotency
key containing group, topic, partition and record offset.

To deploy, run `npx wrangler secret put PUSH_TOKEN`, then `npm run deploy`, and
change `endpoint` to `wss://<your-worker-host>/consumer`. Restart the broker after
configuration or secret changes. Use `ws://` only for local development; bearer
authentication requires `wss://` on untrusted networks. The deployment commands
create resources in your own Cloudflare account.

Local development verifies protocol handling and durable deduplication. The test
below also verifies automatic idle hibernation and message-triggered reconstruction
in local workerd; production eviction timing and billing require deployment observations.

The example uses one named actor. Multiple subscriptions can share it; progress
is keyed by group/topic/partition. To use distinct actors, extend the Worker route
with an actor name and configure each subscription's endpoint accordingly.

See [the wire protocol and operational limits](../../docs/websocket-push.md) and
[Cloudflare's hibernation API](https://developers.cloudflare.com/durable-objects/best-practices/websockets/).

## Local end-to-end test

From the repository root:

```sh
CARGO_BUILD_JOBS=4 cargo build -p nisshi --no-default-features --features dynostore
cd examples/cloudflare-websocket-consumer
npm ci
npm run test:e2e
```

This starts the real Nisshi binary on Kafka port 29092 and local workerd on port
28787. KafkaJS produces records before and after a 35-second idle interval.
The actor receives both records through the broker's native WebSocket delivery. The test verifies
exact keys, values, binary and duplicate headers; durable counts; and committed
Kafka offsets 1 then 2. The idle interval covers the broker's native Ping interval,
with no actor requests or callbacks. There is no forced eviction or runtime
restart. The next Kafka record
must run on a new actor instance with the original socket attachment and exactly
one WebSocket upgrade overall.

A test-only subclass records constructor, socket and message identities while
calling the example's real handlers. Production example code has no eviction
hooks. Reports and raw broker/runtime logs are written to ignored `.e2e/` files;
processes are stopped on completion. Override ports with `NISSHI_KAFKA_PORT` and
`NISSHI_WORKER_PORT`, or the binary path with `NISSHI_BINARY`.

This verifies [WebSocket hibernation](https://developers.cloudflare.com/durable-objects/best-practices/websockets/)
in the pinned local workerd runtime: automatic idle eviction followed by wake-up
on the same socket. It does not measure Cloudflare's production eviction timing
or billing. The test stays local and makes no account changes.
