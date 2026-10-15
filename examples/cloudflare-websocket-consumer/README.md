# Hibernating Durable Object consumer

The broker connects to this Worker, which forwards the connection to one named
Durable Object. `ctx.acceptWebSocket()` keeps the connection alive while the
object hibernates. New committed records invoke `webSocketMessage()`. The actor
uses request/response messages on that same socket to produce output records and
commit its input offset in one Kafka transaction. Request timeouts exist only
while work is in progress; there are no idle timers, polling loops or outgoing
connections in the actor.

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

Create topics `events` and `results`, each with one partition, and produce records
to `events` using your usual Kafka client. The broker can start before the topics
exist; delivery retries. Use a fresh dynostore cluster for this example; older
transaction history requires migration before committed-only push is safe.
Read the counter with `curl -H "Authorization: Bearer $NISSHI_PUSH_TOKEN"
http://127.0.0.1:8787/state`. Consume `results` with `read_committed` to see outputs.
The example copies keys, values, timestamps and headers to that topic. Replace
that transformation with your application logic. The configured `output_topics`
allowlist must include `OUTPUT_TOPIC` from `wrangler.jsonc`.

The durable application result is the Kafka output, committed together with the
input offset. The DO counter is a local receipt of confirmed commits, not an
atomic transaction spanning Kafka and DO storage. A pending receipt survives a
lost commit response and is reconciled against the broker's offset on reconnect.
Incomplete Kafka transactions are aborted/fenced before retry. External tool
effects still require idempotency using the input topic, partition and offset;
Kafka transactions cannot roll back an external action.

To deploy, run `npx wrangler secret put PUSH_TOKEN`, then `npm run deploy`, and
change `endpoint` to `wss://<your-worker-host>/consumer`. Restart the broker after
configuration or secret changes. Use `ws://` only for local development; bearer
authentication requires `wss://` on untrusted networks. The deployment commands
create resources in your own Cloudflare account.

Local development verifies protocol handling and durable deduplication. The test
below also verifies automatic idle hibernation and message-triggered reconstruction
in local workerd; production eviction timing and billing require deployment observations.

The example uses one named actor with one subscription and one accepted socket.
To use distinct actors, extend the Worker route with an actor name and configure
each subscription's endpoint and unique `transactional_id` accordingly.

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
one WebSocket upgrade during this idle/wake check.

The test then holds a source transaction open, aborts it, and verifies its intent
never reaches the actor. It commits another source transaction and checks delivery.
It also aborts one output transaction before retry, drops a successful commit
response before the application receives it, and verifies recovery. A separate
KafkaJS `read_committed` consumer sees exactly the five expected output records,
while the source group advances to offset 8. The deliberate failure scenarios
reconnect the socket; automatic hibernation itself does not.

A test-only subclass records constructor, socket and message identities while
calling the example's real handlers. Production example code has no eviction
hooks. Reports and raw broker/runtime logs are written to ignored `.e2e/` files;
processes are stopped on completion. Override ports with `NISSHI_KAFKA_PORT` and
`NISSHI_WORKER_PORT`, or the binary path with `NISSHI_BINARY`.

This verifies [WebSocket hibernation](https://developers.cloudflare.com/durable-objects/best-practices/websockets/)
in the pinned local workerd runtime: automatic idle eviction followed by wake-up
on the same socket. It does not measure Cloudflare's production eviction timing
or billing. The test stays local and makes no account changes.
