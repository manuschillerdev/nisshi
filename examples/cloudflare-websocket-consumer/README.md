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

Local development verifies protocol handling and durable deduplication. Proving
idle eviction and message-triggered wake-up requires observing the deployed DO;
this example does not claim a local simulator proves production hibernation.

The example uses one named actor. Multiple subscriptions can share it; progress
is keyed by group/topic/partition. To use distinct actors, extend the Worker route
with an actor name and configure each subscription's endpoint accordingly.

See [the wire protocol and operational limits](../../docs/websocket-push.md) and
[Cloudflare's hibernation API](https://developers.cloudflare.com/durable-objects/best-practices/websockets/).
