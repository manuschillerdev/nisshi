# HTTP push consumer in a Durable Object

This example counts newly delivered records and stores that count atomically with
its deduplication cursor. The broker sends requests only when records arrive.
The object has no timers or outgoing connections and can sleep between requests.

Run from this directory with Node.js and npm installed:

```sh
printf 'PUSH_TOKEN=local-development-secret\n' > .dev.vars
npx --yes wrangler@4 dev
```

In another terminal, start Nisshi from the repository root:

```sh
export PUSH_TOKEN=local-development-secret
cargo run -p nisshi --no-default-features --features dynostore -- broker \
  --storage-engine memory://nisshi/ \
  --http-push-config examples/http-push-durable-object/subscriptions.json
```

Create the `orders` topic with one partition using a Kafka client, then produce
records to partition zero. With Kafka CLI tools:

```sh
kafka-topics --bootstrap-server localhost:9092 --create \
  --topic orders --partitions 1 --replication-factor 1
printf 'hello\nworld\n' | kafka-console-producer \
  --bootstrap-server localhost:9092 --topic orders
curl -H 'Authorization: Bearer local-development-secret' \
  http://127.0.0.1:8787/consumers/orders
```

The response lists each subscription's next offset and count. Stop and restart
the local Worker; `.wrangler/state` preserves its database. Replayed records do
not increment the count again. The memory broker backend loses records and
broker offsets on restart; use persistent storage to test broker recovery.

To deploy the example, run `npx wrangler@4 secret put PUSH_TOKEN`, then
`npx wrangler@4 deploy`. Change the subscription endpoint to the deployed HTTPS
URL and set the matching broker environment variable. Restart the broker to load
the configuration. These commands deploy only when you run them.

The Worker checks bearer authentication before routing to the DO. The example's
counter represents local SQL side effects. An external API call cannot share the
DO transaction: use the record's topic/partition/offset as an idempotency key or
persist an outbox before acknowledging. Acknowledge only durable processing;
`waitUntil()` after returning a successful response is too late.

See [the delivery contract](../../docs/http-push.md) and Cloudflare's
[SQLite storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/).
