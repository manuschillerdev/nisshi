# WebSocket push consumers

Nisshi can initiate WebSocket connections to consumer endpoints and push records
as they arrive. The delivery tasks run inside the broker. A Cloudflare Durable
Object accepts the connection using `ctx.acceptWebSocket()` and processes records
in `webSocketMessage()`, so the consumer can hibernate between deliveries. The
broker stays running and polls storage; consumers do not poll or send heartbeats.
Other actor runtimes must support hibernation on accepted connections as well.

Enable delivery with `nisshi broker --websocket-push-config subscriptions.json`
(or `NISSHI_WEBSOCKET_PUSH_CONFIG`). The configuration is read once at startup:

```json
{
  "subscriptions": [{
    "group_id": "customer-projection",
    "topic": "events",
    "partition": 0,
    "endpoint": "wss://consumer.example.com/consumer",
    "bearer_token_env": "NISSHI_PUSH_TOKEN"
  }]
}
```

`bearer_token_env` is optional and names an environment variable containing the
bearer token. Missing or empty credentials fail startup. The handshake sends
`Authorization: Bearer <token>`. Credentials and peer message bodies are not
logged. URLs must use `ws` or `wss`, without user info, query strings or fragments.
Use `wss` outside local development; TLS uses system trust roots. The broker does
not follow redirects. This is trusted operator configuration, so endpoints may
address private networks. Kafka authentication is independent of endpoint auth.

A subscription has a stable group ID and one explicit topic/partition. Configure
each group/topic/partition on **exactly one broker**, and do not use that group
with Kafka consumers. There is no consumer-group membership, balancing, fencing,
or automatic takeover. When moving a subscription, stop its old owner before
starting its replacement. Different group IDs independently receive all records.
Configuration changes require a restart; removing configuration preserves offsets.

## Delivery and acknowledgement

The broker sends a JSON text message with at most 100 records and 1 MiB of encoded
JSON. Each subscription has at most one batch in flight. For example:

```json
{
  "version": 1,
  "type": "records",
  "delivery_id": "019cdd31-1020-7000-8000-000000000001",
  "group_id": "customer-projection",
  "topic": "events",
  "partition": 0,
  "next_offset": "8",
  "records": [{
    "offset": "7",
    "timestamp": "1760000000000",
    "key": null,
    "value": "aGVsbG8=",
    "headers": [{"key": "a2luZA==", "value": "ZXZlbnQ="}]
  }]
}
```

Offsets and timestamps are decimal strings, preserving signed 64-bit precision
in JavaScript. Timestamps are milliseconds since the Unix epoch. Keys, values and
header keys/values are standard padded base64; `null` differs from an empty byte
string (`""`). Headers stay ordered and duplicate keys are preserved. Transaction
control records are omitted. Consumption uses **read-uncommitted** isolation:
records in open or aborted transactions can be delivered.

After durably processing the records, reply on the same socket with:

```json
{
  "version": 1,
  "type": "ack",
  "delivery_id": "019cdd31-1020-7000-8000-000000000001",
  "group_id": "customer-projection",
  "topic": "events",
  "partition": 0,
  "next_offset": "8"
}
```

Every field must match the pending delivery, with no extra fields. `next_offset`
is the next position to consume, including any omitted control records or
compaction gaps. Only a valid acknowledgement allows committing data progress.
The initial starting position and progress over control-only batches can be
committed without a delivery. Incoming messages/frames are limited to 4 KiB.

Delivery is **at least once**. A disconnect, malformed/wrong acknowledgement,
processing timeout or failed offset commit causes reconnect and retry. Retries
within a broker run keep the same delivery ID and payload. Restart may produce a
new ID or batch boundary: deduplicate by group/topic/partition/offset, not delivery
ID. Preserve partition order and atomically store the deduplication cursor with
local application changes before acknowledging. External effects need their own
idempotency mechanism. Failed processing blocks that partition; no records are
discarded after a retry limit.

## Recovery and limits

Stored Kafka group offsets survive broker restarts when the storage backend is
durable. Memory storage does not survive process exit. A subscription without a
committed offset starts at the earliest retained offset and persists that starting
position before connecting. Stop the broker, reset the group's offsets using
existing Kafka tooling, then restart to replay or resolve an expired cursor.
If retention overtakes the cursor, delivery reports an error and stops advancing;
it does not silently jump past lost records. Deleting/recreating a topic or
resetting the source log requires coordinated reset of consumer state and offsets.

The broker polls every 500 ms when idle and drains backlog immediately after an
acknowledgement. Connect, send, storage operations and flush have 10-second
timeouts; acknowledgement has a 30-second timeout. Retry delay doubles from
500 ms to 30 seconds. WebSocket protocol Ping frames run every 30 seconds while
idle, and a missing Pong by the next tick triggers reconnect. No JSON heartbeats
or empty record batches are sent. Cloudflare handles protocol ping/pong without
waking hibernating objects. Reconnection itself wakes the endpoint to accept the
new socket. Broker shutdown cancels outstanding delivery; uncommitted data replays.

Kafka fetch can return an oversized first batch. The broker uses the existing
bounded Kafka decoder and splits records into bounded JSON deliveries, filtering
out offsets already acknowledged. A **single record** that cannot fit in 1 MiB
blocks the subscription and reports an error; it is never dropped. Limits are
fixed in this initial implementation. Keep individual records below the limit,
including base64 expansion and metadata. Delivery does not imply infinite
retention or exactly-once external side effects.

A runnable [Durable Object example](../examples/cloudflare-websocket-consumer/)
includes a secured handshake, persistent deduplication and a counter updated in
the same transaction as the offset. Accepted WebSockets are required for
[Cloudflare hibernation](https://developers.cloudflare.com/durable-objects/best-practices/websockets/).
