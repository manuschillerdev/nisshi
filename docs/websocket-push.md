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
    "bearer_token_env": "NISSHI_PUSH_TOKEN",
    "transactional_id": "customer-projection-0",
    "output_topics": ["projected-events"]
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
with Kafka consumers. There is no consumer-group membership, balancing, subscription-owner fencing,
or automatic takeover. When moving a subscription, stop its old owner before
starting its replacement. Different group IDs independently receive all records.
Configuration changes require a restart; removing configuration preserves offsets.
`transactional_id` and `output_topics` enable Kafka producer requests on the same
accepted socket. The transaction ID must be unique across subscriptions and
brokers, and output topic names form an allowlist. Omitting both fields enables
the acknowledgement-only consumer described below. Endpoint authentication grants
the configured actor access to its subscription and, in transactional mode, write
access to the allowed output topics. It does not expose general Kafka administration.

Delivery always uses **read-committed** isolation: open transactions block the
last stable offset, aborted records are filtered, and transaction markers are
omitted. Filtering also applies when resuming inside an aborted batch or when a
producer ID is reused by a later committed transaction. Startup rejects storage
backends without committed transactional consumption support. Dynostore
(`memory`, `s3`, `gs`) and SQLite (`sqlite`, built with `libsql`) implement this
capability. PostgreSQL and other unverified backends fail closed; PostgreSQL's
abort metadata is not implemented.
Dynostore with a lakehouse sink configured also fails closed: Kafka transactions
cannot atomically publish an external lakehouse write. Aborted transaction history
is retained until topic deletion, so repeated aborts can grow cluster metadata;
`DeleteRecords` does not garbage-collect that history.

Existing Dynostore metadata that has transaction history predating this change
cannot reconstruct its discarded abort history and is rejected. Use a fresh
cluster for transactional consumption; there is no automatic historical migration.
Upgraded Dynostore keeps authoritative group offsets and transaction decisions
together in cluster metadata. Mixing old and new brokers, or downgrading after
this migration, is unsupported.

SQLite stores aborted ranges in its database and publishes output records,
transaction decisions and consumed offsets in one database transaction. Abort
history survives restart and reuse of a producer epoch. Its transaction
maintenance sweep aborts timed-out work and fences the old producer. SQLite
databases with transaction history predating abort tracking are rejected; use a
fresh database for this proof of concept. SQLite with a lakehouse sink also fails
closed. Keep the database and WAL on persistent storage across broker replacement.

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
string (`""`). Headers stay ordered and duplicate keys are preserved.

## Kafka transactions on the same socket

In transactional mode, the actor sends JSON requests and receives correlated
responses on the accepted socket. This maps a bounded subset of Kafka operations
to the existing broker storage APIs; it is not a binary Kafka tunnel. Requests
work while a delivery is pending and while idle. No consumer polling or heartbeat
is necessary. Each request has this envelope:

```json
{"version":1,"type":"request","request_id":"1","method":"init_producer_id","params":{}}
```

```json
{"version":1,"type":"response","request_id":"1","error_code":0,"result":{"producer_id":"42","producer_epoch":0}}
```

`error_code` uses Kafka numeric error codes (`0` means success). Error responses
have an empty result and never echo payloads. Request IDs must be nonempty and at
most 128 bytes. Send one request at a time and await its response. Producer IDs,
offsets and timestamps are canonical decimal strings; epochs, partition numbers
and base sequences are JSON integers. All incoming frames and messages are
limited to 1 MiB. Unknown fields and malformed envelopes are rejected.

The group, input topic/partition and transactional ID are fixed by configuration:

| Method | Parameters | Successful result |
| --- | --- | --- |
| `offset_fetch` | `{}` | `{"next_offset":"8"}` for this subscription |
| `init_producer_id` | `{}` | `producer_id`, `producer_epoch`; fences/aborts a previous unfinished incarnation |
| `add_partitions_to_txn` | `producer_id`, `producer_epoch`, `topic`, `partition` | `{}` |
| `produce` | `producer_id`, `producer_epoch`, `topic`, `partition`, `base_sequence`, `records` | `{"base_offset":"12"}` |
| `add_offsets_to_txn` | `producer_id`, `producer_epoch` | `{}` |
| `txn_offset_commit` | `producer_id`, `producer_epoch`, `delivery_id`, `next_offset` | `{}` |
| `end_txn` | `producer_id`, `producer_epoch`, `committed` (boolean) | `{}` |

`produce.records` contains 1–100 records with `timestamp`, `key`, `value` and
`headers` using the delivery encoding above, without an `offset` field. The broker
builds ordinary transactional Kafka batches; actors cannot inject control records.
The topic must be in `output_topics`. Sequence numbers start at zero after
initialization and increase by the number of records produced to that partition;
retrying a produce uses the same sequence and identical records. Requests must use
the producer identity initialized on their current socket.

For each delivery, initialize a producer, then fetch the committed offset. This
order matters: initialization can finish a previously prepared commit. If the
offset already covers the delivery, do not produce again. Otherwise add output
partitions, produce results, add the configured group to the transaction, stage
the exact delivery ID and next offset, and commit with `end_txn`. The entire
delivery must finish within 30 seconds, including all requests. Initialization
also uses a 30-second Kafka transaction timeout.

`txn_offset_commit` stages the input cursor; it does not publish it. A successful
`end_txn` commits output records and the consumed offset as one transaction.
Ordinary ACK messages are rejected in this mode, and a commit cannot complete a
pending delivery without its matching staged offset. Sending `committed:false`
aborts output and staged offsets; the pending input remains available for a new
transaction. Consumers of the output must also use read-committed isolation.

A disconnect during processing can replay the input. Initialize again to fence
the previous producer before retrying. If a commit result is lost or uncertain,
the broker reloads the durable group offset before replaying: a committed delivery
is suppressed, while an uncommitted delivery can retry. The broker also reconciles
after producer initialization. If initialization completed the old pending commit,
it closes the connection before accepting further output and resumes from the
committed cursor on a new connection. Error/timeout outcomes
from commit require reconnect and reconciliation. Output and input offsets share
the Kafka transaction; Durable Object storage and external side effects do not.
They require their own idempotency or reconciliation.

## Acknowledgement-only mode

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
The initial starting position and progress over control-only or aborted-only
batches can be committed without a delivery. This also applies in transactional
mode because no visible application records are being acknowledged.

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
delivery completion. Connect, send, storage operations, individual RPCs and flush
have 10-second timeouts; delivery completion has a 30-second timeout. Retry delay doubles from
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
includes a secured handshake and a transactional consume/produce loop on the
accepted socket. Accepted WebSockets are required for
[Cloudflare hibernation](https://developers.cloudflare.com/durable-objects/best-practices/websockets/).
