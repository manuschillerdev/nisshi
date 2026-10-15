# HTTP push consumers

Nisshi can deliver a configured topic partition directly to an HTTP endpoint.
This runs inside the broker. A Cloudflare Durable Object or another actor can
sleep between requests; no Kafka polling loop or group heartbeat runs in the
consumer. The broker keeps running and polls storage for new records.

Enable it with `nisshi broker --http-push-config subscriptions.json` or the
`HTTP_PUSH_CONFIG` environment variable. The file is loaded once at startup:

```json
{
  "subscriptions": [
    {
      "group_id": "orders-http",
      "topic": "orders",
      "partition": 0,
      "endpoint": "https://consumer.example/consumers/orders",
      "bearer_token_env": "ORDERS_PUSH_TOKEN"
    }
  ]
}
```

Set `ORDERS_PUSH_TOKEN` in the broker's environment. The broker sends
`Authorization: Bearer <value>`. The field is optional for trusted local
endpoints. HTTPS uses standard certificate validation. HTTP is supported for
local/private networks. URLs must not contain userinfo, query parameters or
fragments; bearer credentials belong in the environment. Redirects are refused.
Only trusted operators should edit this file: its endpoints receive topic data
and the broker makes outbound connections to them.

Configure each `(group_id, topic, partition)` on **one broker only**. This first
version uses explicit assignments and independent tasks, without group balancing,
leases or automatic failover. Use a dedicated group ID that no Kafka consumer or
other push owner commits to. A broker restart resumes delivery from storage.
Use persistent broker storage when offsets must survive process restarts.

With no committed offset, delivery begins at the earliest retained offset. That
initial position is persisted before delivery, including on an empty topic, so
an outage followed by retention cannot silently skip records after a restart.
An existing committed offset takes precedence. Precommit an offset with Kafka's
offset tooling before starting the subscription to select another position.

## Delivery and acknowledgement

Each POST has `Content-Type: application/json` and this versioned body:

```json
{
  "version": 1,
  "group_id": "orders-http",
  "topic": "orders",
  "partition": 0,
  "next_offset": "43",
  "records": [
    {
      "offset": "42",
      "timestamp": "1792022400000",
      "key": null,
      "value": "aGVsbG8=",
      "headers": [{ "key": "dHJhY2U=", "value": null }]
    }
  ]
}
```

Offsets and timestamps are decimal strings, preserving 64-bit precision in
JavaScript. Timestamps are milliseconds since the Unix epoch. Key, value and
header fields are standard padded base64 or `null`; empty bytes encode as `""`.
Headers remain an ordered array, preserving duplicate keys and null values.

Any **2xx response acknowledges the entire batch**. Its body is ignored. Return
success only after all records have been processed durably. Every other status,
redirect, timeout or connection error retries the same payload indefinitely with
an exponential delay from 250 ms to 30 seconds. One batch is in flight per
subscription. Failed offset commits are retried before fetching more records.

Delivery is **at least once**, ordered within each partition. A lost response or
broker crash can replay records, including after the receiver processed them.
Deduplicate by topic/partition/offset within the subscription and store that
cursor atomically with local side effects. Batches may be split differently
after restart, so deduplicate records rather than whole request bodies.

Consumption uses **read_uncommitted**: open or aborted transaction data may be
delivered. Kafka control records are filtered, and their offsets are advanced
without HTTP requests. Compacted offsets can contain gaps. Transaction-aware
filtering and group coordination are outside this version.

## Bounds and recovery

Optional top-level settings:

| Setting | Default | Accepted range |
| --- | ---: | ---: |
| `poll_interval_ms` | 1000 | 1–60000 |
| `request_timeout_ms` | 30000 | 1–300000 |
| `max_batch_bytes` | 262144 | 1024–16777216 |
| `max_records` | 100 | 1–10000 |

The byte limit applies to the complete encoded HTTP body, including base64 and
JSON overhead. The broker splits Kafka batches at record boundaries. Storage may
return a first Kafka batch larger than its fetch limit; decoding retains the
existing Kafka batch safety limits. A record that cannot fit by itself stalls
that subscription and logs its offset. Increase `max_batch_bytes` and restart;
the broker never skips the record automatically. At most 1024 subscriptions are
accepted per file; unknown fields, duplicate assignments and invalid limits
fail startup.

If retention overtakes a committed offset, or the cursor exceeds the partition's
high watermark, delivery stalls and logs the range. Stop the owning broker,
inspect the gap, explicitly reset the group's offset using Kafka tooling, and
restart. Resetting accepts loss of any data already removed by retention. Do not
reuse a subscription for a deleted/recreated topic without resetting both broker
and receiver state. Cancellation interrupts delivery and retry waits; an
uncommitted batch is replayed after restart.

The [Durable Object example](../examples/http-push-durable-object/) includes a
secured endpoint and persistent deduplication. HTTP requests can activate a
Durable Object on demand; see Cloudflare's
[object lifecycle](https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/).
