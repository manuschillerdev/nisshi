import { DurableObject } from "cloudflare:workers";

type Delivery = {
  version: 1;
  type: "records";
  delivery_id: string;
  group_id: string;
  topic: string;
  partition: number;
  next_offset: string;
  records: Array<{ offset: string }>;
};

type Progress = { next_offset: string; count: string };

function isOffset(offset: unknown): offset is string {
  return typeof offset === "string" && /^(0|[1-9][0-9]{0,18})$/.test(offset)
    && BigInt(offset) <= 9223372036854775807n;
}

function isDelivery(delivery: unknown): delivery is Delivery {
  if (typeof delivery !== "object" || delivery === null
    || !("version" in delivery) || delivery.version !== 1
    || !("type" in delivery) || delivery.type !== "records"
    || !("delivery_id" in delivery) || typeof delivery.delivery_id !== "string"
    || !("group_id" in delivery) || typeof delivery.group_id !== "string"
    || !("topic" in delivery) || typeof delivery.topic !== "string"
    || !("partition" in delivery) || typeof delivery.partition !== "number"
    || !Number.isInteger(delivery.partition) || delivery.partition < 0
    || !("next_offset" in delivery) || !isOffset(delivery.next_offset)
    || !("records" in delivery) || !Array.isArray(delivery.records)
    || delivery.records.length === 0 || delivery.records.length > 100) return false;
  let previous = -1n;
  const next = BigInt(delivery.next_offset);
  for (const record of delivery.records) {
    if (typeof record !== "object" || record === null || !isOffset(record.offset)) return false;
    const offset = BigInt(record.offset);
    if (offset <= previous || offset >= next) return false;
    previous = offset;
  }
  return true;
}

export class Consumer extends DurableObject<Env> {
  async fetch(request: Request): Promise<Response> {
    if (new URL(request.url).pathname === "/state") {
      return Response.json(Object.fromEntries(await this.ctx.storage.list({ prefix: "progress:" })));
    }
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("WebSocket upgrade required", { status: 426 });
    }
    const [client, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server);
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(socket: WebSocket, message: string | ArrayBuffer): Promise<void> {
    if (typeof message !== "string" || new TextEncoder().encode(message).length > 1024 * 1024) {
      socket.close(1009, "Expected bounded JSON text");
      return;
    }
    let delivery: unknown;
    try { delivery = JSON.parse(message); } catch {
      socket.close(1008, "Invalid JSON");
      return;
    }
    if (!isDelivery(delivery)) {
      socket.close(1008, "Invalid delivery");
      return;
    }
    const { version, delivery_id, group_id, topic, partition, next_offset, records } = delivery;
    const key = `progress:${JSON.stringify([group_id, topic, partition])}`;
    try {
      // This example's effect is a counter. Keep application changes and the
      // deduplication cursor in the same transaction; external effects need idempotency.
      await this.ctx.storage.transaction(async (transaction) => {
        const progress = await transaction.get<Progress>(key);
        const cursor = BigInt(progress?.next_offset ?? "0");
        let count = BigInt(progress?.count ?? "0");
        for (const record of records) {
          if (BigInt(record.offset) >= cursor) count++;
        }
        await transaction.put(key, {
          next_offset: (BigInt(next_offset) > cursor ? BigInt(next_offset) : cursor).toString(),
          count: count.toString(),
        } satisfies Progress);
      });
      socket.send(JSON.stringify({ version, type: "ack", delivery_id, group_id, topic, partition, next_offset }));
    } catch {
      console.error("Consumer transaction failed; broker will replay the batch");
      socket.close(1011, "Processing failed");
    }
  }

  webSocketClose(socket: WebSocket, code: number, reason: string): void {
    socket.close(code, reason);
  }
}

async function authenticated(request: Request, secret: string): Promise<boolean> {
  if (!secret) return false;
  const encoder = new TextEncoder();
  const [actual, expected] = await Promise.all([
    crypto.subtle.digest("SHA-256", encoder.encode(request.headers.get("Authorization") ?? "")),
    crypto.subtle.digest("SHA-256", encoder.encode(`Bearer ${secret}`)),
  ]);
  return crypto.subtle.timingSafeEqual(actual, expected);
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (!await authenticated(request, env.PUSH_TOKEN)) return new Response("Unauthorized", { status: 401 });
    if (!["/consumer", "/state"].includes(new URL(request.url).pathname)) return new Response("Not found", { status: 404 });
    // One named actor for this example. Route distinct names here for multiple consumers.
    return env.CONSUMERS.getByName("example").fetch(request);
  },
} satisfies ExportedHandler<Env>;
