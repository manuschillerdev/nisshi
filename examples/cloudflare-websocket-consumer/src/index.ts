import { DurableObject } from "cloudflare:workers";

type Delivery = {
  version: 1;
  type: "records";
  delivery_id: string;
  group_id: string;
  topic: string;
  partition: number;
  next_offset: string;
  records: Array<{
    offset: string;
    timestamp: string;
    key: string | null;
    value: string | null;
    headers: Array<{ key: string | null; value: string | null }>;
  }>;
};

type Progress = { next_offset: string; count: string };
type Pending = Pick<Delivery, "group_id" | "topic" | "partition" | "next_offset" | "records">;
type RpcResult = Record<string, unknown>;
type Waiter = {
  socket: WebSocket;
  resolve: (value: RpcResult) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
};

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
    if (typeof record.timestamp !== "string" || !/^-?\d{1,19}$/.test(record.timestamp)
      || BigInt(record.timestamp) < -9223372036854775808n || BigInt(record.timestamp) > 9223372036854775807n
      || !(record.key === null || typeof record.key === "string")
      || !(record.value === null || typeof record.value === "string") || !Array.isArray(record.headers)
      || !record.headers.every((header: unknown) => typeof header === "object" && header !== null
        && "key" in header && (header.key === null || typeof header.key === "string")
        && "value" in header && (header.value === null || typeof header.value === "string"))) return false;
    const offset = BigInt(record.offset);
    if (offset <= previous || offset >= next) return false;
    previous = offset;
  }
  return true;
}

export class Consumer extends DurableObject<Env> {
  private requests = new Map<string, Waiter>();
  private processing: Promise<void> = Promise.resolve();

  async fetch(request: Request): Promise<Response> {
    if (new URL(request.url).pathname === "/state") {
      return Response.json(Object.fromEntries(await this.ctx.storage.list({ prefix: "progress:" })));
    }
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("WebSocket upgrade required", { status: 426 });
    }
    if (this.ctx.getWebSockets().some((socket) => socket.readyState === WebSocket.OPEN)) {
      return new Response("This example accepts one subscription per actor", { status: 409 });
    }
    const [client, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server);
    // A lost commit response can leave a local receipt pending even though
    // Kafka committed. Reconcile on reconnect without requiring another record.
    this.processing = this.reconcile(server).then(() => {}).catch(() => server.close(1011, "Recovery failed"));
    this.ctx.waitUntil(this.processing);
    return new Response(null, { status: 101, webSocket: client });
  }

  protected request(socket: WebSocket, method: string, params: RpcResult = {}): Promise<RpcResult> {
    const request_id = crypto.randomUUID();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.requests.delete(request_id);
        reject(new Error("Broker request timed out"));
        socket.close(1011, "Broker request timed out");
      }, 10_000);
      this.requests.set(request_id, { socket, resolve, reject, timer });
      try {
        socket.send(JSON.stringify({ version: 1, type: "request", request_id, method, params }));
      } catch (error) {
        clearTimeout(timer);
        this.requests.delete(request_id);
        reject(error instanceof Error ? error : new Error("Socket send failed"));
      }
    });
  }

  private async recordCommit(pending: Pending): Promise<void> {
    const key = `progress:${JSON.stringify([pending.group_id, pending.topic, pending.partition])}`;
    await this.ctx.storage.transaction(async (transaction) => {
      const progress = await transaction.get<Progress>(key);
      const cursor = BigInt(progress?.next_offset ?? "0");
      let count = BigInt(progress?.count ?? "0");
      for (const record of pending.records) if (BigInt(record.offset) >= cursor) count++;
      await transaction.put(key, {
        next_offset: (BigInt(pending.next_offset) > cursor ? BigInt(pending.next_offset) : cursor).toString(),
        count: count.toString(),
      } satisfies Progress);
      await transaction.delete("pending");
    });
  }

  private async reconcile(socket: WebSocket): Promise<string> {
    const result = await this.request(socket, "offset_fetch");
    if (!isOffset(result.next_offset)) throw new Error("Invalid broker offset");
    const pending = await this.ctx.storage.get<Pending>("pending");
    if (pending && BigInt(result.next_offset) >= BigInt(pending.next_offset)) {
      await this.recordCommit(pending);
    }
    return result.next_offset;
  }

  private async process(socket: WebSocket, delivery: Delivery): Promise<void> {
    // Initialization can finish a previous prepared commit. Fetch the offset
    // afterwards so that recovery cannot produce that delivery's output twice.
    const producer = await this.request(socket, "init_producer_id");
    if (typeof producer.producer_id !== "string" || !Number.isInteger(producer.producer_epoch)) {
      throw new Error("Invalid producer identity");
    }
    const committed = await this.reconcile(socket);
    if (BigInt(committed) >= BigInt(delivery.next_offset)) {
      socket.close(1012, "Commit recovered");
      return;
    }
    // This receipt is only a local projection. The durable application effect
    // is the Kafka output, committed atomically with the input offset below.
    await this.ctx.storage.put("pending", delivery);
    const identity = { producer_id: producer.producer_id, producer_epoch: producer.producer_epoch };
    const destination = { topic: this.env.OUTPUT_TOPIC, partition: 0 };
    await this.request(socket, "add_partitions_to_txn", { ...identity, ...destination });
    await this.request(socket, "produce", {
      ...identity, ...destination, base_sequence: 0,
      records: delivery.records.map(({ timestamp, key, value, headers }) => ({ timestamp, key, value, headers })),
    });
    await this.request(socket, "add_offsets_to_txn", identity);
    await this.request(socket, "txn_offset_commit", {
      ...identity, delivery_id: delivery.delivery_id, next_offset: delivery.next_offset,
    });
    await this.request(socket, "end_txn", { ...identity, committed: true });
    await this.recordCommit(delivery);
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
    if (typeof delivery === "object" && delivery !== null && "type" in delivery && delivery.type === "response") {
      if (!("request_id" in delivery) || typeof delivery.request_id !== "string") {
        socket.close(1008, "Invalid response");
        return;
      }
      const waiter = this.requests.get(delivery.request_id);
      if (!waiter || waiter.socket !== socket) return;
      clearTimeout(waiter.timer);
      this.requests.delete(delivery.request_id);
      if (!("version" in delivery) || delivery.version !== 1 || !("error_code" in delivery) || delivery.error_code !== 0
        || !("result" in delivery) || typeof delivery.result !== "object" || delivery.result === null || Array.isArray(delivery.result)) {
        waiter.reject(new Error("Broker request failed"));
      } else {
        waiter.resolve(delivery.result as RpcResult);
      }
      return;
    }
    if (!isDelivery(delivery)) {
      socket.close(1008, "Invalid delivery");
      return;
    }
    const records = delivery;
    const processing = this.processing.then(() => this.process(socket, records));
    this.processing = processing.catch(() => socket.close(1011, "Processing failed"));
    await this.processing;
  }

  webSocketClose(socket: WebSocket, code: number, reason: string): void {
    for (const [id, waiter] of this.requests) {
      if (waiter.socket !== socket) continue;
      clearTimeout(waiter.timer);
      this.requests.delete(id);
      waiter.reject(new Error("Broker disconnected"));
    }
    socket.close(code, reason);
  }

  webSocketError(socket: WebSocket): void {
    this.webSocketClose(socket, 1011, "Socket error");
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
