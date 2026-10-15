import { DurableObject } from "cloudflare:workers";

export class Consumer extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS streams (
      stream TEXT PRIMARY KEY, next_offset TEXT NOT NULL, processed TEXT NOT NULL
    )`);
  }

  async fetch(request) {
    if (request.method === "GET") {
      return Response.json(this.ctx.storage.sql.exec("SELECT * FROM streams").toArray());
    }
    let batch;
    try {
      batch = await request.json();
      if (batch.version !== 1 || typeof batch.group_id !== "string" ||
          typeof batch.topic !== "string" || !Number.isInteger(batch.partition) ||
          batch.partition < 0 || !Array.isArray(batch.records) || !batch.records.length ||
          typeof batch.next_offset !== "string" || !/^\d+$/.test(batch.next_offset)) {
        throw new Error("Invalid batch");
      }
      let previous = -1n;
      for (const record of batch.records) {
        if (typeof record.offset !== "string" || !/^\d+$/.test(record.offset) ||
            BigInt(record.offset) <= previous || BigInt(record.offset) >= BigInt(batch.next_offset)) {
          throw new Error("Invalid offsets");
        }
        previous = BigInt(record.offset);
      }
    } catch {
      return new Response("Invalid delivery", { status: 400 });
    }
    const stream = JSON.stringify([batch.group_id, batch.topic, batch.partition]);
    this.ctx.storage.transactionSync(() => {
      const stored = this.ctx.storage.sql.exec(
        "SELECT next_offset, processed FROM streams WHERE stream = ?", stream,
      ).toArray()[0];
      let nextOffset = BigInt(stored?.next_offset ?? "0");
      let processed = BigInt(stored?.processed ?? "0");
      for (const record of batch.records) {
        if (BigInt(record.offset) < nextOffset) continue;
        // Replace the counter with local SQL effects inside this transaction.
        processed++;
      }
      if (BigInt(batch.next_offset) > nextOffset) nextOffset = BigInt(batch.next_offset);
      this.ctx.storage.sql.exec(
        `INSERT INTO streams VALUES (?, ?, ?) ON CONFLICT(stream) DO UPDATE SET
         next_offset = excluded.next_offset, processed = excluded.processed`,
        stream, nextOffset.toString(), processed.toString(),
      );
    });
    return new Response(null, { status: 204 });
  }
}

export default {
  async fetch(request, env) {
    if (!env.PUSH_TOKEN || request.headers.get("Authorization") !== `Bearer ${env.PUSH_TOKEN}`) {
      return new Response("Unauthorized", { status: 401 });
    }
    const match = /^\/consumers\/([a-zA-Z0-9_-]+)$/.exec(new URL(request.url).pathname);
    if (!match) return new Response("Not found", { status: 404 });
    if (!["POST", "GET"].includes(request.method)) {
      return new Response("Method not allowed", { status: 405 });
    }
    return env.CONSUMERS.getByName(match[1]).fetch(request);
  },
};
