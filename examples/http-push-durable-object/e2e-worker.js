import worker, { Consumer } from "./worker.js";

// Test instrumentation is separate from the deployed example.
export class ObservedConsumer extends Consumer {
  instance = crypto.randomUUID();

  async fetch(request) {
    const batch = request.method === "POST" ? await request.clone().json() : null;
    const response = await super.fetch(request);
    const decision = await this.env.EVIDENCE.fetch("http://evidence", {
      method: "POST",
      body: JSON.stringify({
        instance: this.instance,
        method: request.method,
        batch,
        status: response.status,
        streams: this.ctx.storage.sql.exec("SELECT * FROM streams").toArray(),
      }),
    });
    if (decision.status === 503) return new Response("Injected lost acknowledgement", { status: 503 });
    return response;
  }
}

export default worker;
