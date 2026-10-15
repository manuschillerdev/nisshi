import worker, { Consumer } from "../src/index";

function record(event: string, fields: Record<string, unknown>): void {
  console.log(`NISSHI_E2E ${JSON.stringify({ event, ...fields })}`);
}

// Lifecycle evidence stays in the test subclass; the example's handlers do the work.
export class ObservedConsumer extends Consumer {
  private instanceId = crypto.randomUUID();
  private phase = "";
  private dropCommitResponse = false;

  protected async request(socket: WebSocket, method: string, params: Record<string, unknown> = {}): Promise<Record<string, unknown>> {
    if (method === "end_txn" && params.committed === true) {
      if (this.phase === "abort-output-once" && !await this.ctx.storage.get("aborted-once")) {
        await this.ctx.storage.put("aborted-once", true);
        const result = await super.request(socket, method, { ...params, committed: false });
        record("injected_abort", { instance_id: this.instanceId });
        socket.close(1012, "Retry aborted transaction");
        throw new Error(`Injected abort: ${JSON.stringify(result)}`);
      }
      if (this.phase === "lost-commit-response" && !await this.ctx.storage.get("dropped-commit-once")) {
        this.dropCommitResponse = true;
      }
    }
    try {
      return await super.request(socket, method, params);
    } finally {
      if (method === "end_txn") this.dropCommitResponse = false;
    }
  }

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    record("constructor", {
      instance_id: this.instanceId,
      attachments: this.ctx.getWebSockets().map((socket) => socket.deserializeAttachment()),
    });
  }

  async fetch(request: Request): Promise<Response> {
    const response = await super.fetch(request);
    if (response.status === 101) {
      const socket = this.ctx.getWebSockets().find((socket) => !socket.deserializeAttachment());
      if (!socket) throw new Error("Missing accepted socket");
      const connection_id = crypto.randomUUID();
      socket.serializeAttachment({ connection_id });
      record("upgrade", { instance_id: this.instanceId, connection_id });
    }
    return response;
  }

  async webSocketMessage(socket: WebSocket, message: string | ArrayBuffer): Promise<void> {
    if (typeof message !== "string") throw new Error("Expected JSON text");
    const delivery = JSON.parse(message);
    const identity = { instance_id: this.instanceId, ...socket.deserializeAttachment() };
    if (delivery.type === "response") {
      if (this.dropCommitResponse && delivery.error_code === 0) {
        await this.ctx.storage.put("dropped-commit-once", true);
        record("dropped_commit_response", identity);
        socket.close(1012, "Lost commit response");
        return;
      }
      record("response", { ...identity, response: delivery });
      await super.webSocketMessage(socket, message);
      return;
    }
    this.phase = JSON.parse(atob(delivery.records[0].value)).phase;
    record("message", { ...identity, delivery });
    await super.webSocketMessage(socket, message);
    const progress = await this.ctx.storage.get<{ next_offset: string }>(`progress:${JSON.stringify([delivery.group_id, delivery.topic, delivery.partition])}`);
    if (progress && BigInt(progress.next_offset) >= BigInt(delivery.next_offset)) {
      record("processed", { ...identity, next_offset: delivery.next_offset });
    }
  }
}

export default worker;
