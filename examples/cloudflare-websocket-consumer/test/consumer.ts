import worker, { Consumer } from "../src/index";

function record(event: string, fields: Record<string, unknown>): void {
  console.log(`NISSHI_E2E ${JSON.stringify({ event, ...fields })}`);
}

// Lifecycle evidence stays in the test subclass; the example's handlers do the work.
export class ObservedConsumer extends Consumer {
  private instanceId = crypto.randomUUID();

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
    record("message", { ...identity, delivery });
    await super.webSocketMessage(socket, message);
    record("processed", { ...identity, next_offset: delivery.next_offset });
  }
}

export default worker;
