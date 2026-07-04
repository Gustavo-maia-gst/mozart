import {
  type IpcFrame,
  type NodeId,
  type PushContracts,
  type PushType,
  type RpcContracts,
  type RpcMethod,
  rpcPayloadSchemas,
} from '@mozart/contracts';
import type { ZodType } from 'zod';
import { type FrameChannel, newFrame } from './frame-channel';
import { type IpcHooks, resolveHooks } from './hooks';

/** Master-side RPC handlers, invoked with the originating node id. */
export type RpcHandlers = {
  [M in RpcMethod]: (nodeId: NodeId, payload: RpcContracts[M]['req']) => Promise<RpcContracts[M]['res']>;
};

/**
 * Master's view of one forked slave: receives `req` frames and answers them,
 * and pushes deliveries/lifecycle events. One instance per node; on restart a
 * fresh `NodeLink` is bound to the new child under the same `nodeId`.
 */
export class NodeLink {
  private readonly hooks: Required<IpcHooks>;

  constructor(
    readonly nodeId: NodeId,
    private readonly channel: FrameChannel,
    private readonly handlers: RpcHandlers,
    hooks?: IpcHooks,
  ) {
    this.hooks = resolveHooks(hooks);
    this.channel.onMessage((frame) => this.onFrame(frame));
  }

  push<T extends PushType>(type: T, payload: PushContracts[T]): boolean {
    const frame = newFrame({ kind: 'push', method: type, payload });
    this.hooks.injectTraceCtx(frame.traceCtx);
    return this.channel.send(frame);
  }

  onClose(cb: () => void): void {
    this.channel.onClose(cb);
  }

  get alive(): boolean {
    return this.channel.alive;
  }

  private onFrame(frame: IpcFrame): void {
    if (frame.kind !== 'req' || !frame.method) return;
    const method = frame.method as RpcMethod;
    const schema: ZodType | undefined = rpcPayloadSchemas[method];
    if (!schema) {
      this.sendError(frame, 'unknown_method', `unknown RPC method: ${frame.method}`);
      return;
    }

    const parsed = schema.safeParse(frame.payload);
    if (!parsed.success) {
      this.sendError(frame, 'invalid_payload', `invalid payload for ${method}`);
      return;
    }

    const handler = this.handlers[method] as (nodeId: NodeId, payload: unknown) => Promise<unknown>;

    this.hooks
      .runWithTraceCtx(frame.traceCtx, () => handler(this.nodeId, parsed.data))
      .then((res) => this.sendResult(frame, res))
      .catch((err: unknown) => {
        const message = err instanceof Error ? err.message : String(err);
        const code = err && typeof err === 'object' && 'code' in err ? String(err.code) : 'handler_error';
        this.sendError(frame, code, message);
      });
  }

  private sendResult(req: IpcFrame, payload: unknown): void {
    this.channel.send(newFrame({ kind: 'res', correlId: req.frameId, ok: true, payload }));
  }

  private sendError(req: IpcFrame, code: string, message: string): void {
    this.channel.send(
      newFrame({
        kind: 'res',
        correlId: req.frameId,
        ok: false,
        payload: { error: { code, message } },
      }),
    );
  }
}
