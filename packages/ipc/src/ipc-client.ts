import type { IpcFrame, PushContracts, PushType, RpcContracts, RpcMethod } from '@mozart/contracts';
import { IpcChannelClosedError, RpcRemoteError, RpcTimeoutError } from './errors';
import { type FrameChannel, newFrame } from './frame-channel';
import { type IpcHooks, resolveHooks } from './hooks';

interface Pending {
  method: string;
  resolve: (value: unknown) => void;
  reject: (err: Error) => void;
  timer?: NodeJS.Timeout;
}

export type PushHandler = <T extends PushType>(
  type: T,
  payload: PushContracts[T],
  frame: IpcFrame,
) => void | Promise<void>;

export interface CallOptions {
  /**
   * Optional timeout. Storage calls deliberately pass none: a storage outage
   * must block the caller (crash-recovery model), not fail it.
   */
  timeoutMs?: number;
}

/**
 * Slave-side RPC client. Issues `req` frames to the master and resolves them
 * against `res` frames; dispatches inbound `push` frames to a handler.
 */
export class IpcClient {
  private readonly hooks: Required<IpcHooks>;
  private readonly pending = new Map<string, Pending>();
  private pushHandler?: PushHandler;
  private closed = false;

  constructor(
    private readonly channel: FrameChannel,
    hooks?: IpcHooks,
  ) {
    this.hooks = resolveHooks(hooks);
    this.channel.onMessage((frame) => this.dispatch(frame));
    this.channel.onClose(() => this.onClose());
  }

  onPush(handler: PushHandler): void {
    this.pushHandler = handler;
  }

  call<M extends RpcMethod>(
    method: M,
    payload: RpcContracts[M]['req'],
    opts?: CallOptions,
  ): Promise<RpcContracts[M]['res']> {
    return new Promise((resolve, reject) => {
      if (this.closed) {
        reject(new IpcChannelClosedError());
        return;
      }
      const frame = newFrame({ kind: 'req', method, payload });
      this.hooks.injectTraceCtx(frame.traceCtx);

      const pending: Pending = {
        method,
        resolve: resolve as (v: unknown) => void,
        reject,
      };
      if (opts?.timeoutMs !== undefined) {
        pending.timer = setTimeout(() => {
          this.pending.delete(frame.frameId);
          reject(new RpcTimeoutError(method, opts.timeoutMs!));
        }, opts.timeoutMs);
      }
      this.pending.set(frame.frameId, pending);

      if (!this.channel.send(frame)) {
        this.pending.delete(frame.frameId);
        if (pending.timer) clearTimeout(pending.timer);
        reject(new IpcChannelClosedError('send failed: channel not writable'));
      }
    });
  }

  private dispatch(frame: IpcFrame): void {
    if (frame.kind === 'res') {
      this.resolveResponse(frame);
    } else if (frame.kind === 'push') {
      this.dispatchPush(frame);
    }
    // slave never receives 'req'.
  }

  private resolveResponse(frame: IpcFrame): void {
    if (!frame.correlId) return;
    const pending = this.pending.get(frame.correlId);
    if (!pending) return; // already timed out / unknown
    this.pending.delete(frame.correlId);
    if (pending.timer) clearTimeout(pending.timer);

    if (frame.ok === false) {
      const err = (frame.payload as { error?: { code?: string; message?: string } })?.error;
      pending.reject(new RpcRemoteError(err?.code ?? 'unknown', err?.message ?? 'remote error'));
    } else {
      pending.resolve(frame.payload);
    }
  }

  private dispatchPush(frame: IpcFrame): void {
    if (!this.pushHandler || !frame.method) return;
    void Promise.resolve(this.pushHandler(frame.method as PushType, frame.payload as never, frame)).catch(() => {
      // Push handlers own their own error semantics (e.g. no ack => redelivery).
    });
  }

  private onClose(): void {
    this.closed = true;
    for (const [, pending] of this.pending) {
      if (pending.timer) clearTimeout(pending.timer);
      pending.reject(new IpcChannelClosedError());
    }
    this.pending.clear();
  }
}
