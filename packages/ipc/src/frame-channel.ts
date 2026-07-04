import type { ChildProcess } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { type IpcFrame, ipcFrameSchema } from '@mozart/contracts';

/**
 * A bidirectional frame pipe. Both endpoints (slave->parent, master->child)
 * are modeled the same way so `IpcClient` and `NodeLink` share one transport
 * abstraction over `child_process`'s built-in JSON IPC channel.
 */
export interface FrameChannel {
  /** Returns false if the channel is dead or the OS buffer is full. Never throws. */
  send(frame: IpcFrame): boolean;
  onMessage(cb: (frame: IpcFrame) => void): void;
  onClose(cb: () => void): void;
  readonly alive: boolean;
}

export interface FrameChannelOptions {
  /** Invoked when an inbound message fails frame-envelope validation. */
  onInvalidFrame?: (raw: unknown, error: unknown) => void;
}

function guardedSend(sender: (frame: IpcFrame) => boolean, frame: IpcFrame): boolean {
  try {
    return sender(frame);
  } catch (err) {
    // ERR_IPC_CHANNEL_CLOSED / EPIPE when the peer was just SIGKILLed.
    const code = (err as NodeJS.ErrnoException).code;
    if (code === 'ERR_IPC_CHANNEL_CLOSED' || code === 'EPIPE') {
      return false;
    }
    throw err;
  }
}

function attachMessageValidation(
  source: NodeJS.EventEmitter,
  opts: FrameChannelOptions | undefined,
  cb: (frame: IpcFrame) => void,
): void {
  source.on('message', (raw: unknown) => {
    const parsed = ipcFrameSchema.safeParse(raw);
    if (!parsed.success) {
      opts?.onInvalidFrame?.(raw, parsed.error);
      return;
    }
    cb(parsed.data);
  });
}

/** Slave-side channel to the parent (master) process. */
export function processFrameChannel(proc: NodeJS.Process = process, opts?: FrameChannelOptions): FrameChannel {
  return {
    send: (frame) => guardedSend((f) => (proc.send ? proc.send(f) : false), frame),
    onMessage: (cb) => attachMessageValidation(proc, opts, cb),
    onClose: (cb) => {
      proc.on('disconnect', cb);
    },
    get alive() {
      return proc.connected === true;
    },
  };
}

/** Master-side channel to a specific forked slave. */
export function childFrameChannel(child: ChildProcess, opts?: FrameChannelOptions): FrameChannel {
  return {
    send: (frame) => guardedSend((f) => child.send(f), frame),
    onMessage: (cb) => attachMessageValidation(child, opts, cb),
    onClose: (cb) => {
      // 'exit' fires even on SIGKILL; 'close' after stdio teardown. Use exit.
      child.once('exit', () => cb());
    },
    get alive() {
      return child.connected === true && !child.killed;
    },
  };
}

export function newFrame(parts: Omit<IpcFrame, 'frameId' | 'traceCtx' | 'sentAt'>): IpcFrame {
  return {
    frameId: randomUUID(),
    traceCtx: {},
    sentAt: Date.now(),
    ...parts,
  };
}
