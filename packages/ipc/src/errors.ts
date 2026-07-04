/** A remote handler on the master rejected an RPC. */
export class RpcRemoteError extends Error {
  constructor(
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = 'RpcRemoteError';
  }
}

/** The IPC channel died (peer gone) while a call was outstanding. */
export class IpcChannelClosedError extends Error {
  constructor(message = 'IPC channel closed') {
    super(message);
    this.name = 'IpcChannelClosedError';
  }
}

/** A call exceeded its optional timeout. */
export class RpcTimeoutError extends Error {
  constructor(method: string, timeoutMs: number) {
    super(`RPC ${method} timed out after ${timeoutMs}ms`);
    this.name = 'RpcTimeoutError';
  }
}
