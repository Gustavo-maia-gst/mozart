import { describe, expect, it, vi } from 'vitest';
import type { IpcFrame, RpcMethod } from '@mozart/contracts';
import type { FrameChannel } from './frame-channel';
import { IpcClient } from './ipc-client';
import { NodeLink, type RpcHandlers } from './node-link';
import { IpcChannelClosedError, RpcRemoteError, RpcTimeoutError } from './errors';

/** Two FrameChannels wired to each other, faithful to the JSON wire boundary. */
function linkedChannels(): { client: FrameChannel; host: FrameChannel; close: () => void } {
  const msg = { client: [] as ((f: IpcFrame) => void)[], host: [] as ((f: IpcFrame) => void)[] };
  const close = { client: [] as (() => void)[], host: [] as (() => void)[] };
  const alive = { client: true, host: true };

  const wire = (self: 'client' | 'host', peer: 'client' | 'host'): FrameChannel => ({
    send(frame) {
      if (!alive[self] || !alive[peer]) return false;
      const copy = JSON.parse(JSON.stringify(frame)) as IpcFrame;
      queueMicrotask(() => msg[peer].forEach((cb) => cb(copy)));
      return true;
    },
    onMessage(cb) {
      msg[self].push(cb);
    },
    onClose(cb) {
      close[self].push(cb);
    },
    get alive() {
      return alive[self];
    },
  });

  return {
    client: wire('client', 'host'),
    host: wire('host', 'client'),
    close: () => {
      alive.client = false;
      alive.host = false;
      close.client.forEach((cb) => cb());
      close.host.forEach((cb) => cb());
    },
  };
}

function makeHandlers(overrides: Partial<RpcHandlers>): RpcHandlers {
  const methods: RpcMethod[] = [
    'node.ready',
    'transport.publish',
    'transport.ack',
    'storage.read',
    'storage.readExclusive',
    'storage.save',
    'storage.lease.save',
    'storage.lease.release',
    'worker.start',
  ];
  const base = {} as Record<RpcMethod, (n: string, p: unknown) => Promise<unknown>>;
  for (const m of methods) {
    base[m] = () => Promise.reject(new Error(`not implemented: ${m}`));
  }
  return { ...base, ...overrides } as RpcHandlers;
}

describe('IpcClient <-> NodeLink RPC', () => {
  it('round-trips a request/response with the origin node id', async () => {
    const { client, host } = linkedChannels();
    const readSpy = vi.fn((nodeId: string, p: { taskId: string }) =>
      Promise.resolve({ data: { who: nodeId, task: p.taskId } }),
    );
    new NodeLink('n1', host, makeHandlers({ 'storage.read': readSpy }));
    const c = new IpcClient(client);

    const res = await c.call('storage.read', { taskId: 't1' });
    expect(res).toEqual({ data: { who: 'n1', task: 't1' } });
    expect(readSpy).toHaveBeenCalledWith('n1', { taskId: 't1' });
  });

  it('propagates remote handler errors as RpcRemoteError', async () => {
    const { client, host } = linkedChannels();
    new NodeLink(
      'n1',
      host,
      makeHandlers({ 'worker.start': () => Promise.reject(new Error('boom')) }),
    );
    const c = new IpcClient(client);
    await expect(c.call('worker.start', { taskId: 't1' })).rejects.toBeInstanceOf(RpcRemoteError);
  });

  it('rejects invalid payloads at the host boundary', async () => {
    const { client, host } = linkedChannels();
    new NodeLink('n1', host, makeHandlers({}));
    const c = new IpcClient(client);
    // taskId must be a non-empty string.
    await expect(
      c.call('storage.read', { taskId: '' } as unknown as { taskId: string }),
    ).rejects.toMatchObject({ code: 'invalid_payload' });
  });

  it('delivers pushes to the client push handler', async () => {
    const { client, host } = linkedChannels();
    const link = new NodeLink('n1', host, makeHandlers({}));
    const c = new IpcClient(client);
    const received: unknown[] = [];
    c.onPush((type, payload) => {
      received.push({ type, payload });
    });

    link.push('protocol.activate', {});
    await new Promise((r) => setTimeout(r, 5));
    expect(received).toEqual([{ type: 'protocol.activate', payload: {} }]);
  });

  it('rejects outstanding calls when the channel closes', async () => {
    const { client, host, close } = linkedChannels();
    // Host handler that never resolves — simulates a storage outage / hang.
    new NodeLink('n1', host, makeHandlers({ 'storage.read': () => new Promise<never>(() => {}) }));
    const c = new IpcClient(client);

    const pending = c.call('storage.read', { taskId: 't1' });
    await new Promise((r) => setTimeout(r, 5));
    close();
    await expect(pending).rejects.toBeInstanceOf(IpcChannelClosedError);
  });

  it('honors a call timeout', async () => {
    const { client, host } = linkedChannels();
    new NodeLink('n1', host, makeHandlers({ 'storage.read': () => new Promise<never>(() => {}) }));
    const c = new IpcClient(client);
    await expect(
      c.call('storage.read', { taskId: 't1' }, { timeoutMs: 20 }),
    ).rejects.toBeInstanceOf(RpcTimeoutError);
  });

  it('runs host handlers within the extracted trace context', async () => {
    const { client, host } = linkedChannels();
    const seen: string[] = [];
    new NodeLink('n1', host, makeHandlers({ 'node.ready': () => Promise.resolve({ scenario: {} as never }) }), {
      runWithTraceCtx: (carrier, fn) => {
        seen.push(carrier.marker ?? 'none');
        return fn();
      },
    });
    const c = new IpcClient(client, {
      injectTraceCtx: (carrier) => {
        carrier.marker = 'from-client';
      },
    });
    await c.call('node.ready', {});
    expect(seen).toEqual(['from-client']);
  });
});
