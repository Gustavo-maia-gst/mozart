import { fork } from 'node:child_process';
import { existsSync } from 'node:fs';
import { join } from 'node:path';
import type { RpcMethod } from '@mozart/contracts';
import { afterEach, describe, expect, it } from 'vitest';
import { childFrameChannel, NodeLink, type RpcHandlers } from '../src';

const distReady = existsSync(join(__dirname, '..', 'dist', 'index.js'));
const fixture = join(__dirname, 'fixture-slave.cjs');

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
  const base = {} as Record<RpcMethod, () => Promise<unknown>>;
  for (const m of methods) base[m] = () => Promise.reject(new Error(`unexpected ${m}`));
  return { ...base, ...overrides } as RpcHandlers;
}

describe.runIf(distReady)('IPC over a real fork', () => {
  let child: ReturnType<typeof fork> | undefined;
  afterEach(() => {
    child?.kill('SIGKILL');
    child = undefined;
  });

  it('round-trips RPC and detects the child crash mid-call', async () => {
    child = fork(fixture, [], { serialization: 'json' });

    let resolveReady: (taskId: string) => void;
    const ready = new Promise<string>((r) => (resolveReady = r));
    let closed = false;

    const link = new NodeLink(
      'n1',
      childFrameChannel(child),
      makeHandlers({
        'node.ready': () => Promise.resolve({ scenario: { nodeId: 'n1' } as never }),
        'worker.start': (_n, p) => {
          resolveReady(p.taskId);
          return Promise.resolve({});
        },
        // Never resolves: the child will hang here until we kill it.
        'storage.read': () => new Promise<never>(() => {}),
        'transport.ack': () => Promise.resolve({}),
      }),
    );
    link.onClose(() => {
      closed = true;
    });

    // 1. Real round trip over the fork.
    expect(await ready).toBe('ready:n1');
    expect(link.alive).toBe(true);

    // 2. Push a delivery; the child starts a storage.read that we never answer.
    link.push('delivery', {
      deliveryId: 'd1',
      messageId: 'm1',
      from: 'n2',
      topic: 'ping',
      body: {},
      attempt: 1,
      traceCtx: {},
    });
    await new Promise((r) => setTimeout(r, 50));

    // 3. SIGKILL mid-call; NodeLink must observe the exit.
    child.kill('SIGKILL');
    await new Promise((r) => setTimeout(r, 100));
    expect(closed).toBe(true);
    expect(link.alive).toBe(false);
  });
});
