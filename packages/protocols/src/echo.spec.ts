import { describe, expect, it } from 'vitest';
import type {
  Delivery,
  ProtocolContext,
  StoragePort,
  TaskState,
  TransportPort,
  WorkerPoolPort,
} from '@mozart/contracts';
import { EchoProtocol } from './echo';

function makeCtx(tasks: { id: string; dependsOn: string[] }[] = [{ id: 't1', dependsOn: [] }]) {
  const store = new Map<string, TaskState>();
  const published: { to: string; topic: string; body: unknown }[] = [];
  const started: string[] = [];

  const storage: StoragePort = {
    read: (id) => Promise.resolve(store.get(id) ?? null),
    save: (id, d) => {
      store.set(id, d);
      return Promise.resolve();
    },
    readExclusive: (id) =>
      Promise.resolve({
        data: store.get(id) ?? null,
        save: (d) => {
          store.set(id, d);
          return Promise.resolve();
        },
        release: () => Promise.resolve(),
      }),
  };
  const transport: TransportPort = {
    publish: (to, topic, body) => {
      published.push({ to, topic, body });
      return Promise.resolve();
    },
  };
  const workers: WorkerPoolPort = {
    start: (t) => {
      started.push(t);
      return Promise.resolve();
    },
  };
  const ctx: ProtocolContext = {
    nodeId: 'n1',
    scenario: { runId: 'r', nodeId: 'n1', protocol: 'echo', nodes: ['n1', 'n2'], dag: { tasks } },
    transport,
    storage,
    workers,
    log: { debug() {}, info() {}, warn() {}, error() {} },
  };
  return { ctx, store, published, started };
}

function ping(messageId: string): Delivery {
  return {
    deliveryId: `d-${messageId}`,
    messageId,
    from: 'n2',
    topic: 'ping',
    body: {},
    attempt: 1,
    traceCtx: {},
  };
}

describe('EchoProtocol', () => {
  it('on activate, greets storage, pings peers, and kicks off the DAG', async () => {
    const { ctx, store, published, started } = makeCtx();
    await new EchoProtocol().onActivate(ctx);

    expect(store.get('echo:n1')).toMatchObject({ hello: 'n1', pings: 0 });
    expect(published).toEqual([{ to: 'n2', topic: 'ping', body: { from: 'n1' } }]);
    expect(started).toEqual(['t1']); // n1 is nodes[0]
  });

  it('is idempotent: a duplicated ping produces exactly one pong', async () => {
    const { ctx, store, published } = makeCtx();
    const echo = new EchoProtocol();
    await echo.onMessage(ping('m1'), ctx);
    await echo.onMessage(ping('m1'), ctx); // duplicate/redelivery

    const pongs = published.filter((p) => p.topic === 'pong');
    expect(pongs).toHaveLength(1);
    expect((store.get('echo:n1') as { pings: number }).pings).toBe(1);
  });

  it('counts distinct pings and pongs each once', async () => {
    const { ctx, store, published } = makeCtx();
    const echo = new EchoProtocol();
    await echo.onMessage(ping('m1'), ctx);
    await echo.onMessage(ping('m2'), ctx);

    expect(published.filter((p) => p.topic === 'pong')).toHaveLength(2);
    expect((store.get('echo:n1') as { pings: number }).pings).toBe(2);
  });

  it('advances the DAG when a task completes', async () => {
    const { ctx, started } = makeCtx([
      { id: 't1', dependsOn: [] },
      { id: 't2', dependsOn: ['t1'] },
    ]);
    await new EchoProtocol().onMessage(
      {
        deliveryId: 'd',
        messageId: 'c1',
        from: 'W',
        topic: 'task.completed',
        body: { taskId: 't1' },
        attempt: 1,
        traceCtx: {},
      },
      ctx,
    );
    expect(started).toEqual(['t2']);
  });
});
