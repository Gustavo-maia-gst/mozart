import {
  type Delivery,
  type Graph,
  type ProtocolContext,
  type StoragePort,
  type TaskState,
  type TransportPort,
  WORKER_TOPICS,
  type WorkerPoolPort,
} from '@mozart/contracts';
import { describe, expect, it } from 'vitest';
import { BaselineProtocol } from './baseline';

// Diamond DAG: a -> {b, c} -> d.
const graph: Graph = {
  id: 'g0',
  tasks: [
    { id: 'a', dependsOn: [] },
    { id: 'b', dependsOn: ['a'] },
    { id: 'c', dependsOn: ['a'] },
    { id: 'd', dependsOn: ['b', 'c'] },
  ],
};

function makeCtx() {
  const store = new Map<string, TaskState>();
  const started: string[] = [];

  const storage: StoragePort = {
    read: (id) => Promise.resolve(store.get(id) ?? null),
    save: (id, d) => {
      store.set(id, d);
      return Promise.resolve();
    },
    readExclusive: () => Promise.reject(new Error('baseline does not lock')),
  };
  const transport: TransportPort = { publish: () => Promise.resolve() };
  const workers: WorkerPoolPort = {
    start: (t) => {
      started.push(t);
      return Promise.resolve();
    },
  };
  const ctx: ProtocolContext = {
    nodeId: 'n1',
    scenario: { runId: 'r', nodeId: 'n1', protocol: 'baseline', nodes: ['n1'], dag: { tasks: graph.tasks }, graphs: [graph] },
    transport,
    storage,
    workers,
    log: { debug() {}, info() {}, warn() {}, error() {} },
  };
  return { ctx, store, started };
}

function completed(taskId: string): Delivery {
  return {
    deliveryId: `d-${taskId}`,
    messageId: `m-${taskId}`,
    from: 'W',
    topic: WORKER_TOPICS.completed,
    body: { taskId },
    attempt: 1,
    traceCtx: {},
  };
}

describe('BaselineProtocol', () => {
  it('persists the graph and starts only the roots', async () => {
    const { ctx, store, started } = makeCtx();
    await new BaselineProtocol().onActivate(ctx);

    expect(store.get('graph:g0')).toEqual({ graph });
    expect(started).toEqual(['a']); // only the dependency-free task
  });

  it('drives the DAG in dependency order from completion events', async () => {
    const { ctx, started } = makeCtx();
    const p = new BaselineProtocol();
    await p.onActivate(ctx);

    await p.onMessage(completed('a')); // unlocks b and c
    expect(started).toEqual(['a', 'b', 'c']);

    await p.onMessage(completed('b')); // d still needs c
    expect(started).toEqual(['a', 'b', 'c']);

    await p.onMessage(completed('c')); // now d is ready
    expect(started).toEqual(['a', 'b', 'c', 'd']);
  });

  it('is idempotent: a duplicated completion does not double-start dependents', async () => {
    const { ctx, started } = makeCtx();
    const p = new BaselineProtocol();
    await p.onActivate(ctx);

    await p.onMessage(completed('a'));
    await p.onMessage(completed('a')); // duplicate / redelivery
    expect(started).toEqual(['a', 'b', 'c']); // b, c started once each
  });
});
