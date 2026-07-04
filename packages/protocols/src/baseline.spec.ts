import {
  type Delivery,
  type Graph,
  PROTOCOL_LOGGER,
  STORAGE_PORT,
  type StoragePort,
  type TaskState,
  TRANSPORT_PORT,
  type TransportPort,
  WORKER_POOL_PORT,
  WORKER_TOPICS,
  type WorkerPoolPort,
} from '@mozart/contracts';
import { Test } from '@nestjs/testing';
import { beforeEach, describe, expect, it } from 'vitest';
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

const store = new Map<string, TaskState>();
const started: string[] = [];

const storage: StoragePort = {
  read: (id) => Promise.resolve(store.get(id) ?? null),
  find: () => Promise.resolve([]),
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

async function makeProtocol(): Promise<BaselineProtocol> {
  const moduleRef = await Test.createTestingModule({
    providers: [
      BaselineProtocol,
      { provide: STORAGE_PORT, useValue: storage },
      { provide: TRANSPORT_PORT, useValue: transport },
      { provide: WORKER_POOL_PORT, useValue: workers },
      { provide: PROTOCOL_LOGGER, useValue: { debug() {}, info() {}, warn() {}, error() {} } },
    ],
  }).compile();
  return moduleRef.get(BaselineProtocol);
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
  beforeEach(() => {
    store.clear();
    started.length = 0;
  });

  it('injects its ports via Nest DI (no context blob)', async () => {
    const p = await makeProtocol();
    // If property injection failed, persistGraph would throw on `this.storage`.
    await expect(p.persistGraph(graph)).resolves.toBeUndefined();
    expect(store.get('graph:g0')).toEqual({ graph });
  });

  it('persists then starts only the roots', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    await p.startGraph('g0');
    expect(started).toEqual(['a']);
  });

  it('drives the DAG in dependency order from completion events', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    await p.startGraph('g0');

    await p.onMessage(completed('a')); // unlocks b and c
    expect(started).toEqual(['a', 'b', 'c']);
    await p.onMessage(completed('b')); // d still needs c
    expect(started).toEqual(['a', 'b', 'c']);
    await p.onMessage(completed('c')); // now d is ready
    expect(started).toEqual(['a', 'b', 'c', 'd']);
  });

  it('is idempotent: a duplicated completion does not double-start dependents', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    await p.startGraph('g0');
    await p.onMessage(completed('a'));
    await p.onMessage(completed('a')); // duplicate / redelivery
    expect(started).toEqual(['a', 'b', 'c']);
  });
});
