import {
  buildGraph,
  ProtocolLogger,
  StoragePort,
  type TaskState,
  TransportPort,
  type WorkerSuccessEvent,
} from '@mozart/contracts';
import { Test } from '@nestjs/testing';
import { beforeEach, describe, expect, it } from 'vitest';
import { BaselineProtocol } from './baseline';

// Diamond DAG: a -> {b, c} -> d.
const graph = buildGraph('g0', [
  { id: 'a' },
  { id: 'b', dependsOn: ['a'] },
  { id: 'c', dependsOn: ['a'] },
  { id: 'd', dependsOn: ['b', 'c'] },
]);

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
  delete: () => Promise.resolve(0),
};
// The baseline dispatches tasks via the transport's sendToWorkerPool; record
// the task ids it hands off so the tests can assert the DAG ordering.
const transport: TransportPort = {
  sendToWorkerPool: (t) => {
    started.push(t);
    return Promise.resolve();
  },
  sendToCoordinators: () => Promise.resolve(),
  completeGraph: () => Promise.resolve(),
};

async function makeProtocol(): Promise<BaselineProtocol> {
  const moduleRef = await Test.createTestingModule({
    providers: [
      BaselineProtocol,
      { provide: StoragePort, useValue: storage },
      { provide: TransportPort, useValue: transport },
      { provide: ProtocolLogger, useValue: { debug() {}, info() {}, warn() {}, error() {} } },
    ],
  }).compile();
  return moduleRef.get(BaselineProtocol);
}

function completed(taskId: string): WorkerSuccessEvent {
  return { taskId };
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
    expect(store.get('graph:g0')).toEqual({ graph: graph.export() });
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

    await p.onWorkerSuccess(completed('a')); // unlocks b and c
    expect(started).toEqual(['a', 'b', 'c']);
    await p.onWorkerSuccess(completed('b')); // d still needs c
    expect(started).toEqual(['a', 'b', 'c']);
    await p.onWorkerSuccess(completed('c')); // now d is ready
    expect(started).toEqual(['a', 'b', 'c', 'd']);
  });

  it('is idempotent: a duplicated completion does not double-start dependents', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    await p.startGraph('g0');
    await p.onWorkerSuccess(completed('a'));
    await p.onWorkerSuccess(completed('a')); // duplicate / redelivery
    expect(started).toEqual(['a', 'b', 'c']);
  });
});
