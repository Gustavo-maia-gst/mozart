import {
  buildGraph,
  type ExclusiveRead,
  type Message,
  ProtocolLogger,
  type StorageQuery,
  StoragePort,
  type TaskMatch,
  type TaskState,
  TransportPort,
  type WorkerFailEvent,
  type WorkerSuccessEvent,
} from '@mozart/contracts';
import { Test } from '@nestjs/testing';
import { beforeEach, describe, expect, it } from 'vitest';
import { DependencyFrontierProtocol } from './dependency_frontier';

// Diamond DAG: a -> {b, c} -> d.
const graph = buildGraph('g0', [
  { id: 'a' },
  { id: 'b', dependsOn: ['a'] },
  { id: 'c', dependsOn: ['a'] },
  { id: 'd', dependsOn: ['b', 'c'] },
]);

/** In-memory S with the same match rule as the real adapter (scalar = eq, array = IN). */
const store = new Map<string, TaskState>();
const matches = (data: TaskState, query: StorageQuery): boolean =>
  Object.entries(query).every(([k, v]) => (Array.isArray(v) ? v.includes(data[k] as never) : data[k] === v));
const fakeStorage: StoragePort = {
  read: (id) => Promise.resolve(store.get(id) ?? null),
  find: (query: StorageQuery) => {
    const hits: TaskMatch[] = [];
    for (const [taskId, data] of store) if (matches(data, query)) hits.push({ taskId, data });
    return Promise.resolve(hits);
  },
  save: (id, data) => {
    store.set(id, data);
    return Promise.resolve();
  },
  readExclusive: (id): Promise<ExclusiveRead> =>
    Promise.resolve({
      data: store.get(id) ?? null,
      save: (data) => {
        store.set(id, data);
        return Promise.resolve();
      },
      release: () => Promise.resolve(),
    }),
};

const started: string[] = [];
let completedGraph: string | undefined;
const fakeTransport: TransportPort = {
  sendToWorkerPool: (t) => {
    started.push(t);
    return Promise.resolve();
  },
  sendToCoordinators: () => Promise.resolve(),
  completeGraph: (id) => {
    completedGraph = id;
    return Promise.resolve();
  },
};

async function makeProtocol(): Promise<DependencyFrontierProtocol> {
  const moduleRef = await Test.createTestingModule({
    providers: [
      DependencyFrontierProtocol,
      { provide: StoragePort, useValue: fakeStorage },
      { provide: TransportPort, useValue: fakeTransport },
      { provide: ProtocolLogger, useValue: { debug() {}, info() {}, warn() {}, error() {} } },
    ],
  }).compile();
  return moduleRef.get(DependencyFrontierProtocol);
}

const success = (taskId: string): WorkerSuccessEvent => ({ taskId });
const fail = (taskId: string): WorkerFailEvent => ({ taskId });

describe('DependencyFrontierProtocol', () => {
  beforeEach(() => {
    store.clear();
    started.length = 0;
    completedGraph = undefined;
  });

  it('persists each task with its edges (deps / dependents)', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    expect(store.get('a')).toMatchObject({ kind: 'task', deps: [], dependents: ['b', 'c'], depCount: 0 });
    expect(store.get('d')).toMatchObject({ deps: ['b', 'c'], dependents: [], depCount: 2 });
  });

  it('starts only the roots', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    await p.startGraph('g0');
    expect(started).toEqual(['a']);
  });

  it('advances the exact frontier: a starts b and c; d waits for both', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    await p.startGraph('g0');

    await p.onWorkerSuccess(success('a')); // unlocks b and c
    expect(started).toEqual(['a', 'b', 'c']);
    await p.onWorkerSuccess(success('b')); // d still needs c
    expect(started).toEqual(['a', 'b', 'c']);
    await p.onWorkerSuccess(success('c')); // now d's deps are all complete
    expect(started).toEqual(['a', 'b', 'c', 'd']);
  });

  it('completes the graph once the last (leaf) task finishes', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    await p.startGraph('g0');
    await p.onWorkerSuccess(success('a'));
    await p.onWorkerSuccess(success('b'));
    await p.onWorkerSuccess(success('c'));
    expect(completedGraph).toBeUndefined(); // d not done yet
    await p.onWorkerSuccess(success('d'));
    expect(completedGraph).toBe('g0');
  });

  it('is idempotent: a duplicated completion never starts a task with unmet deps', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    await p.startGraph('g0');
    await p.onWorkerSuccess(success('a'));
    await p.onWorkerSuccess(success('a')); // duplicate / redelivery
    await p.onWorkerSuccess(success('b'));
    await p.onWorkerSuccess(success('b')); // duplicate
    // d only ever starts after BOTH b and c complete — never from a duplicate.
    expect(started).not.toContain('d');
    expect(new Set(started)).toEqual(new Set(['a', 'b', 'c']));
  });

  it('retries a failed task by re-dispatching it', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    await p.startGraph('g0');
    await p.onWorkerFail(fail('a'));
    expect(started).toEqual(['a', 'a']); // re-dispatched
  });
});
