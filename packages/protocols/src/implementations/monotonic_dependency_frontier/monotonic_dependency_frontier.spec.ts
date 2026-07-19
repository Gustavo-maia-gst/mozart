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
import { MonotonicDependencyFrontierProtocol } from './monotonic_dependency_frontier';

// Diamond DAG: a -> {b, c} -> d.
const graph = buildGraph('g0', [
  { id: 'a' },
  { id: 'b', dependsOn: ['a'] },
  { id: 'c', dependsOn: ['a'] },
  { id: 'd', dependsOn: ['b', 'c'] },
]);

/** In-memory S with the real match rule (scalar = eq, array = IN) and delete-by-WHERE. */
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
  delete: (query: StorageQuery) => {
    let n = 0;
    for (const [id, data] of store)
      if (matches(data, query)) {
        store.delete(id);
        n++;
      }
    return Promise.resolve(n);
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

async function makeProtocol(): Promise<MonotonicDependencyFrontierProtocol> {
  const moduleRef = await Test.createTestingModule({
    providers: [
      MonotonicDependencyFrontierProtocol,
      { provide: StoragePort, useValue: fakeStorage },
      { provide: TransportPort, useValue: fakeTransport },
      { provide: ProtocolLogger, useValue: { debug() {}, info() {}, warn() {}, error() {} } },
    ],
  }).compile();
  return moduleRef.get(MonotonicDependencyFrontierProtocol);
}

const success = (taskId: string): WorkerSuccessEvent => ({ taskId });
const fail = (taskId: string): WorkerFailEvent => ({ taskId });

describe('MonotonicDependencyFrontierProtocol', () => {
  beforeEach(() => {
    store.clear();
    started.length = 0;
    completedGraph = undefined;
  });

  it('persists tasks (with dependents) and one edge record per dependency', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    expect(store.get('a')).toMatchObject({ kind: 'task', dependents: ['b', 'c'], status: 'pending' });
    expect(store.get('d')).toMatchObject({ dependents: [] });
    // Dependency edges dep -> dependent, keyed edge:src->tgt.
    expect(store.get('edge:a->b')).toEqual({ kind: 'edge', graphId: 'g0', source: 'a', target: 'b' });
    expect(store.get('edge:b->d')).toMatchObject({ source: 'b', target: 'd' });
  });

  it('starts only the roots (tasks no edge targets)', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    await p.startGraph('g0');
    expect(started).toEqual(['a']);
  });

  it('advances the exact frontier: a frees b and c; d waits for both', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    await p.startGraph('g0');

    await p.onWorkerSuccess(success('a')); // consumes a->b, a->c → both unblocked
    expect(started).toEqual(['a', 'b', 'c']);
    await p.onWorkerSuccess(success('b')); // consumes b->d, but c->d still blocks d
    expect(started).toEqual(['a', 'b', 'c']);
    await p.onWorkerSuccess(success('c')); // consumes c->d → d unblocked
    expect(started).toEqual(['a', 'b', 'c', 'd']);
  });

  it('consumes edges as sources complete', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    await p.startGraph('g0');
    await p.onWorkerSuccess(success('a'));
    expect(store.has('edge:a->b')).toBe(false);
    expect(store.has('edge:a->c')).toBe(false);
    expect(store.has('edge:b->d')).toBe(true); // b not done yet
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
    expect(started).not.toContain('d'); // d needs both b and c
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
