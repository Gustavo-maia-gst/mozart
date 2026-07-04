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
import { TopologicalBarrierProtocol } from './topologic_barrier';

// Diamond DAG: a -> {b, c} -> d. Topological orders: a=0, b=c=1, d=2.
const graph = buildGraph('g0', [
  { id: 'a' },
  { id: 'b', dependsOn: ['a'] },
  { id: 'c', dependsOn: ['a'] },
  { id: 'd', dependsOn: ['b', 'c'] },
]);

/** In-memory S: equality-filter `find`, plus a trivial (non-blocking) exclusive read. */
const store = new Map<string, TaskState>();
const fakeStorage: StoragePort = {
  read: (id) => Promise.resolve(store.get(id) ?? null),
  find: (query: StorageQuery) => {
    const hits: TaskMatch[] = [];
    for (const [taskId, data] of store) {
      if (Object.entries(query).every(([k, v]) => data[k] === v)) hits.push({ taskId, data });
    }
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

// Record dispatches to W and coordinator messages so tests can assert ordering.
const started: string[] = [];
const messages: { topic: string; body: unknown }[] = [];
let completedGraph: string | undefined;
const fakeTransport: TransportPort = {
  sendToWorkerPool: (t) => {
    started.push(t);
    return Promise.resolve();
  },
  sendToCoordinators: (topic, body) => {
    messages.push({ topic, body });
    return Promise.resolve();
  },
  completeGraph: (id) => {
    completedGraph = id;
    return Promise.resolve();
  },
};

async function makeProtocol(): Promise<TopologicalBarrierProtocol> {
  const moduleRef = await Test.createTestingModule({
    providers: [
      TopologicalBarrierProtocol,
      { provide: StoragePort, useValue: fakeStorage },
      { provide: TransportPort, useValue: fakeTransport },
      { provide: ProtocolLogger, useValue: { debug() {}, info() {}, warn() {}, error() {} } },
    ],
  }).compile();
  return moduleRef.get(TopologicalBarrierProtocol);
}

const success = (taskId: string): WorkerSuccessEvent => ({ taskId });
const fail = (taskId: string): WorkerFailEvent => ({ taskId });
const advance = (body: unknown): Message => ({ topic: 'barrier.advance', body });

/** Drain queued advance messages back into the protocol (what the transport would deliver). */
async function pump(p: TopologicalBarrierProtocol): Promise<void> {
  for (const m of messages.splice(0)) await p.onMessage(advance(m.body));
}

describe('TopologicalBarrierProtocol', () => {
  beforeEach(() => {
    store.clear();
    started.length = 0;
    messages.length = 0;
    completedGraph = undefined;
  });

  it('persists each task with its topological order', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    expect(store.get('a')).toEqual({ kind: 'task', graphId: 'g0', order: 0, status: 'pending' });
    expect(store.get('b')).toMatchObject({ order: 1 });
    expect(store.get('c')).toMatchObject({ order: 1 });
    expect(store.get('d')).toMatchObject({ order: 2 });
  });

  it('starts only order 0, then advances a whole level at a time', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    await p.startGraph('g0');
    expect(started).toEqual(['a']); // only the root level

    await p.onWorkerSuccess(success('a')); // order 0 done → advance to order 1
    await pump(p);
    expect(started).toEqual(['a', 'b', 'c']); // both order-1 tasks start together

    await p.onWorkerSuccess(success('b')); // order 1 not done yet (c pending)
    await pump(p);
    expect(started).toEqual(['a', 'b', 'c']); // barrier holds d back

    await p.onWorkerSuccess(success('c')); // order 1 complete → advance to order 2
    await pump(p);
    expect(started).toEqual(['a', 'b', 'c', 'd']);
  });

  it('completes the graph exactly once after the last order', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    await p.startGraph('g0');
    await p.onWorkerSuccess(success('a'));
    await pump(p);
    await p.onWorkerSuccess(success('b'));
    await p.onWorkerSuccess(success('c'));
    await pump(p);
    await p.onWorkerSuccess(success('d')); // last order done → advance to empty order 3
    await pump(p);
    expect(completedGraph).toBe('g0');
  });

  it('is idempotent under a duplicated completion: never starts the next order early', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    await p.startGraph('g0');
    await p.onWorkerSuccess(success('a'));
    await p.onWorkerSuccess(success('a')); // duplicate / redelivery re-drives the barrier
    await pump(p);
    // A duplicate re-dispatches the already-running order-1 tasks (W dedupes
    // those), but the barrier still holds d back: order 2 never starts early.
    expect(new Set(started)).toEqual(new Set(['a', 'b', 'c']));
    expect(started).not.toContain('d');
  });

  it('retries a failed task by re-dispatching it', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    await p.startGraph('g0');
    await p.onWorkerFail(fail('a'));
    expect(started).toEqual(['a', 'a']); // re-dispatched
  });
});
