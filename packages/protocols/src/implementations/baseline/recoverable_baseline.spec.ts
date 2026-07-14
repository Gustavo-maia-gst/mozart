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
import { RecoverableBaselineProtocol } from './recoverable_baseline';

// Diamond DAG: a -> {b, c} -> d.
const graph = buildGraph('g0', [
  { id: 'a' },
  { id: 'b', dependsOn: ['a'] },
  { id: 'c', dependsOn: ['a'] },
  { id: 'd', dependsOn: ['b', 'c'] },
]);

// The durable store survives a "restart" (a fresh protocol instance reuses it).
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

/** A fresh protocol instance over the shared durable store — models a restart. */
async function makeProtocol(): Promise<RecoverableBaselineProtocol> {
  const moduleRef = await Test.createTestingModule({
    providers: [
      RecoverableBaselineProtocol,
      { provide: StoragePort, useValue: fakeStorage },
      { provide: TransportPort, useValue: fakeTransport },
      { provide: ProtocolLogger, useValue: { debug() {}, info() {}, warn() {}, error() {} } },
    ],
  }).compile();
  return moduleRef.get(RecoverableBaselineProtocol);
}

const success = (taskId: string): WorkerSuccessEvent => ({ taskId });
const fail = (taskId: string): WorkerFailEvent => ({ taskId });

describe('RecoverableBaselineProtocol', () => {
  beforeEach(() => {
    store.clear();
    started.length = 0;
    completedGraph = undefined;
  });

  it('drives the DAG in dependency order and completes the graph', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    await p.startGraph('g0');
    expect(started).toEqual(['a']);
    await p.onWorkerSuccess(success('a'));
    expect(started).toEqual(['a', 'b', 'c']);
    await p.onWorkerSuccess(success('b'));
    expect(completedGraph).toBeUndefined();
    await p.onWorkerSuccess(success('c'));
    await p.onWorkerSuccess(success('d'));
    expect(completedGraph).toBe('g0');
  });

  it('logs a write-ahead entry per action (running before dispatch, complete before advancing)', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    await p.startGraph('g0');
    expect(store.get('a')).toMatchObject({ kind: 'tasklog', status: 'running' });
    await p.onWorkerSuccess(success('a'));
    expect(store.get('a')).toMatchObject({ status: 'complete' });
    expect(store.get('b')).toMatchObject({ status: 'running' });
  });

  it('is idempotent: a duplicated completion does not double-advance', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    await p.startGraph('g0');
    await p.onWorkerSuccess(success('a'));
    await p.onWorkerSuccess(success('a')); // duplicate
    expect(started).toEqual(['a', 'b', 'c']);
  });

  it('recovers in-flight state after a restart and finishes the graph', async () => {
    const p1 = await makeProtocol();
    await p1.persistGraph(graph);
    await p1.startGraph('g0'); // dispatch a
    await p1.onWorkerSuccess(success('a')); // a complete → dispatch b, c

    // Crash + stateless restart: a brand-new instance, only the durable log survives.
    started.length = 0;
    const p2 = await makeProtocol();
    await p2.onWorkerSuccess(success('b')); // first touch → rebuild frontier from the log

    // Recovery replays a=complete, then re-dispatches the ready frontier {b, c}.
    expect(started).toEqual(['b', 'c']);
    await p2.onWorkerSuccess(success('c')); // b + c done → d ready
    expect(started).toEqual(['b', 'c', 'd']);
    await p2.onWorkerSuccess(success('d'));
    expect(completedGraph).toBe('g0');
  });

  it('signals completion on restart if every task was already logged complete', async () => {
    const p1 = await makeProtocol();
    await p1.persistGraph(graph);
    await p1.startGraph('g0');
    for (const t of ['a', 'b', 'c', 'd'] as const) await p1.onWorkerSuccess(success(t));
    completedGraph = undefined; // pretend the pre-crash completeGraph never reached the master

    const p2 = await makeProtocol();
    await p2.startGraph('g0'); // reload → all complete → re-signal
    expect(completedGraph).toBe('g0');
  });

  it('retries a failed task by re-dispatching it', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    await p.startGraph('g0');
    await p.onWorkerFail(fail('a'));
    expect(started).toEqual(['a', 'a']);
  });
});
