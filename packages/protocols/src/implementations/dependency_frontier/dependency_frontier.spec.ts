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

/** Drain queued `task.finished` messages back into the protocol (what the transport would deliver). */
async function pump(p: DependencyFrontierProtocol): Promise<void> {
  for (const m of messages.splice(0)) await p.onMessage({ topic: m.topic, body: m.body as never });
}

describe('DependencyFrontierProtocol', () => {
  beforeEach(() => {
    store.clear();
    started.length = 0;
    messages.length = 0;
    completedGraph = undefined;
  });

  it('starts only the roots', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    await p.startGraph('g0');
    expect(started).toEqual(['a']);
  });

  it('advances the exact frontier: a frees b and c; d waits for both', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    await p.startGraph('g0');

    await p.onWorkerSuccess(success('a')); // emits task.finished(a)
    await pump(p);
    expect(started).toEqual(['a', 'b', 'c']);
    await p.onWorkerSuccess(success('b'));
    await pump(p);
    expect(started).toEqual(['a', 'b', 'c']); // d still needs c
    await p.onWorkerSuccess(success('c'));
    await pump(p);
    expect(started).toEqual(['a', 'b', 'c', 'd']);
  });

  it('completes the graph once every task is done', async () => {
    const p = await makeProtocol();
    await p.persistGraph(graph);
    await p.startGraph('g0');
    for (const t of ['a', 'b', 'c'] as const) {
      await p.onWorkerSuccess(success(t));
      await pump(p);
    }
    expect(completedGraph).toBeUndefined();
    await p.onWorkerSuccess(success('d'));
    await pump(p);
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
