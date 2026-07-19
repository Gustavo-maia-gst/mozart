import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { parse } from 'yaml';
import { faultSchema, Scenario, scenarioSchema } from './scenario';

const scenariosDir = join(__dirname, '..', '..', '..', 'scenarios');

describe('scenarioSchema', () => {
  it('parses baseline.yaml', () => {
    const raw: unknown = parse(readFileSync(join(scenariosDir, 'baseline.yaml'), 'utf8'));
    const data = scenarioSchema.parse(raw);

    expect(data.name).toBe('baseline');
    expect(data.seed).toBe('1'); // number coerced to string
    expect(data.protocol).toBe('baseline');
    expect(data.nodes.map((n) => n.id)).toEqual(['n1']);
    expect(data.graphs[0]?.tasks.map((t) => t.id)).toEqual(['a', 'b', 'c', 'd']);
    expect(data.graphs[0]?.tasks[3]?.dependsOn).toEqual(['b', 'c']);
    expect(data.transport.ackTimeoutMs).toBe(2000);
  });

  it('applies defaults for optional sections', () => {
    const data = scenarioSchema.parse({
      name: 'minimal',
      seed: 1,
      protocol: 'baseline',
      nodes: [{ id: 'n1' }],
      graphs: [{ id: 'g0', tasks: [{ id: 't1' }] }],
      storage: { adapter: 'in-memory' },
      endCondition: { type: 'timeout', ms: 1000 },
    });

    expect(data.transport.ackTimeoutMs).toBe(2000);
    expect(data.latency).toEqual({});
    expect(data.faults).toEqual([]);
    expect(data.graphs[0]?.tasks[0]?.dependsOn).toEqual([]);
  });

  it('expands a numeric node count into n<i>/node<Alpha> coordinators', () => {
    const data = scenarioSchema.parse({
      name: 'counted',
      seed: 1,
      protocol: 'pull',
      nodes: 28,
      graphs: [{ id: 'g0', tasks: [{ id: 't1' }] }],
      storage: { adapter: 'in-memory' },
      endCondition: { type: 'timeout', ms: 1000 },
    });

    expect(data.nodes[0]).toEqual({ id: 'n1', name: 'nodeA' });
    expect(data.nodes[2]).toEqual({ id: 'n3', name: 'nodeC' });
    expect(data.nodes[25]).toEqual({ id: 'n26', name: 'nodeZ' });
    expect(data.nodes[27]).toEqual({ id: 'n28', name: 'nodeAB' }); // spreadsheet wrap
  });

  it('rejects an unknown fault action', () => {
    expect(() =>
      scenarioSchema.parse({
        name: 'bad',
        seed: 1,
        protocol: 'baseline',
        nodes: [{ id: 'n1' }],
        graphs: [{ id: 'g0', tasks: [{ id: 't1' }] }],
        storage: { adapter: 'in-memory' },
        faults: [{ action: 'meteorStrike', at: 0 }],
        endCondition: { type: 'timeout', ms: 1000 },
      }),
    ).toThrow();
  });
});

describe('faultSchema — conditionalKill', () => {
  it('parses a minimal dynamic key into the canonical shape, applying defaults', () => {
    const data = faultSchema.parse({ failAfterWorkerSuccessMessage: "message.taskId === 'g0-b'" });
    expect(data).toEqual({
      action: 'conditionalKill',
      phase: 'after',
      hook: 'WorkerSuccessMessage',
      filter: "message.taskId === 'g0-b'",
      restartAfterMs: 500,
      times: 1,
    });
  });

  it('parses "before" and honours explicit restartAfterMs/times', () => {
    const data = faultSchema.parse({
      failBeforeStorageSave: "message.taskId === 'g1-c'",
      restartAfterMs: 1000,
      times: 3,
    });
    expect(data).toMatchObject({ action: 'conditionalKill', phase: 'before', hook: 'StorageSave', times: 3 });
    expect((data as { restartAfterMs: number }).restartAfterMs).toBe(1000);
  });

  it('still parses the existing timed faults unchanged', () => {
    expect(faultSchema.parse({ action: 'killNode', at: 0, node: 'n1' })).toEqual({
      action: 'killNode',
      at: 0,
      node: 'n1',
    });
  });

  it('rejects a filter with invalid syntax', () => {
    expect(() => faultSchema.parse({ failAfterStorageSave: 'message.taskId ===' })).toThrow();
  });

  it('rejects a scenario with neither a fail key nor a recognized action', () => {
    expect(() => faultSchema.parse({ restartAfterMs: 500 })).toThrow();
  });

  it('rejects two dynamic fail keys on the same entry', () => {
    expect(() => faultSchema.parse({ failAfterStorageSave: 'true', failBeforeStorageSave: 'true' })).toThrow();
  });

  it('rejects an unrecognized hook name', () => {
    expect(() => faultSchema.parse({ failAfterBogusHook: 'true' })).toThrow();
  });

  it('accepts a conditionalKill fault inside a full scenario', () => {
    const data = scenarioSchema.parse({
      name: 'chaos',
      seed: 1,
      protocol: 'baseline',
      nodes: [{ id: 'n1' }],
      graphs: [{ id: 'g0', tasks: [{ id: 't1' }] }],
      storage: { adapter: 'in-memory' },
      faults: [{ failAfterWorkerSuccessMessage: "message.taskId === 'g0-t1'" }],
      endCondition: { type: 'timeout', ms: 1000 },
    });
    expect(data.faults[0]).toMatchObject({ action: 'conditionalKill', hook: 'WorkerSuccessMessage' });
  });
});

describe('Scenario', () => {
  const data = scenarioSchema.parse({
    name: 'ns',
    seed: 1,
    protocol: 'baseline',
    nodes: [{ id: 'n1', name: 'coordinator' }],
    graphs: [
      { id: 'g0', tasks: [{ id: 'a' }, { id: 'b', dependsOn: ['a'] }] },
      { id: 'g1', tasks: [{ id: 'a' }] },
    ],
    storage: { adapter: 'in-memory' },
    endCondition: { type: 'timeout', ms: 1000 },
  });

  it('namespaces task ids and dependsOn as <graphId>-<taskId>', () => {
    const scenario = new Scenario(data);
    expect(scenario.graphs.map((g) => g.nodes())).toEqual([['g0-a', 'g0-b'], ['g1-a']]);
    // Same local id `a` in two graphs stays distinct once namespaced. Edges run
    // dep -> dependent, so `g0-b`'s dependency is its in-neighbour `g0-a`.
    expect(scenario.graphs[0]?.inNeighbors('g0-b')).toEqual(['g0-a']);
    expect(scenario.graphs.map((g) => g.getAttribute('id'))).toEqual(['g0', 'g1']);
  });

  it('exposes coordinator ids and names', () => {
    const scenario = new Scenario(data);
    expect(scenario.coordinatorIds()).toEqual(['n1']);
    expect(scenario.nodeName('n1')).toBe('coordinator');
    expect(scenario.nodeName('unknown')).toBe('unknown'); // falls back to id
  });

  const multiNode = (protocol: string) =>
    scenarioSchema.parse({
      name: 'ns',
      seed: 1,
      protocol,
      nodes: [{ id: 'n1' }, { id: 'n2' }, { id: 'n3' }],
      graphs: [{ id: 'g0', tasks: [{ id: 'a' }] }],
      storage: { adapter: 'in-memory' },
      endCondition: { type: 'timeout', ms: 1000 },
    });

  it('collapses to a single coordinator for the centralized baseline', () => {
    // Baseline keeps its frontier in memory, so extra declared nodes are ignored.
    expect(new Scenario(multiNode('baseline')).coordinatorIds()).toEqual(['n1']);
  });

  it('keeps all declared coordinators for distributed protocols', () => {
    expect(new Scenario(multiNode('topological-barrier')).coordinatorIds()).toEqual(['n1', 'n2', 'n3']);
  });
});
