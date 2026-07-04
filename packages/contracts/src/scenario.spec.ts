import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { parse } from 'yaml';
import { Scenario, scenarioSchema } from './scenario';

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
    expect(scenario.graphs.map((g) => g.tasks.map((t) => t.id))).toEqual([['g0-a', 'g0-b'], ['g1-a']]);
    // Same local id `a` in two graphs stays distinct once namespaced.
    expect(scenario.graphs[0]?.tasks[1]?.dependsOn).toEqual(['g0-a']);
  });

  it('exposes coordinator ids and names', () => {
    const scenario = new Scenario(data);
    expect(scenario.coordinatorIds()).toEqual(['n1']);
    expect(scenario.nodeName('n1')).toBe('coordinator');
    expect(scenario.nodeName('unknown')).toBe('unknown'); // falls back to id
  });
});
