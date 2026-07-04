import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { parse } from 'yaml';
import { scenarioSchema } from './scenario';

const scenariosDir = join(__dirname, '..', '..', '..', 'scenarios');

describe('scenarioSchema', () => {
  it('parses baseline.yaml', () => {
    const raw: unknown = parse(readFileSync(join(scenariosDir, 'baseline.yaml'), 'utf8'));
    const scenario = scenarioSchema.parse(raw);

    expect(scenario.name).toBe('baseline');
    expect(scenario.seed).toBe('1'); // number coerced to string
    expect(scenario.protocol).toBe('baseline');
    expect(scenario.nodes.map((n) => n.id)).toEqual(['n1']);
    expect(scenario.dag.tasks.map((t) => t.id)).toEqual(['a', 'b', 'c', 'd']);
    expect(scenario.dag.tasks[3]?.dependsOn).toEqual(['b', 'c']);
    expect(scenario.transport.ackTimeoutMs).toBe(2000);
  });

  it('applies defaults for optional sections', () => {
    const scenario = scenarioSchema.parse({
      name: 'minimal',
      seed: 1,
      protocol: 'echo',
      nodes: [{ id: 'n1' }],
      dag: { tasks: [{ id: 't1' }] },
      storage: { adapter: 'in-memory' },
      endCondition: { type: 'timeout', ms: 1000 },
    });

    expect(scenario.transport.ackTimeoutMs).toBe(2000);
    expect(scenario.latency).toEqual({});
    expect(scenario.faults).toEqual([]);
    expect(scenario.dag.tasks[0]?.dependsOn).toEqual([]);
  });

  it('rejects an unknown fault action', () => {
    expect(() =>
      scenarioSchema.parse({
        name: 'bad',
        seed: 1,
        protocol: 'echo',
        nodes: [{ id: 'n1' }],
        dag: { tasks: [{ id: 't1' }] },
        storage: { adapter: 'in-memory' },
        faults: [{ action: 'meteorStrike', at: 0 }],
        endCondition: { type: 'timeout', ms: 1000 },
      }),
    ).toThrow();
  });
});
