import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { parse } from 'yaml';
import { describe, expect, it } from 'vitest';
import { scenarioSchema } from './scenario';

const scenariosDir = join(__dirname, '..', '..', '..', 'scenarios');

describe('scenarioSchema', () => {
  it('parses echo-smoke.yaml', () => {
    const raw: unknown = parse(readFileSync(join(scenariosDir, 'echo-smoke.yaml'), 'utf8'));
    const scenario = scenarioSchema.parse(raw);

    expect(scenario.name).toBe('echo-smoke');
    expect(scenario.seed).toBe('42'); // number coerced to string
    expect(scenario.nodes.map((n) => n.id)).toEqual(['n1', 'n2']);
    expect(scenario.dag.tasks[1]?.dependsOn).toEqual(['t1']);
    expect(scenario.latency['worker.taskDuration']).toEqual({
      distribution: 'lognormal',
      mu: 6.9,
      sigma: 0.5,
    });
    expect(scenario.faults[0]).toMatchObject({ action: 'killNode', node: 'n2' });
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
