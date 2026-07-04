import { readFileSync, rmSync } from 'node:fs';
import type { Scenario } from '@mozart/contracts';
import { Test } from '@nestjs/testing';
import { afterAll, describe, expect, it } from 'vitest';
import { CoreModule } from '../core/core.module';
import { RunModule } from './run.module';
import { RunService } from './run.service';

const scenario: Scenario = {
  name: 'unit',
  seed: '1',
  protocol: 'baseline',
  nodes: [{ id: 'n1' }, { id: 'n2' }],
  dag: { tasks: [{ id: 't1', dependsOn: [] }] },
  storage: { adapter: 'in-memory' },
  transport: { ackTimeoutMs: 2000 },
  latency: {},
  faults: [],
  endCondition: { type: 'timeout', ms: 1000 },
};

const logDir = 'runs/__test__';

describe('RunService (skeleton)', () => {
  afterAll(() => rmSync(logDir, { recursive: true, force: true }));

  it('records run.started/run.finished and writes a JSONL log', async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [
        CoreModule.forRun({
          scenario,
          runId: 'unit-run',
          env: {
            MOZART_PG_URL: '',
            OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: '',
            MOZART_LOG_DIR: logDir,
          },
        }),
        RunModule,
      ],
    }).compile();

    const summary = await moduleRef.get(RunService).run({ dryRun: true });

    expect(summary.runId).toBe('unit-run');
    expect(summary.events).toEqual({ 'run.started': 1, 'run.finished': 1 });

    const lines = readFileSync(summary.logPath, 'utf8').trim().split('\n');
    const events = lines.map((l) => JSON.parse(l) as { seq: number; type: string });
    expect(events.map((e) => e.type)).toEqual(['run.started', 'run.finished']);
    expect(events.map((e) => e.seq)).toEqual([0, 1]); // monotonic total order

    await moduleRef.close();
  });
});
