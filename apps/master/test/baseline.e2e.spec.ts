import { existsSync, readFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { type HarnessEvent, Scenario } from '@mozart/contracts';
import { initTelemetry, type Telemetry } from '@mozart/telemetry';
import { Test } from '@nestjs/testing';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { CoreModule } from '../src/core/core.module';
import { MetricsModule } from '../src/metrics/metrics.module';
import { RunModule } from '../src/run/run.module';
import { RunService } from '../src/run/run.service';

const repoRoot = join(__dirname, '..', '..', '..');
const slaveEntry = join(repoRoot, 'apps', 'slave', 'dist', 'main.js');
const distReady =
  existsSync(slaveEntry) && existsSync(join(repoRoot, 'packages', 'protocols', 'dist', 'index.js'));
const logDir = 'runs/__e2e_baseline__';

// Diamond DAG: a -> {b, c} -> d (runtime ids are namespaced to g0-*).
const scenario = new Scenario({
  name: 'baseline',
  seed: '1',
  protocol: 'baseline',
  nodes: [{ id: 'n1' }],
  graphs: [
    {
      id: 'g0',
      tasks: [
        { id: 'a', dependsOn: [], costMs: 40 },
        { id: 'b', dependsOn: ['a'], costMs: 40 },
        { id: 'c', dependsOn: ['a'], costMs: 40 },
        { id: 'd', dependsOn: ['b', 'c'], costMs: 40 },
      ],
    },
  ],
  storage: { adapter: 'in-memory' },
  transport: { ackTimeoutMs: 2000 },
  latency: {
    'transport.deliver': { distribution: 'constant', value: 10 },
    'storage.read': { distribution: 'constant', value: 5 },
    'storage.save': { distribution: 'constant', value: 5 },
  },
  faults: [],
  endCondition: { type: 'timeout', ms: 1500 },
});

describe.runIf(distReady)('baseline drives a DAG end-to-end (e2e)', () => {
  let telemetry: Telemetry;
  beforeAll(() => {
    telemetry = initTelemetry({ serviceName: 'mozart-master-e2e' });
  });
  afterAll(async () => {
    await telemetry.shutdown();
    rmSync(logDir, { recursive: true, force: true });
  });

  it('persists the graph and completes every task in dependency order', async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [
        CoreModule.forRun({
          scenario,
          runId: 'e2e-baseline',
          env: {
            MOZART_PG_URL: '',
            OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: '',
            OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: '',
            MOZART_LOG_DIR: logDir,
            MOZART_SLAVE_ENTRYPOINT: slaveEntry,
          },
        }),
        MetricsModule,
        RunModule,
      ],
    }).compile();

    const summary = await moduleRef.get(RunService).run();
    await moduleRef.close();

    const events: HarnessEvent[] = readFileSync(summary.logPath, 'utf8')
      .trim()
      .split('\n')
      .map((l) => JSON.parse(l) as HarnessEvent);
    const ofType = (t: string) => events.filter((e) => e.type === t);

    // The graph was persisted and read back by the protocol.
    expect(ofType('storage.save').length).toBeGreaterThanOrEqual(1);
    expect(ofType('storage.read').length).toBeGreaterThanOrEqual(1);

    // Every task completed exactly once (runtime ids are namespaced per graph).
    const completedTasks = ofType('worker.completed').map((e) => e.taskId);
    expect(new Set(completedTasks)).toEqual(new Set(['g0-a', 'g0-b', 'g0-c', 'g0-d']));

    // Dependency order: d started only after both b and c completed.
    const seqOf = (type: string, taskId: string) =>
      events.find((e) => e.type === type && e.taskId === taskId)?.seq ?? Infinity;
    const dStarted = seqOf('worker.started', 'g0-d');
    expect(dStarted).toBeGreaterThan(seqOf('worker.completed', 'g0-b'));
    expect(dStarted).toBeGreaterThan(seqOf('worker.completed', 'g0-c'));

    expect(ofType('run.finished')).toHaveLength(1);
  });
});
