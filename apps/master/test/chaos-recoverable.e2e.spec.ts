import { existsSync, readFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { type HarnessEvent, Scenario, scenarioSchema } from '@mozart/contracts';
import { initTelemetry, type Telemetry } from '@mozart/telemetry';
import { Test } from '@nestjs/testing';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { CoreModule } from '../src/core/core.module';
import { MetricsModule } from '../src/metrics/metrics.module';
import { RunModule } from '../src/run/run.module';
import { RunService } from '../src/run/run.service';

const repoRoot = join(__dirname, '..', '..', '..');
const slaveEntry = join(repoRoot, 'apps', 'slave', 'dist', 'main.js');
const distReady = existsSync(slaveEntry) && existsSync(join(repoRoot, 'packages', 'protocols', 'dist', 'index.js'));
const logDir = 'runs/__e2e_chaos_recoverable__';

// Same diamond DAG as the baseline e2e spec: a -> {b, c} -> d. The coordinator
// is killed right as it's about to receive b's completion (before the push is
// even delivered — deterministic, no process-timing race) and restarts; the
// write-ahead log lets it resume where it left off (see
// scenarios/chaos/kill-after-success-recoverable.yaml for the full narrative).
//
// Parsed through `scenarioSchema` (rather than built as `ScenarioData`
// directly) so the test also exercises the real YAML-facing dynamic-key
// syntax and its transform into the canonical `conditionalKill` fault.
const scenario = new Scenario(
  scenarioSchema.parse({
    name: 'chaos-recoverable',
    seed: '1',
    protocol: 'baseline-recoverable',
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
    transport: { ackTimeoutMs: 500 },
    latency: {
      'transport.deliver': { distribution: 'constant', value: 10 },
      'storage.read': { distribution: 'constant', value: 5 },
      'storage.save': { distribution: 'constant', value: 5 },
      'storage.find': { distribution: 'constant', value: 5 },
    },
    faults: [
      {
        failBeforeWorkerSuccessMessage: "message.taskId === 'g0-b'",
        restartAfterMs: 300,
        times: 1,
      },
    ],
    endCondition: { type: 'timeout', ms: 3000 },
  }),
);

describe.runIf(distReady)('conditional fault crashes and recovers a coordinator (e2e)', () => {
  let telemetry: Telemetry;
  beforeAll(() => {
    telemetry = initTelemetry({ serviceName: 'mozart-master-e2e' });
  });
  afterAll(async () => {
    await telemetry.shutdown();
    rmSync(logDir, { recursive: true, force: true });
  });

  it('kills the coordinator on the filtered message and completes after restart', async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [
        CoreModule.forRun({
          scenario,
          runId: 'e2e-chaos-recoverable',
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

    // The conditional fault fired exactly once, on the filtered node.
    const injected = ofType('fault.injected');
    expect(injected).toHaveLength(1);
    expect(injected[0]?.data).toMatchObject({ action: 'conditionalKill', hook: 'WorkerSuccessMessage', phase: 'before' });
    expect(injected[0]?.nodeId).toBe('n1');
    expect(ofType('node.killed')).toHaveLength(1);
    expect(ofType('node.restarted')).toHaveLength(1);

    // Despite the crash, the write-ahead log lets the coordinator resume: every
    // task completes and the graph reaches completion (not the timeout cap).
    const completedTasks = ofType('worker.completed').map((e) => e.taskId);
    expect(new Set(completedTasks)).toEqual(new Set(['g0-a', 'g0-b', 'g0-c', 'g0-d']));
    expect(ofType('graph.completed').length).toBeGreaterThanOrEqual(1);
    expect(ofType('run.finished')).toHaveLength(1);
  });
});
