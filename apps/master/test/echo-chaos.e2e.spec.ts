import { existsSync, readFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { Test } from '@nestjs/testing';
import type { HarnessEvent, Scenario } from '@mozart/contracts';
import { initTelemetry, type Telemetry } from '@mozart/telemetry';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { CoreModule } from '../src/core/core.module';
import { RunModule } from '../src/run/run.module';
import { RunService } from '../src/run/run.service';

const repoRoot = join(__dirname, '..', '..', '..');
const slaveEntry = join(repoRoot, 'apps', 'slave', 'dist', 'main.js');
const distReady =
  existsSync(slaveEntry) && existsSync(join(repoRoot, 'packages', 'ipc', 'dist', 'index.js'));
const logDir = 'runs/__e2e__';

const scenario: Scenario = {
  name: 'echo-chaos',
  seed: '42',
  protocol: 'echo',
  nodes: [{ id: 'n1' }, { id: 'n2' }],
  dag: {
    tasks: [
      { id: 't1', dependsOn: [], costMs: 40 },
      { id: 't2', dependsOn: ['t1'], costMs: 40 },
    ],
  },
  storage: { adapter: 'in-memory' },
  transport: { ackTimeoutMs: 120 },
  latency: {
    'transport.deliver': { distribution: 'constant', value: 100 },
    'storage.read': { distribution: 'constant', value: 5 },
    'storage.readExclusive': { distribution: 'constant', value: 5 },
    'storage.save': { distribution: 'constant', value: 5 },
  },
  faults: [{ action: 'killNode', at: 40, node: 'n2', restartAfterMs: 250 }],
  endCondition: { type: 'timeout', ms: 1600 },
};

describe.runIf(distReady)('echo run under a kill+restart fault (e2e)', () => {
  // Master-side telemetry so event-log records carry a traceId (verifying
  // end-to-end propagation). Slaves init their own telemetry in their main.ts.
  let telemetry: Telemetry;
  beforeAll(() => {
    telemetry = initTelemetry({ serviceName: 'mozart-master-e2e' });
  });
  afterAll(async () => {
    await telemetry.shutdown();
    rmSync(logDir, { recursive: true, force: true });
  });

  it('survives a downed node: redelivers and acks after restart', async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [
        CoreModule.forRun({
          scenario,
          runId: 'e2e',
          env: {
            MOZART_PG_URL: '',
            OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: '',
            MOZART_LOG_DIR: logDir,
            MOZART_SLAVE_ENTRYPOINT: slaveEntry,
          },
        }),
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

    // Both nodes came up, the fault fired, and n2 was killed then restarted.
    expect(new Set(ofType('node.ready').map((e) => e.nodeId))).toEqual(new Set(['n1', 'n2']));
    expect(ofType('fault.injected').some((e) => e.data?.action === 'killNode')).toBe(true);
    expect(ofType('node.killed').map((e) => e.nodeId)).toContain('n2');
    expect(ofType('node.exited').find((e) => e.nodeId === 'n2')?.data?.injected).toBe(true);
    expect(ofType('node.restarted').map((e) => e.nodeId)).toContain('n2');

    // The ping to the downed node was redelivered and eventually acked.
    const redelivN2 = ofType('transport.redelivered').filter((e) => e.channel === 'n1->n2');
    expect(redelivN2.length).toBeGreaterThanOrEqual(1);
    const firstRedeliverSeq = Math.min(...redelivN2.map((e) => e.seq));
    const ackedN2After = ofType('transport.acked').filter(
      (e) => e.channel === 'n1->n2' && e.seq > firstRedeliverSeq,
    );
    expect(ackedN2After.length).toBeGreaterThanOrEqual(1);

    expect(ofType('run.finished')).toHaveLength(1);

    // Trace coherence: every message's published/delivered/acked share one traceId.
    const byMessage = new Map<string, Set<string>>();
    for (const e of events) {
      if (!e.messageId || !e.traceId) continue;
      (byMessage.get(e.messageId) ?? byMessage.set(e.messageId, new Set()).get(e.messageId)!).add(
        e.traceId,
      );
    }
    expect(byMessage.size).toBeGreaterThan(0);
    for (const [, traceIds] of byMessage) expect(traceIds.size).toBe(1);
  });
});
