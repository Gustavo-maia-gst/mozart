import { existsSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { Scenario } from '@mozart/contracts';
import { Test } from '@nestjs/testing';
import { afterAll, afterEach, beforeEach, describe, expect, it } from 'vitest';
import { CoreModule } from '../src/core/core.module';
import { EventLogService } from '../src/event-log/event-log.service';
import { IpcServerModule } from '../src/ipc-server/ipc-server.module';
import { NodeRegistry } from '../src/ipc-server/node-registry';
import { ProcessManagerService } from '../src/ipc-server/process-manager.service';
import { MetricsModule } from '../src/metrics/metrics.module';
import { StorageService } from '../src/storage/storage.service';

const distReady = existsSync(join(__dirname, '..', '..', '..', 'packages', 'ipc', 'dist', 'index.js'));
const fixture = join(__dirname, 'fixture-node.cjs');
const logDir = 'runs/__pmtest__';

const scenario = new Scenario({
  name: 'pm',
  seed: '1',
  protocol: 'baseline',
  nodes: [{ id: 'n1' }],
  graphs: [{ id: 'g0', tasks: [{ id: 't1', dependsOn: [] }] }],
  storage: { adapter: 'in-memory' },
  transport: { ackTimeoutMs: 2000 },
  latency: {},
  faults: [],
  endCondition: { type: 'timeout', ms: 5000 },
});

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

describe.runIf(distReady)('ProcessManagerService (real forks)', () => {
  let moduleRef: Awaited<ReturnType<typeof buildModule>>;
  let pm: ProcessManagerService;
  let storage: StorageService;
  let registry: NodeRegistry;

  async function buildModule() {
    const m = await Test.createTestingModule({
      imports: [
        CoreModule.forRun({
          scenario,
          runId: 'pm-test',
          env: {
            MOZART_PG_URL: '',
            OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: '',
            OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: '',
            MOZART_LOG_DIR: logDir,
            MOZART_SLAVE_ENTRYPOINT: fixture,
          },
        }),
        MetricsModule,
        IpcServerModule,
      ],
    }).compile();
    m.get(EventLogService).open();
    return m;
  }

  beforeEach(async () => {
    moduleRef = await buildModule();
    pm = moduleRef.get(ProcessManagerService);
    storage = moduleRef.get(StorageService);
    registry = moduleRef.get(NodeRegistry);
  });

  afterEach(async () => {
    await pm.shutdown(1000);
    await moduleRef.close();
  });

  afterAll(() => rmSync(logDir, { recursive: true, force: true }));

  it('spawns, reaches readiness, force-releases locks on crash, and restarts', async () => {
    pm.spawnAll();
    await pm.awaitAllReady(5000);
    expect(registry.liveNodeIds()).toEqual(['n1']);

    // On startup the fixture acquires (and holds) an exclusive lock.
    await sleep(300);
    expect(storage.heldLeaseCount()).toBe(1);

    // Crash it: onExit must force-release the held lock.
    pm.kill('n1');
    await sleep(300);
    expect(storage.heldLeaseCount()).toBe(0);
    expect(registry.get('n1')).toBeUndefined();

    // Restart under the same nodeId; the link is rebound.
    pm.restart('n1');
    await pm.awaitAllReady(5000);
    expect(registry.liveNodeIds()).toEqual(['n1']);
  });
});
