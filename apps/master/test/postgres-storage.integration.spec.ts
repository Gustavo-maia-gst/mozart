import { randomUUID } from 'node:crypto';
import { LatencyModel } from '@mozart/latency';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { SystemClock, TimerScheduler } from '../src/clock/clock';
import type { EventInput, EventLogService } from '../src/event-log/event-log.service';
import { MetricsService } from '../src/metrics/metrics.service';
import { PostgresStorageAdapter } from '../src/storage/postgres.adapter';
import { StorageService } from '../src/storage/storage.service';
import { StorageGate } from '../src/storage/storage-gate';

const enabled = process.env.MOZART_INTEGRATION === '1';
const pgUrl = process.env.MOZART_PG_URL ?? 'postgres://mozart:mozart@localhost:5432/mozart';

class FakeEventLog {
  readonly events: EventInput[] = [];
  public record(e: EventInput): unknown {
    this.events.push(e);
    return e;
  }
}

async function pending(p: Promise<unknown>): Promise<boolean> {
  return Promise.race([
    p.then(
      () => false,
      () => false,
    ),
    new Promise<boolean>((r) => setTimeout(() => r(true), 200)),
  ]);
}

describe.runIf(enabled)('PostgresStorageAdapter (integration)', () => {
  let adapter: PostgresStorageAdapter;
  let service: StorageService;

  beforeAll(async () => {
    adapter = new PostgresStorageAdapter(pgUrl);
    await adapter.init();
    service = new StorageService(
      adapter,
      new LatencyModel('s', {}),
      new TimerScheduler(),
      new SystemClock(),
      new StorageGate(),
      new FakeEventLog() as unknown as EventLogService,
      new MetricsService(),
    );
  });

  afterAll(async () => {
    await adapter.dispose();
  });

  it('serializes SELECT ... FOR UPDATE across two pooled clients', async () => {
    const t = `t-${randomUUID()}`;
    const first = await service.readExclusive('n1', t);
    const second = service.readExclusive('n2', t);
    expect(await pending(second)).toBe(true);

    await service.leaseSave(first.leaseId, { v: 1 });
    const acquired = await second;
    expect(acquired.data).toEqual({ v: 1 });
    await service.leaseRelease(acquired.leaseId);
  });

  it('releases the FOR UPDATE lock (ROLLBACK) when the holder crashes', async () => {
    const t = `t-${randomUUID()}`;
    await service.readExclusive('n1', t);
    const waiting = service.readExclusive('n2', t);
    expect(await pending(waiting)).toBe(true);

    await service.releaseNode('n1'); // ROLLBACK + release the client
    const acquired = await waiting;
    expect(acquired.data).toBeNull(); // n1 wrote nothing; rolled back
    await service.leaseRelease(acquired.leaseId);
  });

  it('commits lease saves visibly to later reads', async () => {
    const t = `t-${randomUUID()}`;
    const held = await service.readExclusive('n1', t);
    await service.leaseSave(held.leaseId, { committed: true });
    expect(await service.read('n2', t)).toEqual({ committed: true });
  });
});
