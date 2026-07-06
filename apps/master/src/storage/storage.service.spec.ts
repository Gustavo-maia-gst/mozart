import { LatencyModel } from '@mozart/latency';
import { beforeEach, describe, expect, it } from 'vitest';
import { SystemClock, TimerScheduler } from '../clock/clock';
import type { EventInput, EventLogService } from '../event-log/event-log.service';
import { MetricsService } from '../metrics/metrics.service';
import { InMemoryStorageAdapter } from './in-memory.adapter';
import { StorageService } from './storage.service';
import { NodeCrashedError } from './storage-adapter';
import { StorageGate } from './storage-gate';

class FakeEventLog {
  readonly events: EventInput[] = [];
  public record(e: EventInput): unknown {
    this.events.push(e);
    return e;
  }
  public ofType(type: string): EventInput[] {
    return this.events.filter((e) => e.type === type);
  }
}

async function pending(p: Promise<unknown>): Promise<boolean> {
  return Promise.race([
    p.then(
      () => false,
      () => false,
    ),
    new Promise<boolean>((r) => setTimeout(() => r(true), 25)),
  ]);
}

function build() {
  const gate = new StorageGate();
  const log = new FakeEventLog();
  const service = new StorageService(
    new InMemoryStorageAdapter(),
    new LatencyModel('s', {}), // all latencies 0
    new TimerScheduler(),
    new SystemClock(),
    gate,
    log as unknown as EventLogService,
    new MetricsService(),
  );
  return { gate, log, service };
}

describe('StorageService (in-memory)', () => {
  let ctx: ReturnType<typeof build>;
  beforeEach(() => {
    ctx = build();
  });

  it('serializes readExclusive on the same task', async () => {
    const { service } = ctx;
    const first = await service.readExclusive('n1', 't1');
    const second = service.readExclusive('n2', 't1');
    expect(await pending(second)).toBe(true); // blocked while n1 holds the lock

    await service.leaseSave(first.leaseId, { v: 1 });
    const acquired = await second; // now granted
    expect(acquired.data).toEqual({ v: 1 }); // sees n1's committed write
  });

  it('never blocks a plain read while a task is locked', async () => {
    const { service } = ctx;
    await service.save('n1', 't1', { v: 0 });
    await service.readExclusive('n1', 't1');
    expect(await pending(service.read('n2', 't1'))).toBe(false);
  });

  it('force-releases a held lock when the holder crashes', async () => {
    const { service, log } = ctx;
    const held = await service.readExclusive('n1', 't1');
    const waiting = service.readExclusive('n2', 't1');
    expect(await pending(waiting)).toBe(true);

    await service.releaseNode('n1');
    expect(log.ofType('storage.lease.force-released')).toHaveLength(1);
    await expect(waiting).resolves.toBeDefined(); // n2 now acquires
    expect(service.heldLeaseCount()).toBe(1); // only n2 holds now
    void held;
  });

  it('cancels a pending acquisition when the waiter crashes', async () => {
    const { service } = ctx;
    await service.readExclusive('n1', 't1'); // n1 holds
    const waiting = service.readExclusive('n2', 't1');
    const caught = waiting.catch((e: unknown) => e);
    expect(await pending(waiting)).toBe(true);

    await service.releaseNode('n2'); // n2 crashes while waiting
    expect(await caught).toBeInstanceOf(NodeCrashedError);
  });

  it('treats leaseSave on a force-released lease as a no-op', async () => {
    const { service } = ctx;
    const held = await service.readExclusive('n1', 't1');
    await service.releaseNode('n1');
    await expect(service.leaseSave(held.leaseId, { v: 9 })).resolves.toBeUndefined();
    expect(await service.read('nx', 't1')).toBeNull(); // nothing was written
  });

  it('parks calls during an outage and resumes on recovery', async () => {
    const { service, gate } = ctx;
    gate.begin('all');
    const read = service.read('n1', 't1');
    expect(await pending(read)).toBe(true);
    gate.end('all');
    await expect(read).resolves.toBeNull();
  });

  it('scopes an outage to a single node', async () => {
    const { service, gate } = ctx;
    gate.begin('n1');
    expect(await pending(service.read('n1', 't1'))).toBe(true); // n1 blocked
    expect(await pending(service.read('n2', 't1'))).toBe(false); // n2 unaffected
    gate.end('n1');
  });

  it('find: scalar attributes match by equality, array attributes by IN', async () => {
    const { service } = ctx;
    await service.save('n1', 'a', { taskId: 'a', status: 'complete' });
    await service.save('n1', 'b', { taskId: 'b', status: 'complete' });
    await service.save('n1', 'c', { taskId: 'c', status: 'pending' });

    // IN on taskId, equality on status → only the complete ones among {a,b,c}.
    const done = await service.find('n1', { taskId: ['a', 'b', 'c'], status: 'complete' });
    expect(new Set(done.map((m) => m.taskId))).toEqual(new Set(['a', 'b']));

    // An empty IN list matches nothing.
    expect(await service.find('n1', { taskId: [] })).toHaveLength(0);
  });

  it('delete removes every record matching the query (by WHERE), returning the count', async () => {
    const { service } = ctx;
    await service.save('n1', 'edge:a->b', { kind: 'edge', source: 'a', target: 'b' });
    await service.save('n1', 'edge:a->c', { kind: 'edge', source: 'a', target: 'c' });
    await service.save('n1', 'edge:b->d', { kind: 'edge', source: 'b', target: 'd' });

    const deleted = await service.delete('n1', { kind: 'edge', source: 'a' });
    expect(deleted).toBe(2);
    expect(await service.read('n1', 'edge:a->b')).toBeNull();
    expect(await service.read('n1', 'edge:b->d')).not.toBeNull(); // untouched
    expect(await service.delete('n1', { kind: 'edge', source: 'a' })).toBe(0); // idempotent
  });
});
