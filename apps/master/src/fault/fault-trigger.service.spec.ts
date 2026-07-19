import type { ConditionalKillFault, NodeId } from '@mozart/contracts';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { CancelHandle, Clock, Scheduler } from '../clock/clock';
import type { EventInput, EventLogService } from '../event-log/event-log.service';
import type { IpcHostService } from '../ipc-server/ipc-host.service';
import type { ProcessManagerService } from '../ipc-server/process-manager.service';
import { MetricsService } from '../metrics/metrics.service';
import type { TransportService } from '../transport/transport.service';
import { FaultTriggerService } from './fault-trigger.service';

class VirtualTime implements Clock, Scheduler {
  private t = 0;
  private timers: { at: number; handle: symbol; fn: () => void }[] = [];
  public now(): number {
    return this.t;
  }
  public after(ms: number, fn: () => void): CancelHandle {
    const handle = Symbol('t');
    this.timers.push({ at: this.t + ms, handle, fn });
    return handle;
  }
  public cancel(handle: CancelHandle): void {
    this.timers = this.timers.filter((x) => x.handle !== handle);
  }
  public advance(ms: number): void {
    const target = this.t + ms;
    for (;;) {
      const due = this.timers.filter((x) => x.at <= target).sort((a, b) => a.at - b.at)[0];
      if (!due) break;
      this.timers = this.timers.filter((x) => x !== due);
      this.t = due.at;
      due.fn();
    }
    this.t = target;
  }
}

class FakePm {
  readonly killed: NodeId[] = [];
  readonly restarted: NodeId[] = [];
  public kill(nodeId: NodeId): void {
    this.killed.push(nodeId);
  }
  public restart(nodeId: NodeId): void {
    this.restarted.push(nodeId);
  }
}

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

function fault(overrides: Partial<ConditionalKillFault> = {}): ConditionalKillFault {
  return {
    action: 'conditionalKill',
    phase: 'before',
    hook: 'StorageSave',
    filter: "message.taskId === 'g0-b'",
    restartAfterMs: 500,
    times: 1,
    ...overrides,
  };
}

function build() {
  const time = new VirtualTime();
  const pm = new FakePm();
  const events = new FakeEventLog();
  const metrics = new MetricsService();
  const ipcHost = {} as IpcHostService;
  const transport = {} as TransportService;

  const triggers = new FaultTriggerService(
    time,
    pm as unknown as ProcessManagerService,
    events as unknown as EventLogService,
    metrics,
    ipcHost,
    transport,
  );
  return { time, pm, events, metrics, ipcHost, transport, triggers };
}

describe('FaultTriggerService', () => {
  let ctx: ReturnType<typeof build>;
  beforeEach(() => {
    ctx = build();
  });

  it('wires itself into ipcHost and transport at construction', () => {
    const { ipcHost, transport } = ctx;
    expect(ipcHost.trigger).toBeTypeOf('function');
    expect(transport.trigger).toBeTypeOf('function');
  });

  it('kills the node and returns true when the filter matches', () => {
    const { triggers, pm, ipcHost } = ctx;
    triggers.register(fault());
    const killed = ipcHost.trigger?.('StorageSave', 'before', { message: { taskId: 'g0-b' }, node: 'n1' });
    expect(killed).toBe(true);
    expect(pm.killed).toEqual(['n1']);
  });

  it('does not kill and returns false when the filter does not match', () => {
    const { triggers, pm, ipcHost } = ctx;
    triggers.register(fault());
    const killed = ipcHost.trigger?.('StorageSave', 'before', { message: { taskId: 'g0-other' }, node: 'n1' });
    expect(killed).toBe(false);
    expect(pm.killed).toEqual([]);
  });

  it('only matches the declared hook and phase', () => {
    const { triggers, pm, ipcHost } = ctx;
    triggers.register(fault({ hook: 'StorageSave', phase: 'before' }));
    expect(ipcHost.trigger?.('StorageSave', 'after', { message: { taskId: 'g0-b' }, node: 'n1' })).toBe(false);
    expect(ipcHost.trigger?.('StorageRead', 'before', { message: { taskId: 'g0-b' }, node: 'n1' })).toBe(false);
    expect(pm.killed).toEqual([]);
  });

  it('schedules a restart after restartAfterMs', () => {
    const { triggers, pm, ipcHost, time } = ctx;
    triggers.register(fault({ restartAfterMs: 500 }));
    ipcHost.trigger?.('StorageSave', 'before', { message: { taskId: 'g0-b' }, node: 'n1' });
    expect(pm.restarted).toEqual([]);
    time.advance(499);
    expect(pm.restarted).toEqual([]);
    time.advance(1);
    expect(pm.restarted).toEqual(['n1']);
  });

  it('is one-shot by default: a second match after times is exhausted no-ops', () => {
    const { triggers, pm, ipcHost } = ctx;
    triggers.register(fault({ times: 1 }));
    ipcHost.trigger?.('StorageSave', 'before', { message: { taskId: 'g0-b' }, node: 'n1' });
    const secondMatch = ipcHost.trigger?.('StorageSave', 'before', { message: { taskId: 'g0-b' }, node: 'n1' });
    expect(secondMatch).toBe(false);
    expect(pm.killed).toEqual(['n1']); // only once
  });

  it('fires up to `times` before it is spent', () => {
    const { triggers, pm, ipcHost } = ctx;
    triggers.register(fault({ times: 3 }));
    for (let i = 0; i < 3; i++) {
      expect(ipcHost.trigger?.('StorageSave', 'before', { message: { taskId: 'g0-b' }, node: 'n1' })).toBe(true);
    }
    expect(ipcHost.trigger?.('StorageSave', 'before', { message: { taskId: 'g0-b' }, node: 'n1' })).toBe(false);
    expect(pm.killed).toEqual(['n1', 'n1', 'n1']);
  });

  it('treats a filter that throws at runtime as a no-match, not a crash', () => {
    const { triggers, pm, ipcHost } = ctx;
    triggers.register(fault({ filter: 'message.taskId.startsWith("g0")' })); // throws if message.taskId is undefined
    expect(() =>
      ipcHost.trigger?.('StorageSave', 'before', { message: {}, node: 'n1' }),
    ).not.toThrow();
    expect(pm.killed).toEqual([]);
  });

  it('records a fault.injected event and the conditionalKill metric on match', () => {
    const { triggers, events, metrics, ipcHost } = ctx;
    const spy = vi.spyOn(metrics, 'countFault');
    triggers.register(fault());
    ipcHost.trigger?.('StorageSave', 'before', { message: { taskId: 'g0-b' }, node: 'n1' });

    expect(spy).toHaveBeenCalledWith('conditionalKill');
    const recorded = events.ofType('fault.injected');
    expect(recorded).toHaveLength(1);
    expect(recorded[0]?.data).toMatchObject({ action: 'conditionalKill', hook: 'StorageSave', phase: 'before' });
    expect(recorded[0]?.nodeId).toBe('n1');
  });

  it('delegates the same way through transport.trigger', () => {
    const { triggers, pm, transport } = ctx;
    triggers.register(fault({ hook: 'ReceiveMessage', phase: 'after' }));
    const killed = transport.trigger?.('ReceiveMessage', 'after', {
      message: { taskId: 'g0-b' },
      topic: 'task.completed',
      node: 'n1',
      attempt: 1,
    });
    expect(killed).toBe(true);
    expect(pm.killed).toEqual(['n1']);
  });

  it('exposes topic and attempt to the filter expression', () => {
    const { triggers, pm, transport } = ctx;
    triggers.register(fault({ hook: 'ReceiveMessage', phase: 'after', filter: "topic === 'task.failed' && attempt > 1" }));
    expect(
      transport.trigger?.('ReceiveMessage', 'after', { message: {}, topic: 'task.completed', node: 'n1', attempt: 2 }),
    ).toBe(false);
    expect(
      transport.trigger?.('ReceiveMessage', 'after', { message: {}, topic: 'task.failed', node: 'n1', attempt: 1 }),
    ).toBe(false);
    expect(
      transport.trigger?.('ReceiveMessage', 'after', { message: {}, topic: 'task.failed', node: 'n1', attempt: 2 }),
    ).toBe(true);
    expect(pm.killed).toEqual(['n1']);
  });
});
