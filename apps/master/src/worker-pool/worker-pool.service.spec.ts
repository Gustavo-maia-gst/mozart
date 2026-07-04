import { beforeEach, describe, expect, it } from 'vitest';
import { LatencyModel } from '@mozart/latency';
import { WORKER_NODE_ID, WORKER_TOPICS, type Json, type NodeId, type Scenario } from '@mozart/contracts';
import type { CancelHandle, Clock, Scheduler } from '../clock/clock';
import type { EventInput, EventLogService } from '../event-log/event-log.service';
import type { TransportService } from '../transport/transport.service';
import { WorkerPoolService } from './worker-pool.service';

class VirtualTime implements Clock, Scheduler {
  private t = 0;
  private timers: { at: number; handle: symbol; fn: () => void }[] = [];
  now(): number {
    return this.t;
  }
  after(ms: number, fn: () => void): CancelHandle {
    const handle = Symbol('t');
    this.timers.push({ at: this.t + ms, handle, fn });
    return handle;
  }
  cancel(handle: CancelHandle): void {
    this.timers = this.timers.filter((x) => x.handle !== handle);
  }
  advance(ms: number): void {
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

class FakeTransport {
  readonly published: { from: NodeId; to: NodeId; topic: string; body: Json }[] = [];
  publish(from: NodeId, to: NodeId, topic: string, body: Json): string {
    this.published.push({ from, to, topic, body });
    return 'm';
  }
}

class FakeEventLog {
  readonly events: EventInput[] = [];
  record(e: EventInput): unknown {
    this.events.push(e);
    return e;
  }
  ofType(type: string): EventInput[] {
    return this.events.filter((e) => e.type === type);
  }
}

const scenario = {
  dag: { tasks: [{ id: 't1', dependsOn: [], costMs: 100 }, { id: 't2', dependsOn: [] }] },
} as Scenario;

function build() {
  const time = new VirtualTime();
  const transport = new FakeTransport();
  const log = new FakeEventLog();
  const latency = new LatencyModel('s', {
    'worker.taskDuration': { distribution: 'constant', value: 50 },
  });
  const worker = new WorkerPoolService(
    time,
    latency,
    scenario,
    transport as unknown as TransportService,
    log as unknown as EventLogService,
  );
  return { time, transport, log, worker };
}

describe('WorkerPoolService', () => {
  let ctx: ReturnType<typeof build>;
  beforeEach(() => {
    ctx = build();
  });

  it('publishes task.completed to the starting node after the task duration', () => {
    const { worker, time, transport } = ctx;
    worker.start('n1', 't1');
    expect(transport.published).toHaveLength(0);
    time.advance(100); // t1 costMs
    expect(transport.published).toEqual([
      { from: WORKER_NODE_ID, to: 'n1', topic: WORKER_TOPICS.completed, body: { taskId: 't1' } },
    ]);
  });

  it('uses the sampled duration when the task has no costMs', () => {
    const { worker, time, transport } = ctx;
    worker.start('n1', 't2'); // no costMs => latency 50
    time.advance(49);
    expect(transport.published).toHaveLength(0);
    time.advance(1);
    expect(transport.published).toHaveLength(1);
  });

  it('ignores a duplicate start while the task is running', () => {
    const { worker, time, transport, log } = ctx;
    worker.start('n1', 't1');
    worker.start('n1', 't1');
    expect(log.ofType('worker.duplicate-start')).toHaveLength(1);
    time.advance(100);
    expect(transport.published).toHaveLength(1); // only one completion
  });

  it('allows restart after completion', () => {
    const { worker, time, transport } = ctx;
    worker.start('n1', 't1');
    time.advance(100);
    worker.start('n1', 't1');
    time.advance(100);
    expect(transport.published).toHaveLength(2);
  });

  it('emits task.failed (one-shot) when the task is marked to fail', () => {
    const { worker, time, transport } = ctx;
    worker.failTask('t1');
    worker.start('n1', 't1');
    time.advance(100);
    expect(transport.published[0]?.topic).toBe(WORKER_TOPICS.failed);

    worker.start('n1', 't1'); // fail flag was one-shot
    time.advance(100);
    expect(transport.published[1]?.topic).toBe(WORKER_TOPICS.completed);
  });
});
