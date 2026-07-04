import { beforeEach, describe, expect, it } from 'vitest';
import { LatencyModel } from '@mozart/latency';
import type { Delivery, NodeId, Scenario } from '@mozart/contracts';
import type { CancelHandle, Clock, Scheduler } from '../clock/clock';
import type { EventLogService, EventInput } from '../event-log/event-log.service';
import { NetworkState, type DeliverySink } from './delivery-sink';
import { TransportService } from './transport.service';

/** Deterministic virtual time: timers fire in order when `advance` is called. */
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
      const due = this.timers
        .filter((x) => x.at <= target)
        .sort((a, b) => a.at - b.at)[0];
      if (!due) break;
      this.timers = this.timers.filter((x) => x !== due);
      this.t = due.at;
      due.fn();
    }
    this.t = target;
  }
}

class FakeSink implements DeliverySink {
  readonly delivered: { to: NodeId; delivery: Delivery }[] = [];
  reachable = true;
  deliver(to: NodeId, delivery: Delivery): boolean {
    if (!this.reachable) return false;
    this.delivered.push({ to, delivery });
    return true;
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

const scenario = { transport: { ackTimeoutMs: 100 } } as Scenario;

function build(latencyMs = 0) {
  const time = new VirtualTime();
  const sink = new FakeSink();
  const log = new FakeEventLog();
  const network = new NetworkState();
  const latency = new LatencyModel('seed', {
    'transport.deliver': { distribution: 'constant', value: latencyMs },
  });
  const transport = new TransportService(
    time,
    time,
    latency,
    scenario,
    sink,
    log as unknown as EventLogService,
    network,
  );
  return { time, sink, log, network, transport };
}

describe('TransportService', () => {
  let ctx: ReturnType<typeof build>;
  beforeEach(() => {
    ctx = build();
  });

  it('delivers FIFO with a single message outstanding per channel', () => {
    const { transport, sink } = ctx;
    transport.publish('n1', 'n2', 'a', { i: 1 });
    transport.publish('n1', 'n2', 'b', { i: 2 });

    // Only the head is delivered until it is acked.
    expect(sink.delivered).toHaveLength(1);
    expect(sink.delivered[0]?.delivery.topic).toBe('a');

    transport.ack(sink.delivered[0]!.delivery.deliveryId);
    expect(sink.delivered).toHaveLength(2);
    expect(sink.delivered[1]?.delivery.topic).toBe('b');
  });

  it('does not deliver the next message before the head is acked', () => {
    const { transport, sink, time } = ctx;
    transport.publish('n1', 'n2', 'a', {});
    transport.publish('n1', 'n2', 'b', {});
    time.advance(50); // well within ack timeout
    expect(sink.delivered).toHaveLength(1);
  });

  it('redelivers the same message after the ack-visibility timeout', () => {
    const { transport, sink, time, log } = ctx;
    transport.publish('n1', 'n2', 'a', {});
    expect(sink.delivered).toHaveLength(1);
    expect(sink.delivered[0]?.delivery.attempt).toBe(1);

    time.advance(100); // ack timeout elapses with no ack
    expect(sink.delivered).toHaveLength(2);
    expect(sink.delivered[1]?.delivery.messageId).toBe(sink.delivered[0]?.delivery.messageId);
    expect(sink.delivered[1]?.delivery.attempt).toBe(2);
    expect(log.ofType('transport.redelivered')).toHaveLength(1);
  });

  it('stops redelivering once acked', () => {
    const { transport, sink, time } = ctx;
    transport.publish('n1', 'n2', 'a', {});
    transport.ack(sink.delivered[0]!.delivery.deliveryId);
    time.advance(500);
    expect(sink.delivered).toHaveLength(1); // no redelivery after ack
  });

  it('ignores stale/duplicate acks', () => {
    const { transport, sink } = ctx;
    transport.publish('n1', 'n2', 'a', {});
    const id = sink.delivered[0]!.delivery.deliveryId;
    transport.ack(id);
    expect(() => transport.ack(id)).not.toThrow();
    transport.ack('never-issued');
    expect(sink.delivered).toHaveLength(1);
  });

  it('injects duplicate deliveries on the next delivery', () => {
    const { transport, sink, log } = ctx;
    transport.scheduleDuplicates('n1', 'n2', 2);
    transport.publish('n1', 'n2', 'a', {});
    expect(sink.delivered).toHaveLength(3); // 1 real + 2 duplicates
    const ids = new Set(sink.delivered.map((d) => d.delivery.messageId));
    expect(ids.size).toBe(1); // same messageId => exercises idempotence
    expect(log.ofType('transport.duplicated')).toHaveLength(2);
  });

  it('pauses a partitioned channel and resumes on unblock', () => {
    const { transport, sink, network, log } = ctx;
    network.outboundBlocked.add('n1');
    transport.publish('n1', 'n2', 'a', {});
    expect(sink.delivered).toHaveLength(0);
    expect(log.ofType('transport.blocked').length).toBeGreaterThanOrEqual(1);

    network.outboundBlocked.delete('n1');
    transport.resumeAll();
    expect(sink.delivered).toHaveLength(1);
  });

  it('applies publish latency before delivery', () => {
    const c = build(50);
    c.transport.publish('n1', 'n2', 'a', {});
    expect(c.sink.delivered).toHaveLength(0);
    c.time.advance(49);
    expect(c.sink.delivered).toHaveLength(0);
    c.time.advance(1);
    expect(c.sink.delivered).toHaveLength(1);
  });

  it('keeps channels independent', () => {
    const { transport, sink } = ctx;
    transport.publish('n1', 'n2', 'a', {});
    transport.publish('n3', 'n2', 'b', {});
    // Different (from,to) channels => both heads deliver independently.
    expect(sink.delivered.map((d) => d.delivery.topic).sort()).toEqual(['a', 'b']);
  });
});
