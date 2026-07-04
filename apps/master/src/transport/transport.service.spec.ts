import { type Delivery, type NodeId, Scenario, type ScenarioData } from '@mozart/contracts';
import { LatencyModel } from '@mozart/latency';
import { beforeEach, describe, expect, it } from 'vitest';
import type { CancelHandle, Clock, Scheduler } from '../clock/clock';
import type { EventInput, EventLogService } from '../event-log/event-log.service';
import { MetricsService } from '../metrics/metrics.service';
import { type DeliverySink, NetworkState } from './delivery-sink';
import { TransportService } from './transport.service';

/** Deterministic virtual time: timers fire in order when `advance` is called. */
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

/** Records deliveries; `live` is the round-robin candidate pool. */
class FakeSink implements DeliverySink {
  readonly delivered: { to: NodeId; delivery: Delivery }[] = [];
  live: NodeId[] = ['c1'];
  reachable = true;
  public deliver(to: NodeId, delivery: Delivery): boolean {
    if (!this.reachable) return false;
    this.delivered.push({ to, delivery });
    return true;
  }
  public liveNodeIds(): NodeId[] {
    return this.live;
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

const scenario = new Scenario({ transport: { ackTimeoutMs: 100 }, graphs: [] } as unknown as ScenarioData);

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
    new MetricsService(),
  );
  return { time, sink, log, network, transport };
}

describe('TransportService', () => {
  let ctx: ReturnType<typeof build>;
  beforeEach(() => {
    ctx = build();
  });

  it('delivers a message to a live coordinator', () => {
    const { transport, sink } = ctx;
    transport.sendToCoordinators('a', { i: 1 }, 'W');
    expect(sink.delivered).toHaveLength(1);
    expect(sink.delivered[0]?.to).toBe('c1');
    expect(sink.delivered[0]?.delivery.topic).toBe('a');
    expect(sink.delivered[0]?.delivery.attempt).toBe(1);
  });

  it('round-robins across coordinators', () => {
    const { transport, sink } = ctx;
    sink.live = ['c1', 'c2', 'c3'];
    transport.sendToCoordinators('a', {}, 'W');
    transport.sendToCoordinators('b', {}, 'W');
    transport.sendToCoordinators('c', {}, 'W');
    expect(sink.delivered.map((d) => d.to)).toEqual(['c1', 'c2', 'c3']);
  });

  it('redelivers to the next coordinator after the ack-visibility timeout', () => {
    const { transport, sink, time, log } = ctx;
    sink.live = ['c1', 'c2'];
    transport.sendToCoordinators('a', {}, 'W');
    expect(sink.delivered).toHaveLength(1);
    expect(sink.delivered[0]?.to).toBe('c1');

    time.advance(100); // ack timeout elapses with no ack
    expect(sink.delivered).toHaveLength(2);
    expect(sink.delivered[1]?.to).toBe('c2'); // next coordinator
    expect(sink.delivered[1]?.delivery.messageId).toBe(sink.delivered[0]?.delivery.messageId);
    expect(sink.delivered[1]?.delivery.attempt).toBe(2);
    expect(log.ofType('transport.redelivered')).toHaveLength(1);
  });

  it('stops redelivering once acked', () => {
    const { transport, sink, time } = ctx;
    transport.sendToCoordinators('a', {}, 'W');
    transport.ack(sink.delivered[0]!.delivery.deliveryId);
    time.advance(500);
    expect(sink.delivered).toHaveLength(1);
  });

  it('ignores stale/duplicate acks', () => {
    const { transport, sink } = ctx;
    transport.sendToCoordinators('a', {}, 'W');
    const id = sink.delivered[0]!.delivery.deliveryId;
    transport.ack(id);
    expect(() => transport.ack(id)).not.toThrow();
    transport.ack('never-issued');
    expect(sink.delivered).toHaveLength(1);
  });

  it('injects duplicate deliveries on the next message', () => {
    const { transport, sink, log } = ctx;
    transport.scheduleDuplicates(2);
    transport.sendToCoordinators('a', {}, 'W');
    expect(sink.delivered).toHaveLength(3); // 1 real + 2 duplicates
    const ids = new Set(sink.delivered.map((d) => d.delivery.messageId));
    expect(ids.size).toBe(1); // same messageId => exercises idempotence
    expect(log.ofType('transport.duplicated')).toHaveLength(2);
  });

  it('parks a message when no coordinator is reachable and resumes on unblock', () => {
    const { transport, sink, network, log } = ctx;
    network.inboundBlocked.add('c1');
    transport.sendToCoordinators('a', {}, 'W');
    expect(sink.delivered).toHaveLength(0);
    expect(log.ofType('transport.blocked').length).toBeGreaterThanOrEqual(1);

    network.inboundBlocked.delete('c1');
    transport.resumeAll();
    expect(sink.delivered).toHaveLength(1);
  });

  it('drops the ack of an outbound-partitioned coordinator (keeps retrying)', () => {
    const { transport, sink, network, time } = ctx;
    transport.sendToCoordinators('a', {}, 'W');
    network.outboundBlocked.add('c1');
    transport.ack(sink.delivered[0]!.delivery.deliveryId); // dropped
    time.advance(100);
    expect(sink.delivered).toHaveLength(2); // redelivered despite the ack
  });

  it('applies publish latency before delivery', () => {
    const c = build(50);
    c.transport.sendToCoordinators('a', {}, 'W');
    expect(c.sink.delivered).toHaveLength(0);
    c.time.advance(49);
    expect(c.sink.delivered).toHaveLength(0);
    c.time.advance(1);
    expect(c.sink.delivered).toHaveLength(1);
  });
});
