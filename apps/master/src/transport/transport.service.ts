import { randomUUID } from 'node:crypto';
import { Inject, Injectable } from '@nestjs/common';
import { context, SpanKind, trace } from '@opentelemetry/api';
import {
  channelKey,
  type ChannelKey,
  type Delivery,
  type Json,
  type NodeId,
  type Scenario,
} from '@mozart/contracts';
import { ATTR, injectActiveContext, runWithExtractedContext, TRACER_NAME } from '@mozart/telemetry';
import type { Clock, Scheduler } from '../clock/clock';
import { EventLogService } from '../event-log/event-log.service';
import { CLOCK, LATENCY_MODEL, SCENARIO, SCHEDULER } from '../tokens';
import type { LatencyModel } from '@mozart/latency';
import { Channel, type QueuedMessage } from './channel';
import { DELIVERY_SINK, NetworkState, type DeliverySink } from './delivery-sink';

const tracer = trace.getTracer(TRACER_NAME);
const DELIVER_LATENCY = 'transport.deliver';

/**
 * Persistent Pub/Sub-style fabric: at-least-once delivery, FIFO per (from,to)
 * channel, explicit acks with visibility-timeout redelivery. Fault hooks for
 * duplication and (via NetworkState) partitions.
 */
@Injectable()
export class TransportService {
  private readonly channels = new Map<ChannelKey, Channel>();
  /** deliveryId -> channel, for O(1) ack routing. */
  private readonly outstanding = new Map<string, Channel>();
  private readonly ackTimeoutMs: number;

  constructor(
    @Inject(SCHEDULER) private readonly scheduler: Scheduler,
    @Inject(CLOCK) private readonly clock: Clock,
    @Inject(LATENCY_MODEL) private readonly latency: LatencyModel,
    @Inject(SCENARIO) scenario: Scenario,
    @Inject(DELIVERY_SINK) private readonly sink: DeliverySink,
    private readonly events: EventLogService,
    private readonly network: NetworkState,
  ) {
    this.ackTimeoutMs = scenario.transport.ackTimeoutMs;
  }

  /** Enqueue a message; captures the active trace context for propagation. */
  publish(from: NodeId, to: NodeId, topic: string, body: Json): string {
    const messageId = randomUUID();
    const publishTraceCtx: Record<string, string> = {};
    injectActiveContext(publishTraceCtx);

    const ch = this.channelFor(from, to);
    ch.queue.push({
      messageId,
      from,
      to,
      topic,
      body,
      publishTraceCtx,
      deliverableAt: this.clock.now() + this.latency.sample(DELIVER_LATENCY),
      attempts: 0,
    });
    this.events.record({
      type: 'transport.published',
      nodeId: from,
      channel: ch.key,
      messageId,
      data: { topic, to },
    });
    this.pump(ch);
    return messageId;
  }

  /** Acknowledge a delivery. Unknown/stale ids are ignored (idempotent). */
  ack(deliveryId: string): void {
    const ch = this.outstanding.get(deliveryId);
    if (!ch || ch.outstanding?.deliveryId !== deliveryId) return;

    this.scheduler.cancel(ch.outstanding.timer);
    this.outstanding.delete(deliveryId);
    const msg = ch.queue.shift();
    ch.outstanding = undefined;
    this.events.record({
      type: 'transport.acked',
      channel: ch.key,
      messageId: msg?.messageId,
      deliveryId,
    });
    this.pump(ch);
  }

  // --- fault hooks (invoked by the fault injector) ---------------------------

  /** Emit `extraCopies` duplicate deliveries on the channel's next delivery. */
  scheduleDuplicates(from: NodeId, to: NodeId, extraCopies: number): void {
    this.channelFor(from, to).duplicateBudget += extraCopies;
  }

  /** Resume channels whose endpoints just became un-partitioned. */
  resumeAll(): void {
    for (const ch of this.channels.values()) this.pump(ch);
  }

  // --- internals -------------------------------------------------------------

  private channelFor(from: NodeId, to: NodeId): Channel {
    const key = channelKey(from, to);
    let ch = this.channels.get(key);
    if (!ch) {
      ch = new Channel(key);
      this.channels.set(key, ch);
    }
    return ch;
  }

  private pump(ch: Channel): void {
    if (ch.outstanding || ch.pumpTimer) return;
    const head = ch.head;
    if (!head) return;
    if (this.network.blocks(head.from, head.to)) {
      // Paused; resumeAll() re-pumps when the partition lifts.
      this.events.record({ type: 'transport.blocked', channel: ch.key, messageId: head.messageId });
      return;
    }
    const wait = head.deliverableAt - this.clock.now();
    if (wait > 0) {
      ch.pumpTimer = this.scheduler.after(wait, () => {
        ch.pumpTimer = undefined;
        this.pump(ch);
      });
      return;
    }
    this.deliverHead(ch, false);
  }

  private deliverHead(ch: Channel, redelivery: boolean): void {
    const head = ch.head;
    if (!head) return;
    head.attempts += 1;
    const deliveryId = randomUUID();

    // Deliver span descends from the publish context; the delivery carries the
    // deliver-span context so the slave's onMessage becomes its child.
    runWithExtractedContext(head.publishTraceCtx, () => {
      const span = tracer.startSpan(redelivery ? 'transport.redeliver' : 'transport.deliver', {
        kind: SpanKind.PRODUCER,
        attributes: {
          [ATTR.channel]: ch.key,
          [ATTR.messageId]: head.messageId,
          [ATTR.topic]: head.topic,
          [ATTR.attempt]: head.attempts,
        },
      });
      context.with(trace.setSpan(context.active(), span), () => {
        const delivery = this.buildDelivery(head, deliveryId);
        this.events.record({
          type: redelivery ? 'transport.redelivered' : 'transport.delivered',
          channel: ch.key,
          messageId: head.messageId,
          deliveryId,
          attempt: head.attempts,
          data: { topic: head.topic },
        });
        const ok = this.sink.deliver(head.to, delivery);
        span.end();
        if (ok && !redelivery) this.emitDuplicates(ch, head);
      });
    });

    // Track outstanding + arm the visibility timer regardless of reachability:
    // if the node is down the timer simply retries later.
    ch.outstanding = {
      deliveryId,
      timer: this.scheduler.after(this.ackTimeoutMs, () => this.onVisibilityTimeout(ch)),
    };
    this.outstanding.set(deliveryId, ch);
  }

  private onVisibilityTimeout(ch: Channel): void {
    if (!ch.outstanding) return;
    // Supersede the previous delivery id, then redeliver the same head.
    this.outstanding.delete(ch.outstanding.deliveryId);
    ch.outstanding = undefined;
    if (this.network.blocks(ch.head?.from ?? '', ch.head?.to ?? '')) {
      this.pump(ch); // will re-block/pause cleanly
      return;
    }
    this.deliverHead(ch, true);
  }

  private emitDuplicates(ch: Channel, head: QueuedMessage): void {
    while (ch.duplicateBudget > 0) {
      ch.duplicateBudget -= 1;
      const dupId = randomUUID();
      const delivery = this.buildDelivery(head, dupId);
      this.events.record({
        type: 'transport.duplicated',
        channel: ch.key,
        messageId: head.messageId,
        deliveryId: dupId,
        attempt: head.attempts,
      });
      this.sink.deliver(head.to, delivery);
    }
  }

  private buildDelivery(head: QueuedMessage, deliveryId: string): Delivery {
    const traceCtx: Record<string, string> = {};
    injectActiveContext(traceCtx);
    return {
      deliveryId,
      messageId: head.messageId,
      from: head.from,
      topic: head.topic,
      body: head.body,
      attempt: head.attempts,
      traceCtx,
    };
  }
}
