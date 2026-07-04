import { randomUUID } from 'node:crypto';
import { type Delivery, type GraphId, type Json, type NodeId, type Scenario } from '@mozart/contracts';
import type { LatencyModel } from '@mozart/latency';
import {
  annotateSpan,
  ATTR,
  injectActiveContext,
  runWithExtractedContext,
  serviceTracer,
  SpanKind,
  Trace,
  TRACER_NAME,
} from '@mozart/telemetry';
import { Inject, Injectable } from '@nestjs/common';
import { context, type Span, SpanStatusCode, trace } from '@opentelemetry/api';
import type { CancelHandle, Clock, Scheduler } from '../clock/clock';
import { EventLogService } from '../event-log/event-log.service';
import { MetricsService } from '../metrics/metrics.service';
import { CLOCK, LATENCY_MODEL, SCENARIO, SCHEDULER } from '../tokens';
import { DELIVERY_SINK, type DeliverySink, NetworkState } from './delivery-sink';

const tracer = trace.getTracer(TRACER_NAME);
/**
 * The simulated broker renders as its own service (⇒ its own colour/lane in
 * Jaeger/Grafana), distinct from the run's main service. Only the message-
 * transport spans (queue residency + redelivery) live here; control-plane spans
 * like startGraph stay on the main service.
 */
export const TRANSPORT_SERVICE = 'transport';
const DELIVER_LATENCY = 'transport.deliver';

/** One in-flight message on the coordinators queue, awaiting an ack. */
interface Pending {
  messageId: string;
  /** Logical origin (`W`, `harness`, a coordinator id) — telemetry only. */
  origin: string;
  topic: string;
  body: Json;
  publishTraceCtx: Record<string, string>;
  publishedAt: number;
  /** Delivery count so far (0 until first dispatch). */
  attempts: number;
  /** Current outstanding delivery id + the coordinator it went to. */
  deliveryId?: string;
  target?: NodeId;
  timer?: CancelHandle;
  /** No live coordinator to take it — parked until {@link resumeAll}. */
  blocked: boolean;
  /** Extra copies to emit on the next successful delivery (fault). */
  duplicateBudget: number;
  /** Lifetime span for the time spent queued awaiting first delivery; ended then. */
  queueSpan?: Span;
}

/**
 * The message fabric. There is no point-to-point addressing: coordinators are
 * one logical entity backed by N interchangeable processes, and every message
 * to them is a work item delivered to whichever coordinator round-robin picks.
 * At-least-once, no ordering, no de-dup: an unacked item is simply re-dispatched
 * (to the next coordinator) until acked. Also owns the per-graph lifetime span
 * and the run's all-graphs-complete signal.
 */
@Injectable()
export class TransportService {
  /** messageId -> in-flight message. */
  private readonly messages = new Map<string, Pending>();
  /** deliveryId -> messageId, for O(1) ack routing. */
  private readonly byDelivery = new Map<string, string>();
  /** One live span per running graph: opened on graph.start, ended on completeGraph. */
  private readonly graphSpans = new Map<GraphId, Span>();
  /** Distinct graphs the coordinators have reported complete. */
  private readonly completedGraphs = new Set<GraphId>();
  /** Re-checked whenever a graph completes; resolves the run's completion wait. */
  private graphsCompleteCheck?: () => void;
  private readonly totalGraphs: number;
  private readonly ackTimeoutMs: number;
  /** Round-robin cursor across live coordinators. */
  private rr = 0;
  /** Duplicate copies to attach to the next enqueued message (fault). */
  private duplicateBudget = 0;

  // biome-ignore lint/complexity/useMaxParams: deps injection
  constructor(
    @Inject(SCHEDULER) private readonly scheduler: Scheduler,
    @Inject(CLOCK) private readonly clock: Clock,
    @Inject(LATENCY_MODEL) private readonly latency: LatencyModel,
    @Inject(SCENARIO) scenario: Scenario,
    @Inject(DELIVERY_SINK) private readonly sink: DeliverySink,
    private readonly events: EventLogService,
    private readonly network: NetworkState,
    private readonly metrics: MetricsService,
  ) {
    this.ackTimeoutMs = scenario.ackTimeoutMs;
    this.totalGraphs = scenario.graphs.length;
  }

  /**
   * Enqueue a message to the coordinators. Captures the active trace context so
   * the delivery descends from the sender's span. `origin` is a telemetry-only
   * label for who sent it — it never enters the {@link Delivery} payload.
   * Returns the message id.
   */
  public sendToCoordinators(topic: string, body: Json, origin: string): string {
    const messageId = randomUUID();
    const publishTraceCtx: Record<string, string> = {};
    injectActiveContext(publishTraceCtx);

    const msg: Pending = {
      messageId,
      origin,
      topic,
      body,
      publishTraceCtx,
      publishedAt: this.clock.now(),
      attempts: 0,
      blocked: false,
      duplicateBudget: this.duplicateBudget,
    };
    this.duplicateBudget = 0;
    this.messages.set(messageId, msg);
    // Span for the queue-residency: opened here (child of the sender's active
    // span), closed on first delivery. Captures deliver latency + any block wait.
    msg.queueSpan = serviceTracer(TRANSPORT_SERVICE).startSpan(`transport.enqueue(${topic})`, {
      attributes: { [ATTR.from]: origin, [ATTR.messageId]: messageId, [ATTR.topic]: topic },
    });
    this.events.record({ type: 'transport.published', nodeId: origin, messageId, data: { topic } });
    this.metrics.countMessage('published');

    const deliverIn = this.latency.sample(DELIVER_LATENCY);
    this.metrics.observeDeliverDuration(deliverIn);
    if (deliverIn <= 0) this.dispatch(messageId, false);
    else this.scheduler.after(deliverIn, () => this.dispatch(messageId, false));
    return messageId;
  }

  /** Acknowledge a delivery. Unknown/stale ids are ignored (idempotent). */
  public ack(deliveryId: string): void {
    const messageId = this.byDelivery.get(deliveryId);
    const msg = messageId ? this.messages.get(messageId) : undefined;
    if (!msg || msg.deliveryId !== deliveryId) return;
    // A partitioned (outbound-blocked) coordinator's ack is dropped: the item
    // stays in flight and the visibility timer re-dispatches it elsewhere.
    if (msg.target && this.network.outboundBlocked.has(msg.target)) return;

    if (msg.timer) this.scheduler.cancel(msg.timer);
    this.messages.delete(msg.messageId);
    this.byDelivery.delete(deliveryId);
    this.metrics.inflightRemoved();
    this.metrics.observeAckLatency(this.clock.now() - msg.publishedAt);
    this.events.record({ type: 'transport.acked', messageId: msg.messageId, deliveryId });
    this.metrics.countMessage('acked');
  }

  // --- graph lifetime span ---------------------------------------------------

  /**
   * Open a graph's lifetime span and run `emit` (the graph.start send) under it,
   * so everything the coordinator does for this graph — startGraph, worker.start,
   * the whole task tree — nests beneath one span that stays open until
   * {@link completeGraph}. Mirrors worker.start → worker.execute for tasks.
   */
  public beginGraph(graphId: GraphId, emit: () => void): void {
    const span = tracer.startSpan(`startGraph(${graphId})`, { attributes: { [ATTR.graphId]: graphId } });
    this.graphSpans.set(graphId, span);
    context.with(trace.setSpan(context.active(), span), emit);
  }

  /** Coordinator signalled the graph is done: close its span (end-of-processing). */
  public completeGraph(graphId: GraphId): void {
    const span = this.graphSpans.get(graphId);
    this.graphSpans.delete(graphId);
    this.completedGraphs.add(graphId);
    this.events.record({ type: 'graph.completed', data: { graphId } });
    span?.setStatus({ code: SpanStatusCode.OK });
    span?.end();
    this.graphsCompleteCheck?.();
  }

  /**
   * Resolves 'complete' once every graph has signalled completion, or 'timeout'
   * after `timeoutMs` — whichever comes first. Lets a run end as soon as all
   * processing is done instead of always waiting the full end-condition; the
   * timeout is the safety cap for graphs that stall (e.g. an injected failTask).
   */
  public awaitAllGraphsComplete(timeoutMs: number): Promise<'complete' | 'timeout'> {
    return new Promise((resolve) => {
      const timer = this.scheduler.after(timeoutMs, () => {
        this.graphsCompleteCheck = undefined;
        resolve('timeout');
      });
      this.graphsCompleteCheck = () => {
        if (this.completedGraphs.size < this.totalGraphs) return;
        this.scheduler.cancel(timer);
        this.graphsCompleteCheck = undefined;
        resolve('complete');
      };
      this.graphsCompleteCheck(); // maybe already all done
    });
  }

  // --- fault hooks (invoked by the fault injector) ---------------------------

  /** Emit `extraCopies` duplicate deliveries on the next enqueued message. */
  public scheduleDuplicates(extraCopies: number): void {
    this.duplicateBudget += extraCopies;
  }

  /** Re-dispatch messages parked because no coordinator was reachable. */
  public resumeAll(): void {
    for (const msg of this.messages.values()) {
      if (msg.blocked) this.dispatch(msg.messageId, msg.attempts > 0);
    }
  }

  // --- internals -------------------------------------------------------------

  /** Pick the next coordinator round-robin, skipping inbound-partitioned ones. */
  private pickTarget(): NodeId | undefined {
    const candidates = this.sink.liveNodeIds().filter((id) => !this.network.inboundBlocked.has(id));
    if (candidates.length === 0) return undefined;
    const target = candidates[this.rr % candidates.length];
    this.rr += 1;
    return target;
  }

  private dispatch(messageId: string, redelivery: boolean): void {
    const msg = this.messages.get(messageId);
    if (!msg) return;

    const target = this.pickTarget();
    if (target === undefined) {
      msg.blocked = true;
      this.events.record({ type: 'transport.blocked', messageId: msg.messageId });
      this.metrics.countMessage('blocked');
      return;
    }
    msg.blocked = false;
    msg.attempts += 1;
    const deliveryId = randomUUID();
    msg.deliveryId = deliveryId;
    msg.target = target;
    this.byDelivery.set(deliveryId, messageId);

    // Deliver under the publish context so the coordinator's handler nests under
    // the sender's span. A redelivery opens its own span — the fault-behaviour metric.
    runWithExtractedContext(msg.publishTraceCtx, () =>
      redelivery ? this.emitRedelivery(msg, target, deliveryId) : this.emitDelivery(msg, target, deliveryId, false),
    );

    msg.timer = this.scheduler.after(this.ackTimeoutMs, () => this.onVisibilityTimeout(messageId));
    this.metrics.inflightAdded();
  }

  @Trace({
    name: (msg: Pending) => `transport.redeliver(${msg.topic})`,
    kind: SpanKind.PRODUCER,
    service: TRANSPORT_SERVICE,
  })
  private emitRedelivery(msg: Pending, target: NodeId, deliveryId: string): void {
    annotateSpan({
      [ATTR.from]: msg.origin,
      [ATTR.messageId]: msg.messageId,
      [ATTR.topic]: msg.topic,
      [ATTR.attempt]: msg.attempts,
    });
    this.emitDelivery(msg, target, deliveryId, true);
  }

  private emitDelivery(msg: Pending, target: NodeId, deliveryId: string, redelivery: boolean): void {
    // First real delivery: the message is no longer awaiting delivery — close its
    // queue-residency span (guarded by presence, so block→resume/redelivery are safe).
    if (msg.queueSpan) {
      msg.queueSpan.setStatus({ code: SpanStatusCode.OK });
      msg.queueSpan.end();
      msg.queueSpan = undefined;
    }
    const delivery = this.buildDelivery(msg, deliveryId);
    this.events.record({
      type: redelivery ? 'transport.redelivered' : 'transport.delivered',
      nodeId: target,
      messageId: msg.messageId,
      deliveryId,
      attempt: msg.attempts,
      data: { topic: msg.topic, from: msg.origin },
    });
    this.metrics.countMessage(redelivery ? 'redelivered' : 'delivered');
    const ok = this.sink.deliver(target, delivery);
    if (ok && !redelivery) this.emitDuplicates(msg);
  }

  private onVisibilityTimeout(messageId: string): void {
    const msg = this.messages.get(messageId);
    if (!msg) return; // already acked
    this.metrics.inflightRemoved(); // dispatch re-adds on redelivery
    if (msg.deliveryId) this.byDelivery.delete(msg.deliveryId);
    this.dispatch(messageId, true);
  }

  private emitDuplicates(msg: Pending): void {
    while (msg.duplicateBudget > 0) {
      msg.duplicateBudget -= 1;
      const target = this.pickTarget();
      if (target === undefined) return;
      const dupId = randomUUID();
      const delivery = this.buildDelivery(msg, dupId);
      this.events.record({
        type: 'transport.duplicated',
        nodeId: target,
        messageId: msg.messageId,
        deliveryId: dupId,
        attempt: msg.attempts,
      });
      this.metrics.countMessage('duplicated');
      this.sink.deliver(target, delivery);
    }
  }

  private buildDelivery(msg: Pending, deliveryId: string): Delivery {
    const traceCtx: Record<string, string> = {};
    injectActiveContext(traceCtx);
    return {
      deliveryId,
      messageId: msg.messageId,
      topic: msg.topic,
      body: msg.body,
      attempt: msg.attempts,
      traceCtx,
    };
  }
}
