import { type NodeId, type Scenario, type TaskId, WORKER_NODE_ID, WORKER_TOPICS } from '@mozart/contracts';
import type { LatencyModel } from '@mozart/latency';
import { ATTR, TRACER_NAME } from '@mozart/telemetry';
import { Inject, Injectable } from '@nestjs/common';
import { context, type Span, SpanStatusCode, trace } from '@opentelemetry/api';
import type { Scheduler } from '../clock/clock';
import { EventLogService } from '../event-log/event-log.service';
import { LATENCY_MODEL, SCENARIO, SCHEDULER } from '../tokens';
import { TransportService } from '../transport/transport.service';

const tracer = trace.getTracer(TRACER_NAME);
const TASK_DURATION = 'worker.taskDuration';

/**
 * Simulated Worker Pool W. `start` is fire-and-forget: after a sampled task
 * duration W publishes a `task.completed` (or `task.failed`) event back to the
 * starting coordinator through the transport, inheriting at-least-once/FIFO.
 */
@Injectable()
export class WorkerPoolService {
  private readonly running = new Set<TaskId>();
  private readonly failNext = new Set<TaskId>();
  private readonly costs = new Map<TaskId, number | undefined>();
  /** One live span per running task: opened on start, ended on complete/fail. */
  private readonly spans = new Map<TaskId, Span>();

  constructor(
    @Inject(SCHEDULER) private readonly scheduler: Scheduler,
    @Inject(LATENCY_MODEL) private readonly latency: LatencyModel,
    @Inject(SCENARIO) scenario: Scenario,
    private readonly transport: TransportService,
    private readonly events: EventLogService,
  ) {
    // Key costs by the runtime (namespaced) task id — the same id worker.start receives.
    for (const graph of scenario.graphs) {
      for (const task of graph.tasks) this.costs.set(task.id, task.costMs);
    }
  }

  /** Marks `taskId`'s next execution to fail (one-shot). */
  public failTask(taskId: TaskId): void {
    this.failNext.add(taskId);
  }

  public start(nodeId: NodeId, taskId: TaskId): void {
    if (this.running.has(taskId)) {
      this.events.record({ type: 'worker.duplicate-start', nodeId, taskId });
      return;
    }
    this.running.add(taskId);
    this.events.record({ type: 'worker.started', nodeId, taskId });

    // Open a span spanning the whole execution — child of the caller's active
    // context (the coordinator's worker.start), held across the simulated
    // duration and ended on complete/fail below.
    const span = tracer.startSpan(`worker.execute(${taskId})`, {
      attributes: { [ATTR.taskId]: taskId, [ATTR.nodeId]: nodeId },
    });
    this.spans.set(taskId, span);
    this.scheduler.after(this.durationFor(taskId), () => this.complete(nodeId, taskId));
  }

  private complete(nodeId: NodeId, taskId: TaskId): void {
    this.running.delete(taskId);
    const failed = this.failNext.delete(taskId);
    const span = this.spans.get(taskId);
    this.spans.delete(taskId);

    // Publish the completion under the execute span so the coordinator's
    // onMessage nests beneath it (one tree across the deliver hop).
    const publish = (): void => {
      const topic = failed ? WORKER_TOPICS.failed : WORKER_TOPICS.completed;
      this.transport.publish(WORKER_NODE_ID, nodeId, topic, { taskId });
      this.events.record({ type: failed ? 'worker.failed' : 'worker.completed', nodeId, taskId });
    };

    if (!span) {
      publish();
      return;
    }
    span.setStatus({ code: failed ? SpanStatusCode.ERROR : SpanStatusCode.OK });
    context.with(trace.setSpan(context.active(), span), publish);
    span.end();
  }

  private durationFor(taskId: TaskId): number {
    const cost = this.costs.get(taskId);
    return cost ?? this.latency.sample(TASK_DURATION);
  }
}
