import { Inject, Injectable } from '@nestjs/common';
import { context, SpanKind, trace } from '@opentelemetry/api';
import {
  WORKER_NODE_ID,
  WORKER_TOPICS,
  type NodeId,
  type Scenario,
  type TaskId,
} from '@mozart/contracts';
import { ATTR, injectActiveContext, runWithExtractedContext, TRACER_NAME } from '@mozart/telemetry';
import type { LatencyModel } from '@mozart/latency';
import type { Clock, Scheduler } from '../clock/clock';
import { EventLogService } from '../event-log/event-log.service';
import { TransportService } from '../transport/transport.service';
import { LATENCY_MODEL, SCENARIO, SCHEDULER } from '../tokens';

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

  constructor(
    @Inject(SCHEDULER) private readonly scheduler: Scheduler,
    @Inject(LATENCY_MODEL) private readonly latency: LatencyModel,
    @Inject(SCENARIO) scenario: Scenario,
    private readonly transport: TransportService,
    private readonly events: EventLogService,
  ) {
    for (const task of scenario.dag.tasks) this.costs.set(task.id, task.costMs);
  }

  /** Marks `taskId`'s next execution to fail (one-shot). */
  failTask(taskId: TaskId): void {
    this.failNext.add(taskId);
  }

  start(nodeId: NodeId, taskId: TaskId): void {
    if (this.running.has(taskId)) {
      this.events.record({ type: 'worker.duplicate-start', nodeId, taskId });
      return;
    }
    this.running.add(taskId);
    this.events.record({ type: 'worker.started', nodeId, taskId });

    // Capture the caller's context so the eventual completion links back to it.
    const startCtx: Record<string, string> = {};
    injectActiveContext(startCtx);
    this.scheduler.after(this.durationFor(taskId), () => this.complete(nodeId, taskId, startCtx));
  }

  private complete(nodeId: NodeId, taskId: TaskId, startCtx: Record<string, string>): void {
    this.running.delete(taskId);
    const failed = this.failNext.delete(taskId);

    runWithExtractedContext(startCtx, () => {
      const span = tracer.startSpan('worker.execute', {
        kind: SpanKind.INTERNAL,
        attributes: { [ATTR.taskId]: taskId, [ATTR.nodeId]: nodeId },
      });
      context.with(trace.setSpan(context.active(), span), () => {
        const topic = failed ? WORKER_TOPICS.failed : WORKER_TOPICS.completed;
        this.transport.publish(WORKER_NODE_ID, nodeId, topic, { taskId });
        this.events.record({
          type: failed ? 'worker.failed' : 'worker.completed',
          nodeId,
          taskId,
        });
        span.end();
      });
    });
  }

  private durationFor(taskId: TaskId): number {
    const cost = this.costs.get(taskId);
    return cost ?? this.latency.sample(TASK_DURATION);
  }
}
