import { type Scenario, type TaskId, WORKER_NODE_ID, WORKER_TOPICS } from '@mozart/contracts';
import type { LatencyModel } from '@mozart/latency';
import { ATTR, TRACER_NAME } from '@mozart/telemetry';
import { Inject, Injectable } from '@nestjs/common';
import { context, type Span, SpanStatusCode, trace } from '@opentelemetry/api';
import type { Clock, Scheduler } from '../clock/clock';
import { EventLogService } from '../event-log/event-log.service';
import { MetricsService } from '../metrics/metrics.service';
import { CLOCK, LATENCY_MODEL, SCENARIO, SCHEDULER } from '../tokens';
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
  /** Dependencies (in-neighbours) per task — passive safety-check input. */
  private readonly deps = new Map<TaskId, TaskId[]>();
  /** Tasks that have completed successfully — a dep is satisfied iff it's here. */
  private readonly completed = new Set<TaskId>();
  /** One live span per running task: opened on start, ended on complete/fail. */
  private readonly spans = new Map<TaskId, Span>();
  /** Clock time of the most recent task completion (for run makespan). */
  private lastCompleteAt?: number;

  // biome-ignore lint/complexity/useMaxParams: deps injection
  constructor(
    @Inject(SCHEDULER) private readonly scheduler: Scheduler,
    @Inject(LATENCY_MODEL) private readonly latency: LatencyModel,
    @Inject(CLOCK) private readonly clock: Clock,
    @Inject(SCENARIO) scenario: Scenario,
    private readonly transport: TransportService,
    private readonly events: EventLogService,
    private readonly metrics: MetricsService,
  ) {
    // Key costs/deps by the runtime (namespaced) task id — the same id worker.start receives.
    for (const graph of scenario.graphs) {
      graph.forEachNode((taskId, attrs) => {
        this.costs.set(taskId, attrs.costMs);
        this.deps.set(taskId, graph.inNeighbors(taskId));
      });
    }
  }

  /** Marks `taskId`'s next execution to fail (one-shot). */
  public failTask(taskId: TaskId): void {
    this.failNext.add(taskId);
  }

  public start(taskId: TaskId): void {
    if (this.running.has(taskId)) {
      // A redundant start for an already-running task (a duplicate/redelivered
      // message that propagated to worker.start). Mark it with a leaf span under
      // the caller's active context (the delivery that caused it) so it's visible
      // in Jaeger next to the worker.execute it was deduped against; then bail.
      tracer.startSpan(`worker.duplicate-start(${taskId})`, { attributes: { [ATTR.taskId]: taskId } }).end();
      this.events.record({ type: 'worker.duplicate-start', taskId });
      this.metrics.countWorkerTask('duplicate-start');
      return;
    }
    // Passive safety check: the harness never blocks execution (it's silent and
    // doesn't participate), but the master is the only place that sees the whole
    // DAG, so it records when a protocol starts a task before all its deps
    // completed — a correctness violation surfaced in the log + a metric.
    const missing = (this.deps.get(taskId) ?? []).filter((dep) => !this.completed.has(dep));
    if (missing.length > 0) {
      tracer.startSpan(`worker.premature-start(${taskId})`, { attributes: { [ATTR.taskId]: taskId } }).end();
      this.events.record({ type: 'worker.premature-start', taskId, data: { missingDeps: missing } });
      this.metrics.countWorkerTask('premature-start');
    }

    this.running.add(taskId);
    this.events.record({ type: 'worker.started', taskId });
    this.metrics.countWorkerTask('started');

    const duration = this.durationFor(taskId);
    this.metrics.observeWorkerTaskDuration(duration);

    // Open a span spanning the whole execution — child of the caller's active
    // context (the coordinator's worker.start), held across the simulated
    // duration and ended on complete/fail below.
    const span = tracer.startSpan(`worker.execute(${taskId})`, { attributes: { [ATTR.taskId]: taskId } });
    this.spans.set(taskId, span);
    this.scheduler.after(duration, () => this.complete(taskId));
  }

  private complete(taskId: TaskId): void {
    this.running.delete(taskId);
    const failed = this.failNext.delete(taskId);
    const span = this.spans.get(taskId);
    this.spans.delete(taskId);
    this.lastCompleteAt = this.clock.now();

    // Publish the completion to the coordinators under the execute span so the
    // handler nests beneath it (one tree across the deliver hop). W addresses the
    // coordinators collectively — it doesn't know or care which one started it.
    // A dep is only "satisfied" by a successful completion (see the start check).
    if (!failed) this.completed.add(taskId);

    const publish = (): void => {
      const topic = failed ? WORKER_TOPICS.failed : WORKER_TOPICS.completed;
      this.transport.sendToCoordinators(topic, { taskId }, WORKER_NODE_ID);
      this.events.record({ type: failed ? 'worker.failed' : 'worker.completed', taskId });
      this.metrics.countWorkerTask(failed ? 'failed' : 'completed');
    };

    if (!span) {
      publish();
      return;
    }
    span.setStatus({ code: failed ? SpanStatusCode.ERROR : SpanStatusCode.OK });
    context.with(trace.setSpan(context.active(), span), publish);
    span.end();
  }

  /** Clock time of the last completion, or undefined if nothing completed. */
  public lastCompletionAt(): number | undefined {
    return this.lastCompleteAt;
  }

  private durationFor(taskId: TaskId): number {
    const cost = this.costs.get(taskId);
    return cost ?? this.latency.sample(TASK_DURATION);
  }
}
