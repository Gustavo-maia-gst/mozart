import { TRACER_NAME } from '@mozart/telemetry';
import { Injectable } from '@nestjs/common';
import { type Counter, type Histogram, metrics, type UpDownCounter } from '@opentelemetry/api';

/** Bounded label values — kept small on purpose (cardinality discipline). */
export type MessageType = 'published' | 'delivered' | 'redelivered' | 'duplicated' | 'acked' | 'blocked';
export type StorageOp = 'read' | 'find' | 'save' | 'readExclusive' | 'delete';
export type TaskOutcome = 'started' | 'completed' | 'failed' | 'duplicate-start' | 'premature-start';
export type FaultAction =
  | 'killNode'
  | 'storageOutage'
  | 'partitionNode'
  | 'duplicateDeliveries'
  | 'failTask'
  | 'conditionalKill';
export type NodeLifecycle = 'spawned' | 'ready' | 'killed' | 'exited' | 'restarted';

/**
 * Owns every OTel metric instrument. Lives in the master (the single choke point
 * all effects pass through) and is called right next to the existing
 * `events.record(...)` sites. Grouping dimensions (protocol/run_id/scenario/seed
 * /latency params) are NOT per-point labels — they ride as resource attributes
 * set at telemetry init. The typed methods below are the cardinality guard: the
 * only attributes a caller can attach are the bounded enums above (never
 * taskId/messageId/deliveryId/channel).
 */
@Injectable()
export class MetricsService {
  // Histograms (exponential via telemetry Views: names end in .duration/.latency/.wait/.makespan).
  private readonly deliverDuration: Histogram;
  private readonly storageOpDuration: Histogram;
  private readonly workerTaskDuration: Histogram;
  private readonly ackLatency: Histogram;
  private readonly lockWait: Histogram;
  private readonly makespan: Histogram;
  private readonly criticalPath: Histogram;

  // Counters.
  private readonly messages: Counter;
  private readonly storageOps: Counter;
  private readonly workerTasks: Counter;
  private readonly faults: Counter;
  private readonly nodeLifecycle: Counter;

  // UpDownCounters (mutated at the same choke points as the events — drift-free).
  private readonly leasesHeld: UpDownCounter;
  private readonly inflight: UpDownCounter;

  constructor() {
    const meter = metrics.getMeter(TRACER_NAME);

    this.deliverDuration = meter.createHistogram('mozart.transport.deliver.duration', {
      unit: 'ms',
      description: 'Simulated communication (publish→deliverable) latency, exact sampled value.',
    });
    this.storageOpDuration = meter.createHistogram('mozart.storage.op.duration', {
      unit: 'ms',
      description: 'Simulated storage op latency, exact sampled value.',
    });
    this.workerTaskDuration = meter.createHistogram('mozart.worker.task.duration', {
      unit: 'ms',
      description: 'Simulated worker task duration (scenario cost or sampled).',
    });
    this.ackLatency = meter.createHistogram('mozart.transport.ack.latency', {
      unit: 'ms',
      description: 'Measured publish→ack round-trip (real clock delta).',
    });
    this.lockWait = meter.createHistogram('mozart.storage.lock.wait', {
      unit: 'ms',
      description: 'Measured exclusive-lock acquire wait (real clock delta).',
    });
    this.makespan = meter.createHistogram('mozart.dag.makespan', {
      unit: 'ms',
      description: 'End-to-end run makespan (activation→last completion).',
    });
    this.criticalPath = meter.createHistogram('mozart.dag.critical_path', {
      unit: 'ms',
      description: 'Theoretical makespan floor: max cost-weighted path (+ start offset) across graphs.',
    });

    this.messages = meter.createCounter('mozart.messages', { description: 'Transport messages by lifecycle type.' });
    this.storageOps = meter.createCounter('mozart.storage.ops', { description: 'Storage operations by op.' });
    this.workerTasks = meter.createCounter('mozart.worker.tasks', { description: 'Worker tasks by outcome.' });
    this.faults = meter.createCounter('mozart.faults', { description: 'Injected faults by action.' });
    this.nodeLifecycle = meter.createCounter('mozart.node.lifecycle', { description: 'Node lifecycle events.' });

    this.leasesHeld = meter.createUpDownCounter('mozart.storage.leases.held', {
      description: 'Currently held exclusive-lock leases.',
    });
    this.inflight = meter.createUpDownCounter('mozart.transport.inflight', {
      description: 'Currently outstanding (delivered, unacked) messages.',
    });
  }

  // --- histograms ------------------------------------------------------------
  public observeDeliverDuration(ms: number): void {
    this.deliverDuration.record(ms);
  }
  public observeStorageOpDuration(op: StorageOp, ms: number): void {
    this.storageOpDuration.record(ms, { op });
  }
  public observeWorkerTaskDuration(ms: number): void {
    this.workerTaskDuration.record(ms);
  }
  public observeAckLatency(ms: number): void {
    this.ackLatency.record(ms);
  }
  public observeLockWait(ms: number): void {
    this.lockWait.record(ms);
  }
  public observeMakespan(ms: number): void {
    this.makespan.record(ms);
  }
  public observeCriticalPath(ms: number): void {
    this.criticalPath.record(ms);
  }

  // --- counters --------------------------------------------------------------
  public countMessage(type: MessageType): void {
    this.messages.add(1, { type });
  }
  public countStorageOp(op: StorageOp): void {
    this.storageOps.add(1, { op });
  }
  public countWorkerTask(outcome: TaskOutcome): void {
    this.workerTasks.add(1, { outcome });
  }
  public countFault(action: FaultAction): void {
    this.faults.add(1, { action });
  }
  public countNodeLifecycle(event: NodeLifecycle): void {
    this.nodeLifecycle.add(1, { event });
  }

  // --- up/down counters ------------------------------------------------------
  public leaseAcquired(): void {
    this.leasesHeld.add(1);
  }
  public leaseReleased(): void {
    this.leasesHeld.add(-1);
  }
  public inflightAdded(): void {
    this.inflight.add(1);
  }
  public inflightRemoved(): void {
    this.inflight.add(-1);
  }
}
