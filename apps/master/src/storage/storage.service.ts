import { randomUUID } from 'node:crypto';
import { Inject, Injectable } from '@nestjs/common';
import { SpanKind, trace } from '@opentelemetry/api';
import type { NodeId, TaskId, TaskState } from '@mozart/contracts';
import { ATTR, TRACER_NAME, withSpan } from '@mozart/telemetry';
import type { LatencyModel } from '@mozart/latency';
import type { Scheduler } from '../clock/clock';
import { EventLogService } from '../event-log/event-log.service';
import { LATENCY_MODEL, SCHEDULER } from '../tokens';
import { StorageGate } from './storage-gate';
import {
  NodeCrashedError,
  STORAGE_ADAPTER,
  type AdapterLease,
  type StorageAdapter,
} from './storage-adapter';

const tracer = trace.getTracer(TRACER_NAME);

interface HeldLease {
  nodeId: NodeId;
  taskId: TaskId;
  lease: AdapterLease;
}

export interface ExclusiveReadResult {
  leaseId: string;
  data: TaskState | null;
}

/**
 * The S facade the coordinators talk to (over IPC). Owns the adapter-agnostic
 * concerns: outage gating, per-op latency, exclusive-lock lease tracking, and
 * — critically — force-release of a crashed node's held AND pending locks.
 */
@Injectable()
export class StorageService {
  private readonly leases = new Map<string, HeldLease>();
  private readonly pendingByNode = new Map<NodeId, Set<AbortController>>();

  constructor(
    @Inject(STORAGE_ADAPTER) private readonly adapter: StorageAdapter,
    @Inject(LATENCY_MODEL) private readonly latency: LatencyModel,
    @Inject(SCHEDULER) private readonly scheduler: Scheduler,
    private readonly gate: StorageGate,
    private readonly events: EventLogService,
  ) {}

  async read(nodeId: NodeId, taskId: TaskId): Promise<TaskState | null> {
    return withSpan(tracer, 'storage.read', spanAttrs(nodeId, taskId), async () => {
      await this.gate.pass(nodeId);
      await this.sleep(this.latency.sample('storage.read'));
      const data = await this.adapter.read(taskId);
      this.events.record({ type: 'storage.read', nodeId, taskId });
      return data;
    });
  }

  async save(nodeId: NodeId, taskId: TaskId, data: TaskState): Promise<void> {
    return withSpan(tracer, 'storage.save', spanAttrs(nodeId, taskId), async () => {
      await this.gate.pass(nodeId);
      await this.sleep(this.latency.sample('storage.save'));
      await this.adapter.save(taskId, data);
      this.events.record({ type: 'storage.save', nodeId, taskId });
    });
  }

  async readExclusive(nodeId: NodeId, taskId: TaskId): Promise<ExclusiveReadResult> {
    return withSpan(tracer, 'storage.readExclusive', spanAttrs(nodeId, taskId), async () => {
      await this.gate.pass(nodeId);
      this.events.record({ type: 'storage.readExclusive.requested', nodeId, taskId });
      await this.sleep(this.latency.sample('storage.readExclusive'));

      const controller = new AbortController();
      this.addPending(nodeId, controller);
      let lease: AdapterLease;
      try {
        lease = await this.adapter.acquire(taskId, controller.signal);
      } finally {
        this.removePending(nodeId, controller);
      }

      const leaseId = randomUUID();
      this.leases.set(leaseId, { nodeId, taskId, lease });
      this.events.record({
        type: 'storage.readExclusive.acquired',
        nodeId,
        taskId,
        data: { leaseId },
      });
      return { leaseId, data: lease.data };
    });
  }

  async leaseSave(leaseId: string, data: TaskState): Promise<void> {
    const held = this.leases.get(leaseId);
    if (!held) return; // stale (e.g. force-released after a crash) — idempotent
    this.leases.delete(leaseId);
    await this.sleep(this.latency.sample('storage.save'));
    await held.lease.save(data);
    this.events.record({ type: 'storage.save', nodeId: held.nodeId, taskId: held.taskId });
    this.events.record({
      type: 'storage.lease.released',
      nodeId: held.nodeId,
      taskId: held.taskId,
      data: { leaseId },
    });
  }

  async leaseRelease(leaseId: string): Promise<void> {
    const held = this.leases.get(leaseId);
    if (!held) return;
    this.leases.delete(leaseId);
    await held.lease.release();
    this.events.record({
      type: 'storage.lease.released',
      nodeId: held.nodeId,
      taskId: held.taskId,
      data: { leaseId },
    });
  }

  /**
   * Force-release everything a crashed node holds or is waiting on. Without
   * this, a SIGKILLed lock holder deadlocks the run.
   */
  async releaseNode(nodeId: NodeId): Promise<void> {
    const pending = this.pendingByNode.get(nodeId);
    if (pending) {
      for (const controller of pending) controller.abort(new NodeCrashedError(nodeId));
      this.pendingByNode.delete(nodeId);
    }
    for (const [leaseId, held] of [...this.leases]) {
      if (held.nodeId !== nodeId) continue;
      this.leases.delete(leaseId);
      await held.lease.release().catch(() => {});
      this.events.record({
        type: 'storage.lease.force-released',
        nodeId,
        taskId: held.taskId,
        data: { leaseId },
      });
    }
  }

  heldLeaseCount(): number {
    return this.leases.size;
  }

  private addPending(nodeId: NodeId, controller: AbortController): void {
    let set = this.pendingByNode.get(nodeId);
    if (!set) {
      set = new Set();
      this.pendingByNode.set(nodeId, set);
    }
    set.add(controller);
  }

  private removePending(nodeId: NodeId, controller: AbortController): void {
    this.pendingByNode.get(nodeId)?.delete(controller);
  }

  private sleep(ms: number): Promise<void> {
    if (ms <= 0) return Promise.resolve();
    return new Promise<void>((resolve) => this.scheduler.after(ms, resolve));
  }
}

function spanAttrs(nodeId: NodeId, taskId: TaskId) {
  return {
    kind: SpanKind.SERVER,
    attributes: { [ATTR.nodeId]: nodeId, [ATTR.taskId]: taskId },
  };
}
