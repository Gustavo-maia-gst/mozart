import { randomUUID } from 'node:crypto';
import type { NodeId, StorageQuery, TaskId, TaskMatch, TaskState } from '@mozart/contracts';
import type { LatencyModel } from '@mozart/latency';
import { Inject, Injectable } from '@nestjs/common';
import type { Clock, Scheduler } from '../clock/clock';
import { EventLogService } from '../event-log/event-log.service';
import { MetricsService } from '../metrics/metrics.service';
import { CLOCK, LATENCY_MODEL, SCHEDULER } from '../tokens';
import { type AdapterLease, NodeCrashedError, STORAGE_ADAPTER, type StorageAdapter } from './storage-adapter';
import { StorageGate } from './storage-gate';

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

  // biome-ignore lint/complexity/useMaxParams: deps injection
  constructor(
    @Inject(STORAGE_ADAPTER) private readonly adapter: StorageAdapter,
    @Inject(LATENCY_MODEL) private readonly latency: LatencyModel,
    @Inject(SCHEDULER) private readonly scheduler: Scheduler,
    @Inject(CLOCK) private readonly clock: Clock,
    private readonly gate: StorageGate,
    private readonly events: EventLogService,
    private readonly metrics: MetricsService,
  ) {}

  public async read(nodeId: NodeId, taskId: TaskId): Promise<TaskState | null> {
    await this.gate.pass(nodeId);
    const ms = this.latency.sample('storage.read');
    this.metrics.observeStorageOpDuration('read', ms);
    await this.sleep(ms);
    const data = await this.adapter.read(taskId);
    this.events.record({ type: 'storage.read', nodeId, taskId });
    this.metrics.countStorageOp('read');
    return data;
  }

  public async find(nodeId: NodeId, query: StorageQuery): Promise<TaskMatch[]> {
    await this.gate.pass(nodeId);
    const ms = this.latency.sample('storage.find');
    this.metrics.observeStorageOpDuration('find', ms);
    await this.sleep(ms);
    const matches = await this.adapter.find(query);
    this.events.record({ type: 'storage.find', nodeId, data: { query, count: matches.length } });
    this.metrics.countStorageOp('find');
    return matches;
  }

  public async save(nodeId: NodeId, taskId: TaskId, data: TaskState): Promise<void> {
    await this.gate.pass(nodeId);
    const ms = this.latency.sample('storage.save');
    this.metrics.observeStorageOpDuration('save', ms);
    await this.sleep(ms);
    await this.adapter.save(taskId, data);
    this.events.record({ type: 'storage.save', nodeId, taskId });
    this.metrics.countStorageOp('save');
  }

  public async delete(nodeId: NodeId, query: StorageQuery): Promise<number> {
    await this.gate.pass(nodeId);
    const ms = this.latency.sample('storage.delete');
    this.metrics.observeStorageOpDuration('delete', ms);
    await this.sleep(ms);
    const deleted = await this.adapter.delete(query);
    this.events.record({ type: 'storage.delete', nodeId, data: { query, deleted } });
    this.metrics.countStorageOp('delete');
    return deleted;
  }

  public async readExclusive(nodeId: NodeId, taskId: TaskId): Promise<ExclusiveReadResult> {
    await this.gate.pass(nodeId);
    this.events.record({ type: 'storage.readExclusive.requested', nodeId, taskId });
    const ms = this.latency.sample('storage.readExclusive');
    this.metrics.observeStorageOpDuration('readExclusive', ms);
    this.metrics.countStorageOp('readExclusive');
    await this.sleep(ms);

    const controller = new AbortController();
    this.addPending(nodeId, controller);
    let lease: AdapterLease;
    const acquireStart = this.clock.now();
    try {
      lease = await this.adapter.acquire(taskId, controller.signal);
    } finally {
      this.removePending(nodeId, controller);
    }
    // Real lock-contention wait (adapter mutex / row lock), not injected latency.
    this.metrics.observeLockWait(this.clock.now() - acquireStart);

    const leaseId = randomUUID();
    this.leases.set(leaseId, { nodeId, taskId, lease });
    this.metrics.leaseAcquired();
    this.events.record({
      type: 'storage.readExclusive.acquired',
      nodeId,
      taskId,
      data: { leaseId },
    });
    return { leaseId, data: lease.data };
  }

  public async leaseSave(leaseId: string, data: TaskState): Promise<void> {
    const held = this.leases.get(leaseId);
    if (!held) return; // stale (e.g. force-released after a crash) — idempotent
    this.leases.delete(leaseId);
    const ms = this.latency.sample('storage.save');
    this.metrics.observeStorageOpDuration('save', ms);
    await this.sleep(ms);
    await held.lease.save(data);
    this.metrics.leaseReleased();
    this.events.record({ type: 'storage.save', nodeId: held.nodeId, taskId: held.taskId });
    this.metrics.countStorageOp('save');
    this.events.record({
      type: 'storage.lease.released',
      nodeId: held.nodeId,
      taskId: held.taskId,
      data: { leaseId },
    });
  }

  public async leaseRelease(leaseId: string): Promise<void> {
    const held = this.leases.get(leaseId);
    if (!held) return;
    this.leases.delete(leaseId);
    await held.lease.release();
    this.metrics.leaseReleased();
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
  public async releaseNode(nodeId: NodeId): Promise<void> {
    const pending = this.pendingByNode.get(nodeId);
    if (pending) {
      for (const controller of pending) controller.abort(new NodeCrashedError(nodeId));
      this.pendingByNode.delete(nodeId);
    }
    for (const [leaseId, held] of [...this.leases]) {
      if (held.nodeId !== nodeId) continue;
      this.leases.delete(leaseId);
      await held.lease.release().catch(() => {});
      this.metrics.leaseReleased();
      this.events.record({
        type: 'storage.lease.force-released',
        nodeId,
        taskId: held.taskId,
        data: { leaseId },
      });
    }
  }

  public heldLeaseCount(): number {
    return this.leases.size;
  }

  /** Wipe all persisted state — used to clear shared storage at end of run. */
  public async clear(): Promise<void> {
    await this.adapter.clear?.();
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
