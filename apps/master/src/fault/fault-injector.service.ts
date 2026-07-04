import type { FaultSpec, NodeId, Scenario, TaskId } from '@mozart/contracts';
import { Inject, Injectable, Logger } from '@nestjs/common';
import type { Scheduler } from '../clock/clock';
import { EventLogService } from '../event-log/event-log.service';
import { ProcessManagerService } from '../ipc-server/process-manager.service';
import { StorageGate } from '../storage/storage-gate';
import { SCENARIO, SCHEDULER } from '../tokens';
import { NetworkState } from '../transport/delivery-sink';
import { TransportService } from '../transport/transport.service';
import { WorkerPoolService } from '../worker-pool/worker-pool.service';

/**
 * Injects the scenario's fault schedule and exposes the same actions
 * imperatively (used by e2e tests). Every fault is written to the event log so
 * later correctness analysis can attribute behaviour to the fault that caused it.
 */
@Injectable()
export class FaultInjectorService {
  private readonly logger = new Logger(FaultInjectorService.name);

  // biome-ignore lint/complexity/useMaxParams: deps injection
  constructor(
    @Inject(SCENARIO) private readonly scenario: Scenario,
    @Inject(SCHEDULER) private readonly scheduler: Scheduler,
    private readonly pm: ProcessManagerService,
    private readonly gate: StorageGate,
    private readonly network: NetworkState,
    private readonly transport: TransportService,
    private readonly worker: WorkerPoolService,
    private readonly events: EventLogService,
  ) {}

  /** Schedule all declared faults relative to now (call once, at run start). */
  public arm(): void {
    for (const fault of this.scenario.faults) {
      if ('at' in fault) this.scheduler.after(fault.at, () => this.apply(fault));
      else this.apply(fault); // failTask has no schedule — arm immediately
    }
  }

  private apply(fault: FaultSpec): void {
    const dispatch: { [K in FaultSpec['action']]: (f: Extract<FaultSpec, { action: K }>) => void } = {
      killNode: (f) => this.killNode(f.node, f.restartAfterMs),
      storageOutage: (f) => this.storageOutage(f.scope, f.durationMs),
      partitionNode: (f) => this.partitionNode(f.node, f.durationMs, f.direction),
      duplicateDeliveries: (f) => this.duplicateDeliveries(f.from, f.to, f.extraCopies),
      failTask: (f) => this.failTask(f.taskId),
    };
    (dispatch[fault.action] as (f: FaultSpec) => void)(fault);
  }

  public killNode(nodeId: NodeId, restartAfterMs?: number): void {
    this.record('killNode', { nodeId, restartAfterMs: restartAfterMs ?? null });
    this.pm.kill(nodeId);
    if (restartAfterMs !== undefined) {
      this.scheduler.after(restartAfterMs, () => this.pm.restart(nodeId));
    }
  }

  public storageOutage(scope: 'all' | NodeId, durationMs: number): void {
    this.record('storageOutage', { scope, durationMs });
    this.gate.begin(scope);
    this.events.record({ type: 'storage.outage.begin', data: { scope } });
    this.scheduler.after(durationMs, () => {
      this.gate.end(scope);
      this.events.record({ type: 'storage.outage.end', data: { scope } });
    });
  }

  public partitionNode(nodeId: NodeId, durationMs: number, direction: 'in' | 'out' | 'both'): void {
    this.record('partitionNode', { nodeId, durationMs, direction });
    if (direction === 'in' || direction === 'both') this.network.inboundBlocked.add(nodeId);
    if (direction === 'out' || direction === 'both') this.network.outboundBlocked.add(nodeId);
    this.scheduler.after(durationMs, () => {
      this.network.inboundBlocked.delete(nodeId);
      this.network.outboundBlocked.delete(nodeId);
      this.transport.resumeAll();
    });
  }

  public duplicateDeliveries(from: NodeId, to: NodeId, extraCopies: number): void {
    this.record('duplicateDeliveries', { from, to, extraCopies });
    this.transport.scheduleDuplicates(from, to, extraCopies);
  }

  public failTask(taskId: TaskId): void {
    this.record('failTask', { taskId });
    this.worker.failTask(taskId);
  }

  private record(action: string, data: Record<string, unknown>): void {
    this.logger.log(`fault: ${action} ${JSON.stringify(data)}`);
    this.events.record({ type: 'fault.injected', data: { action, ...data } });
  }
}
