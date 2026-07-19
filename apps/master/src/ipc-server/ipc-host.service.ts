import type { FaultHook, NodeId, Scenario } from '@mozart/contracts';
import type { RpcHandlers } from '@mozart/ipc';
import { Inject, Injectable } from '@nestjs/common';
import { StorageService } from '../storage/storage.service';
import { RUN_ID, SCENARIO } from '../tokens';
import { TransportService } from '../transport/transport.service';
import { WorkerPoolService } from '../worker-pool/worker-pool.service';

/** Returns true if it killed `ctx.node` (the caller must not run the guarded effect). */
export type FaultTriggerFn = (
  hook: FaultHook,
  phase: 'before' | 'after',
  ctx: { message: unknown; topic?: string; node: NodeId },
) => boolean;

/**
 * Single ingress: maps RPC methods from slaves onto the master's services.
 * Handlers receive the originating nodeId from the NodeLink, so one shared
 * handler map serves every node.
 */
@Injectable()
export class IpcHostService {
  /** Set by the process manager to learn when a node completes its handshake. */
  onNodeReady?: (nodeId: NodeId) => void;
  /** Set by FaultTriggerService; gates the RPCs that map to a fault hook. */
  trigger?: FaultTriggerFn;

  constructor(
    @Inject(SCENARIO) private readonly scenario: Scenario,
    @Inject(RUN_ID) private readonly runId: string,
    private readonly transport: TransportService,
    private readonly storage: StorageService,
    private readonly worker: WorkerPoolService,
  ) {}

  public buildHandlers(): RpcHandlers {
    return {
      'node.ready': (nodeId) => {
        this.onNodeReady?.(nodeId);
        return Promise.resolve({ scenario: this.scenario.infoFor(nodeId, this.runId) });
      },
      'transport.toCoordinators': (nodeId, { topic, body }) =>
        this.guard('SendMessage', nodeId, body, topic, async () => {
          this.transport.sendToCoordinators(topic, body, nodeId);
          return {};
        }),
      'transport.toWorkerPool': (nodeId, { taskId }) =>
        this.guard('SendToWorker', nodeId, { taskId }, undefined, async () => {
          this.worker.start(taskId);
          return {};
        }),
      'transport.ack': (_nodeId, { deliveryId }) => {
        this.transport.ack(deliveryId);
        return Promise.resolve({});
      },
      'transport.completeGraph': (_nodeId, { graphId }) => {
        this.transport.completeGraph(graphId);
        return Promise.resolve({});
      },
      'storage.read': (nodeId, { taskId }) =>
        this.guard('StorageRead', nodeId, { taskId }, undefined, async () => ({
          data: await this.storage.read(nodeId, taskId),
        })),
      'storage.find': (nodeId, { query }) =>
        this.guard('StorageFind', nodeId, { query }, undefined, async () => ({
          matches: await this.storage.find(nodeId, query),
        })),
      'storage.readExclusive': (nodeId, { taskId }) =>
        this.guard('StorageReadExclusive', nodeId, { taskId }, undefined, () =>
          this.storage.readExclusive(nodeId, taskId),
        ),
      'storage.save': (nodeId, { taskId, data }) =>
        this.guard('StorageSave', nodeId, { taskId, data }, undefined, async () => {
          await this.storage.save(nodeId, taskId, data);
          return {};
        }),
      'storage.delete': (nodeId, { query }) =>
        this.guard('StorageDelete', nodeId, { query }, undefined, async () => ({
          deleted: await this.storage.delete(nodeId, query),
        })),
      'storage.lease.save': async (_nodeId, { leaseId, data }) => {
        await this.storage.leaseSave(leaseId, data);
        return {};
      },
      'storage.lease.release': async (_nodeId, { leaseId }) => {
        await this.storage.leaseRelease(leaseId);
        return {};
      },
    };
  }

  /**
   * Wraps one RPC with its fault hook: `before` may kill `nodeId` and skip
   * `run` entirely (the effect never happens); `after` runs the real effect
   * first, then may kill `nodeId` before the (never-observed — the channel is
   * already dead) response would go out.
   */
  private async guard<T>(
    hook: FaultHook,
    nodeId: NodeId,
    message: unknown,
    topic: string | undefined,
    run: () => Promise<T>,
  ): Promise<T> {
    if (this.trigger?.(hook, 'before', { message, topic, node: nodeId })) {
      // Node is already dead (kill is synchronous) — this value is never sent.
      return {} as T;
    }
    const res = await run();
    this.trigger?.(hook, 'after', { message, topic, node: nodeId });
    return res;
  }
}
