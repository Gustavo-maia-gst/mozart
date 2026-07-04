import type { NodeId, Scenario } from '@mozart/contracts';
import type { RpcHandlers } from '@mozart/ipc';
import { Inject, Injectable } from '@nestjs/common';
import { scenarioInfoFor } from '../scenario/scenario';
import { StorageService } from '../storage/storage.service';
import { RUN_ID, SCENARIO } from '../tokens';
import { TransportService } from '../transport/transport.service';
import { WorkerPoolService } from '../worker-pool/worker-pool.service';

/**
 * Single ingress: maps RPC methods from slaves onto the master's services.
 * Handlers receive the originating nodeId from the NodeLink, so one shared
 * handler map serves every node.
 */
@Injectable()
export class IpcHostService {
  /** Set by the process manager to learn when a node completes its handshake. */
  onNodeReady?: (nodeId: NodeId) => void;

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
        return Promise.resolve({ scenario: scenarioInfoFor(this.scenario, this.runId, nodeId) });
      },
      'transport.publish': (nodeId, { to, topic, body }) =>
        Promise.resolve({ messageId: this.transport.publish(nodeId, to, topic, body) }),
      'transport.ack': (_nodeId, { deliveryId }) => {
        this.transport.ack(deliveryId);
        return Promise.resolve({});
      },
      'storage.read': async (nodeId, { taskId }) => ({
        data: await this.storage.read(nodeId, taskId),
      }),
      'storage.find': async (nodeId, { query }) => ({
        matches: await this.storage.find(nodeId, query),
      }),
      'storage.readExclusive': (nodeId, { taskId }) => this.storage.readExclusive(nodeId, taskId),
      'storage.save': async (nodeId, { taskId, data }) => {
        await this.storage.save(nodeId, taskId, data);
        return {};
      },
      'storage.lease.save': async (_nodeId, { leaseId, data }) => {
        await this.storage.leaseSave(leaseId, data);
        return {};
      },
      'storage.lease.release': async (_nodeId, { leaseId }) => {
        await this.storage.leaseRelease(leaseId);
        return {};
      },
      'worker.start': (nodeId, { taskId }) => {
        this.worker.start(nodeId, taskId);
        return Promise.resolve({});
      },
    };
  }
}
