import type {
  ExclusiveRead,
  Json,
  NodeId,
  StoragePort,
  StorageQuery,
  TaskId,
  TaskMatch,
  TaskState,
  TransportPort,
  WorkerPoolPort,
} from '@mozart/contracts';
import type { IpcClient } from '@mozart/ipc';
import { annotateSpan, ATTR, SpanKind, Trace } from '@mozart/telemetry';
import { Inject, Injectable } from '@nestjs/common';
import { IPC_CLIENT } from '../tokens';

/**
 * All ports are thin, traced wrappers over the master RPC. `@Trace` opens the
 * span; `nodeId`/`taskId` are stamped from the ambient trace scope the harness
 * opens per dispatch, so callers don't thread them through.
 */

@Injectable()
@Trace({ name: 'transport', kind: SpanKind.PRODUCER })
export class TransportClient implements TransportPort {
  constructor(@Inject(IPC_CLIENT) private readonly ipc: IpcClient) {}

  public async publish(to: NodeId, topic: string, body: Json): Promise<void> {
    annotateSpan({ [ATTR.topic]: topic });
    await this.ipc.call('transport.publish', { to, topic, body });
  }
}

@Injectable()
@Trace({ name: 'worker', kind: SpanKind.CLIENT })
export class WorkerPoolClient implements WorkerPoolPort {
  constructor(@Inject(IPC_CLIENT) private readonly ipc: IpcClient) {}

  public async start(taskId: TaskId): Promise<void> {
    annotateSpan({ [ATTR.taskId]: taskId });
    await this.ipc.call('worker.start', { taskId });
  }
}

@Injectable()
@Trace({ name: 'storage', kind: SpanKind.CLIENT })
export class StorageClient implements StoragePort {
  constructor(@Inject(IPC_CLIENT) private readonly ipc: IpcClient) {}

  public async read(taskId: TaskId): Promise<TaskState | null> {
    const { data } = await this.ipc.call('storage.read', { taskId });
    return data;
  }

  public async find(query: StorageQuery): Promise<TaskMatch[]> {
    const { matches } = await this.ipc.call('storage.find', { query });
    return matches;
  }

  public async save(taskId: TaskId, data: TaskState): Promise<void> {
    await this.ipc.call('storage.save', { taskId, data });
  }

  public async readExclusive(taskId: TaskId): Promise<ExclusiveRead> {
    const r = await this.ipc.call('storage.readExclusive', { taskId });
    return new RemoteExclusiveRead(this.ipc, r.leaseId, r.data);
  }
}

/** Handle over a held lease; save/release map to lease RPCs. */
@Trace({ name: 'storage.lease', kind: SpanKind.CLIENT })
class RemoteExclusiveRead implements ExclusiveRead {
  constructor(
    private readonly ipc: IpcClient,
    private readonly leaseId: string,
    readonly data: TaskState | null,
  ) {}

  public async save(data: TaskState): Promise<void> {
    await this.ipc.call('storage.lease.save', { leaseId: this.leaseId, data });
  }

  public async release(): Promise<void> {
    await this.ipc.call('storage.lease.release', { leaseId: this.leaseId });
  }
}
