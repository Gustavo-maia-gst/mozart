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
export class TransportClient implements TransportPort {
  constructor(@Inject(IPC_CLIENT) private readonly ipc: IpcClient) {}

  @Trace({ name: (_to, topic) => `transport.publish(${topic})`, kind: SpanKind.PRODUCER })
  public async publish(to: NodeId, topic: string, body: Json): Promise<void> {
    annotateSpan({ [ATTR.topic]: topic });
    await this.ipc.call('transport.publish', { to, topic, body });
  }
}

@Injectable()
export class WorkerPoolClient implements WorkerPoolPort {
  constructor(@Inject(IPC_CLIENT) private readonly ipc: IpcClient) {}

  @Trace({ name: (taskId) => `worker.start(${taskId})`, kind: SpanKind.CLIENT })
  public async start(taskId: TaskId): Promise<void> {
    await this.ipc.call('worker.start', { taskId });
  }
}

@Injectable()
export class StorageClient implements StoragePort {
  constructor(@Inject(IPC_CLIENT) private readonly ipc: IpcClient) {}

  @Trace({ name: (taskId) => `storage.read(${taskId})`, kind: SpanKind.CLIENT })
  public async read(taskId: TaskId): Promise<TaskState | null> {
    const { data } = await this.ipc.call('storage.read', { taskId });
    return data;
  }

  @Trace({ name: 'storage.find', kind: SpanKind.CLIENT })
  public async find(query: StorageQuery): Promise<TaskMatch[]> {
    const { matches } = await this.ipc.call('storage.find', { query });
    return matches;
  }

  @Trace({ name: (taskId) => `storage.save(${taskId})`, kind: SpanKind.CLIENT })
  public async save(taskId: TaskId, data: TaskState): Promise<void> {
    await this.ipc.call('storage.save', { taskId, data });
  }

  @Trace({ name: (taskId) => `storage.readExclusive(${taskId})`, kind: SpanKind.CLIENT })
  public async readExclusive(taskId: TaskId): Promise<ExclusiveRead> {
    const r = await this.ipc.call('storage.readExclusive', { taskId });
    return new RemoteExclusiveRead(this.ipc, r.leaseId, r.data);
  }
}

/** Handle over a held lease; save/release map to lease RPCs. */
class RemoteExclusiveRead implements ExclusiveRead {
  constructor(
    private readonly ipc: IpcClient,
    private readonly leaseId: string,
    readonly data: TaskState | null,
  ) {}

  @Trace({ name: 'storage.lease.save', kind: SpanKind.CLIENT })
  public async save(data: TaskState): Promise<void> {
    await this.ipc.call('storage.lease.save', { leaseId: this.leaseId, data });
  }

  @Trace({ name: 'storage.lease.release', kind: SpanKind.CLIENT })
  public async release(): Promise<void> {
    await this.ipc.call('storage.lease.release', { leaseId: this.leaseId });
  }
}
