import { Inject, Injectable } from '@nestjs/common';
import { SpanKind, trace } from '@opentelemetry/api';
import type {
  ExclusiveRead,
  Json,
  NodeId,
  StoragePort,
  TaskId,
  TaskState,
  TransportPort,
  WorkerPoolPort,
} from '@mozart/contracts';
import type { IpcClient } from '@mozart/ipc';
import { ATTR, TRACER_NAME, withSpan } from '@mozart/telemetry';
import { IPC_CLIENT } from '../tokens';

const tracer = trace.getTracer(TRACER_NAME);
const CLIENT = { kind: SpanKind.CLIENT } as const;

/** All ports are thin, traced wrappers over the master RPC. */

@Injectable()
export class TransportClient implements TransportPort {
  constructor(@Inject(IPC_CLIENT) private readonly ipc: IpcClient) {}

  async publish(to: NodeId, topic: string, body: Json): Promise<void> {
    await withSpan(
      tracer,
      'transport.publish',
      { kind: SpanKind.PRODUCER, attributes: { [ATTR.topic]: topic } },
      () => this.ipc.call('transport.publish', { to, topic, body }),
    );
  }
}

@Injectable()
export class WorkerPoolClient implements WorkerPoolPort {
  constructor(@Inject(IPC_CLIENT) private readonly ipc: IpcClient) {}

  async start(taskId: TaskId): Promise<void> {
    await withSpan(tracer, 'worker.start', { ...CLIENT, attributes: { [ATTR.taskId]: taskId } }, () =>
      this.ipc.call('worker.start', { taskId }),
    );
  }
}

@Injectable()
export class StorageClient implements StoragePort {
  constructor(@Inject(IPC_CLIENT) private readonly ipc: IpcClient) {}

  read(taskId: TaskId): Promise<TaskState | null> {
    return withSpan(tracer, 'storage.read', { ...CLIENT, attributes: { [ATTR.taskId]: taskId } }, () =>
      this.ipc.call('storage.read', { taskId }).then((r) => r.data),
    );
  }

  save(taskId: TaskId, data: TaskState): Promise<void> {
    return withSpan(
      tracer,
      'storage.save',
      { ...CLIENT, attributes: { [ATTR.taskId]: taskId } },
      async () => {
        await this.ipc.call('storage.save', { taskId, data });
      },
    );
  }

  readExclusive(taskId: TaskId): Promise<ExclusiveRead> {
    return withSpan(
      tracer,
      'storage.readExclusive',
      { ...CLIENT, attributes: { [ATTR.taskId]: taskId } },
      () =>
        this.ipc
          .call('storage.readExclusive', { taskId })
          .then((r) => new RemoteExclusiveRead(this.ipc, r.leaseId, r.data)),
    );
  }
}

/** Handle over a held lease; save/release map to lease RPCs. */
class RemoteExclusiveRead implements ExclusiveRead {
  constructor(
    private readonly ipc: IpcClient,
    private readonly leaseId: string,
    readonly data: TaskState | null,
  ) {}

  save(data: TaskState): Promise<void> {
    return withSpan(tracer, 'storage.lease.save', CLIENT, async () => {
      await this.ipc.call('storage.lease.save', { leaseId: this.leaseId, data });
    });
  }

  release(): Promise<void> {
    return withSpan(tracer, 'storage.lease.release', CLIENT, async () => {
      await this.ipc.call('storage.lease.release', { leaseId: this.leaseId });
    });
  }
}
