import { Module } from '@nestjs/common';
import { IpcClient, processFrameChannel } from '@mozart/ipc';
import { traceContextHooks } from '@mozart/telemetry';
import { IPC_CLIENT } from '../tokens';
import { StorageClient, TransportClient, WorkerPoolClient } from './ports';

/**
 * Provides the single IpcClient to the parent (master) and the three ports
 * backed by it. Telemetry hooks inject the active client-span context into
 * every outgoing frame, so master-side server spans parent correctly.
 */
@Module({
  providers: [
    { provide: IPC_CLIENT, useFactory: () => new IpcClient(processFrameChannel(), traceContextHooks()) },
    TransportClient,
    StorageClient,
    WorkerPoolClient,
  ],
  exports: [IPC_CLIENT, TransportClient, StorageClient, WorkerPoolClient],
})
export class HarnessClientModule {}
