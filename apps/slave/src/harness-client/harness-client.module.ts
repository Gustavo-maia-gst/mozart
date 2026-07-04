import { IpcClient, processFrameChannel } from '@mozart/ipc';
import { traceContextHooks } from '@mozart/telemetry';
import { Module } from '@nestjs/common';
import { IPC_CLIENT } from '../tokens';
import { StorageClient, TransportClient } from './ports';

/**
 * Provides the single IpcClient to the parent (master) and the ports backed by
 * it. Telemetry hooks inject the active client-span context into every outgoing
 * frame, so master-side server spans parent correctly.
 */
@Module({
  providers: [
    {
      provide: IPC_CLIENT,
      useFactory: () => new IpcClient(processFrameChannel(), traceContextHooks()),
    },
    TransportClient,
    StorageClient,
  ],
  exports: [IPC_CLIENT, TransportClient, StorageClient],
})
export class HarnessClientModule {}
