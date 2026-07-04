import {
  PROTOCOL,
  PROTOCOL_LOGGER,
  STORAGE_PORT,
  TRANSPORT_PORT,
  WORKER_POOL_PORT,
} from '@mozart/contracts';
import { resolveProtocol } from '@mozart/protocols';
import { type DynamicModule, Module } from '@nestjs/common';
import { HarnessClientModule } from '../harness-client/harness-client.module';
import { StorageClient, TransportClient, WorkerPoolClient } from '../harness-client/ports';
import { ProtocolHostService } from './protocol-host.service';
import { SpanLogger } from './span-logger';

@Module({})
export class ProtocolHostModule {
  /**
   * Registers the named protocol as a Nest provider (so it is instantiated with
   * full DI — its ports injected as properties) and binds the port tokens to the
   * IPC-backed harness clients.
   */
  static forProtocol(name: string): DynamicModule {
    const ProtocolClass = resolveProtocol(name);
    return {
      module: ProtocolHostModule,
      imports: [HarnessClientModule],
      providers: [
        ProtocolClass,
        { provide: PROTOCOL, useExisting: ProtocolClass },
        { provide: TRANSPORT_PORT, useExisting: TransportClient },
        { provide: STORAGE_PORT, useExisting: StorageClient },
        { provide: WORKER_POOL_PORT, useExisting: WorkerPoolClient },
        { provide: PROTOCOL_LOGGER, useClass: SpanLogger },
        ProtocolHostService,
      ],
      exports: [ProtocolHostService],
    };
  }
}
