import { ProtocolLogger, StoragePort, TransportPort, WorkerPoolPort } from '@mozart/contracts';
import { Protocol, resolveProtocol } from '@mozart/protocols';
import { type DynamicModule, Module } from '@nestjs/common';
import { HarnessClientModule } from '../harness-client/harness-client.module';
import { StorageClient, TransportClient, WorkerPoolClient } from '../harness-client/ports';
import { ProtocolHostService } from './protocol-host.service';
import { SpanLogger } from './span-logger';

@Module({})
export class ProtocolHostModule {
  /**
   * Registers the named protocol under the `Protocol` token (Nest instantiates
   * it with full constructor DI) and binds the port tokens to the IPC-backed
   * harness clients.
   */
  static forProtocol(name: string): DynamicModule {
    return {
      module: ProtocolHostModule,
      imports: [HarnessClientModule],
      providers: [
        { provide: Protocol, useClass: resolveProtocol(name) },
        { provide: TransportPort, useExisting: TransportClient },
        { provide: StoragePort, useExisting: StorageClient },
        { provide: WorkerPoolPort, useExisting: WorkerPoolClient },
        { provide: ProtocolLogger, useClass: SpanLogger },
        ProtocolHostService,
      ],
      exports: [ProtocolHostService],
    };
  }
}
