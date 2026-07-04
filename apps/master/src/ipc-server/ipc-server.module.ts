import { Module } from '@nestjs/common';
import { EventLogModule } from '../event-log/event-log.module';
import { StorageModule } from '../storage/storage.module';
import { TransportModule } from '../transport/transport.module';
import { WorkerPoolModule } from '../worker-pool/worker-pool.module';
import { DeliveryModule } from './delivery.module';
import { IpcHostService } from './ipc-host.service';
import { ProcessManagerService } from './process-manager.service';

@Module({
  imports: [DeliveryModule, TransportModule, StorageModule, WorkerPoolModule, EventLogModule],
  providers: [IpcHostService, ProcessManagerService],
  exports: [ProcessManagerService, IpcHostService],
})
export class IpcServerModule {}
