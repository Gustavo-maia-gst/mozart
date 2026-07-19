import { Module } from '@nestjs/common';
import { EventLogModule } from '../event-log/event-log.module';
import { IpcServerModule } from '../ipc-server/ipc-server.module';
import { StorageModule } from '../storage/storage.module';
import { TransportModule } from '../transport/transport.module';
import { WorkerPoolModule } from '../worker-pool/worker-pool.module';
import { FaultInjectorService } from './fault-injector.service';
import { FaultTriggerService } from './fault-trigger.service';

@Module({
  imports: [IpcServerModule, StorageModule, TransportModule, WorkerPoolModule, EventLogModule],
  providers: [FaultTriggerService, FaultInjectorService],
  exports: [FaultInjectorService, FaultTriggerService],
})
export class FaultModule {}
