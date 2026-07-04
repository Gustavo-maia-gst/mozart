import { Module } from '@nestjs/common';
import { EventLogModule } from '../event-log/event-log.module';
import { FaultModule } from '../fault/fault.module';
import { IpcServerModule } from '../ipc-server/ipc-server.module';
import { StorageModule } from '../storage/storage.module';
import { TransportModule } from '../transport/transport.module';
import { WorkerPoolModule } from '../worker-pool/worker-pool.module';
import { ActivationService } from './activation.service';
import { RunService } from './run.service';

@Module({
  imports: [EventLogModule, IpcServerModule, FaultModule, WorkerPoolModule, TransportModule, StorageModule],
  providers: [RunService, ActivationService],
  exports: [RunService],
})
export class RunModule {}
