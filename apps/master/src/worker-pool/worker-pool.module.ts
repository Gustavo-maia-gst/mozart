import { Module } from '@nestjs/common';
import { EventLogModule } from '../event-log/event-log.module';
import { TransportModule } from '../transport/transport.module';
import { WorkerPoolService } from './worker-pool.service';

@Module({
  imports: [TransportModule, EventLogModule],
  providers: [WorkerPoolService],
  exports: [WorkerPoolService],
})
export class WorkerPoolModule {}
