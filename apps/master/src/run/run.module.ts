import { Module } from '@nestjs/common';
import { EventLogModule } from '../event-log/event-log.module';
import { FaultModule } from '../fault/fault.module';
import { IpcServerModule } from '../ipc-server/ipc-server.module';
import { RunService } from './run.service';

@Module({
  imports: [EventLogModule, IpcServerModule, FaultModule],
  providers: [RunService],
  exports: [RunService],
})
export class RunModule {}
