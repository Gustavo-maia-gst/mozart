import { Module } from '@nestjs/common';
import { EventLogModule } from '../event-log/event-log.module';
import { RunService } from './run.service';

@Module({
  imports: [EventLogModule],
  providers: [RunService],
  exports: [RunService],
})
export class RunModule {}
