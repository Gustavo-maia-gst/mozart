import { Module } from '@nestjs/common';
import { ClockModule } from '../clock/clock';
import { EventLogService } from './event-log.service';

@Module({
  imports: [ClockModule],
  providers: [EventLogService],
  exports: [EventLogService],
})
export class EventLogModule {}
