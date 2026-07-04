import { Module } from '@nestjs/common';
import { EventLogModule } from '../event-log/event-log.module';
import { NetworkState } from './delivery-sink';
import { TransportService } from './transport.service';

/**
 * Note: the DELIVERY_SINK provider is supplied by whoever wires transport to
 * the outside world (the IPC server module, or a test). This module declares
 * transport's own providers and re-exports NetworkState for the fault injector.
 */
@Module({
  imports: [EventLogModule],
  providers: [TransportService, NetworkState],
  exports: [TransportService, NetworkState],
})
export class TransportModule {}
