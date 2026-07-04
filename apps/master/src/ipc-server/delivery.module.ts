import { Module } from '@nestjs/common';
import { DELIVERY_SINK } from '../transport/delivery-sink';
import { NodeRegistry } from './node-registry';

/**
 * Provides the NodeRegistry and exposes it as the transport's DELIVERY_SINK.
 * Kept separate (and dependency-free) so TransportModule can import it without
 * pulling in the IPC server / process manager (which depend on the transport).
 */
@Module({
  providers: [NodeRegistry, { provide: DELIVERY_SINK, useExisting: NodeRegistry }],
  exports: [NodeRegistry, DELIVERY_SINK],
})
export class DeliveryModule {}
