import type { Delivery, NodeId } from '@mozart/contracts';
import { Injectable } from '@nestjs/common';

export const DELIVERY_SINK = Symbol('DELIVERY_SINK');

/**
 * Where the transport hands a delivery to its destination. In the running
 * system this is a NodeLink IPC push; in tests it's a fake. Returns false if
 * the destination is currently unreachable (e.g. crashed) — the transport
 * then relies on the ack-visibility timer to retry.
 */
export interface DeliverySink {
  deliver(to: NodeId, delivery: Delivery): boolean;
  /** Live coordinator ids — the transport's round-robin dispatch targets. */
  liveNodeIds(): NodeId[];
}

/**
 * Shared partition state. A channel `(from -> to)` is blocked if `from`'s
 * outbound or `to`'s inbound is partitioned. Mutated by the fault injector,
 * read by the transport.
 */
@Injectable()
export class NetworkState {
  readonly inboundBlocked = new Set<NodeId>();
  readonly outboundBlocked = new Set<NodeId>();

  public blocks(from: NodeId, to: NodeId): boolean {
    return this.outboundBlocked.has(from) || this.inboundBlocked.has(to);
  }
}
