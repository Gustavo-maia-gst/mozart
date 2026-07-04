import type { Delivery, NodeId } from '@mozart/contracts';
import type { NodeLink } from '@mozart/ipc';
import { Injectable, Logger } from '@nestjs/common';
import type { DeliverySink } from '../transport/delivery-sink';

/**
 * Live map of nodeId -> current NodeLink, and the transport's DeliverySink.
 * Keyed by nodeId (never by process handle) so a restarted node's channel is
 * transparently rebound. Dependency-free, which breaks the transport<->ipc
 * cycle (transport injects this as DELIVERY_SINK; nothing here needs transport).
 */
@Injectable()
export class NodeRegistry implements DeliverySink {
  private readonly logger = new Logger(NodeRegistry.name);
  private readonly links = new Map<NodeId, NodeLink>();

  register(link: NodeLink): void {
    this.links.set(link.nodeId, link);
  }

  unregister(nodeId: NodeId): void {
    this.links.delete(nodeId);
  }

  get(nodeId: NodeId): NodeLink | undefined {
    return this.links.get(nodeId);
  }

  liveNodeIds(): NodeId[] {
    return [...this.links.keys()];
  }

  deliver(to: NodeId, delivery: Delivery): boolean {
    const link = this.links.get(to);
    if (!link?.alive) return false; // crashed/absent — transport will retry
    const sent = link.push('delivery', delivery);
    if (!sent) this.logger.warn(`push to ${to} not writable (buffer full or closing)`);
    return sent;
  }
}
