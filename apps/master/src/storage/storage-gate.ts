import type { NodeId } from '@mozart/contracts';
import { Injectable } from '@nestjs/common';

interface Waiter {
  nodeId: NodeId;
  resolve: () => void;
}

/**
 * Models S's crash-recovery outages. During an outage, storage calls do NOT
 * fail — they PARK here and resume when S recovers (the honest simulation: a
 * caller simply blocks). Outage scope is either global ('all') or a single
 * node id (that node alone can't see S).
 */
@Injectable()
export class StorageGate {
  private globalOutage = false;
  private readonly nodeOutage = new Set<NodeId>();
  private waiters: Waiter[] = [];

  affected(nodeId: NodeId): boolean {
    return this.globalOutage || this.nodeOutage.has(nodeId);
  }

  /** Resolves immediately if S is available to `nodeId`, else when it recovers. */
  pass(nodeId: NodeId): Promise<void> {
    if (!this.affected(nodeId)) return Promise.resolve();
    return new Promise<void>((resolve) => this.waiters.push({ nodeId, resolve }));
  }

  begin(scope: 'all' | NodeId): void {
    if (scope === 'all') this.globalOutage = true;
    else this.nodeOutage.add(scope);
  }

  end(scope: 'all' | NodeId): void {
    if (scope === 'all') this.globalOutage = false;
    else this.nodeOutage.delete(scope);
    this.releaseUnblocked();
  }

  private releaseUnblocked(): void {
    const remaining: Waiter[] = [];
    for (const w of this.waiters) {
      if (this.affected(w.nodeId)) remaining.push(w);
      else w.resolve();
    }
    this.waiters = remaining;
  }
}
