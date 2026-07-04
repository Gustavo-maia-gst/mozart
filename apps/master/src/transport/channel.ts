import type { ChannelKey, Json, NodeId } from '@mozart/contracts';
import type { CancelHandle } from '../clock/clock';

export interface QueuedMessage {
  messageId: string;
  from: NodeId;
  to: NodeId;
  topic: string;
  body: Json;
  /** Trace context captured at publish; deliveries descend from it. */
  publishTraceCtx: Record<string, string>;
  /** Clock time at publish — used to measure ack round-trip latency. */
  publishedAt: number;
  /** Clock time at/after which the head may be delivered (publish latency). */
  deliverableAt: number;
  /** Delivery count so far (0 until first delivery). */
  attempts: number;
}

export interface Outstanding {
  deliveryId: string;
  timer: CancelHandle;
}

/**
 * One logical FIFO pipe `(from -> to)`. At most one message is outstanding
 * (delivered, awaiting ack) at a time — this single-outstanding rule gives
 * FIFO + at-least-once + head-of-line redelivery with no reordering.
 */
export class Channel {
  readonly queue: QueuedMessage[] = [];
  outstanding?: Outstanding;
  pumpTimer?: CancelHandle;
  /** Extra duplicate copies to emit on the next successful delivery (fault). */
  duplicateBudget = 0;

  constructor(readonly key: ChannelKey) {}

  public get head(): QueuedMessage | undefined {
    return this.queue[0];
  }
}
