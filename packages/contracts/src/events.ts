import type { JsonObject } from './json';

/**
 * Event-log taxonomy. The master appends one record per occurrence to
 * `runs/<runId>/events.jsonl`; `seq` is a master-side monotonic counter, so
 * the log is totally ordered. This log (plus master spans) is the ground
 * truth for later metric/correctness analysis.
 */
export const EVENT_TYPES = [
  'run.started',
  'run.finished',
  'node.spawned',
  'node.ready',
  'node.killed',
  'node.exited',
  'node.restarted',
  'transport.published',
  'transport.delivered',
  'transport.acked',
  'transport.redelivered',
  'transport.duplicated',
  'transport.blocked',
  'storage.read',
  'storage.find',
  'storage.readExclusive.requested',
  'storage.readExclusive.acquired',
  'storage.save',
  'storage.delete',
  'storage.lease.released',
  'storage.lease.force-released',
  'storage.outage.begin',
  'storage.outage.end',
  'worker.started',
  'worker.duplicate-start',
  'worker.premature-start',
  'worker.completed',
  'worker.failed',
  'graph.completed',
  'fault.injected',
] as const;

export type EventType = (typeof EVENT_TYPES)[number];

export interface HarnessEvent {
  /** Master wall-clock ms. */
  ts: number;
  /** Monotonic per-run counter — total order over all events. */
  seq: number;
  type: EventType;
  runId: string;
  nodeId?: string;
  channel?: string;
  taskId?: string;
  messageId?: string;
  deliveryId?: string;
  attempt?: number;
  traceId?: string;
  spanId?: string;
  data?: JsonObject;
}
