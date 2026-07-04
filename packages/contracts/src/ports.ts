import type { NodeId, TaskId } from './ids';
import type { Json, JsonObject } from './json';

/** State persisted for a task in the shared storage S. */
export type TaskState = JsonObject;

/** A task's stored state together with its id — one hit of a storage query. */
export interface TaskMatch {
  taskId: TaskId;
  data: TaskState;
}

/**
 * Equality filter over stored task state: an AND of top-level `attribute ===
 * value` predicates. An empty query matches every task. Values are compared by
 * equality; intended for scalar attributes (`{ status: 'done' }`).
 */
export type StorageQuery = JsonObject;

/**
 * A message pushed to a protocol handler. Deliveries come from the harness
 * transport (at-least-once): the same `messageId` may arrive more than once,
 * each time under a fresh `deliveryId` with `attempt` incremented.
 */
export interface Delivery {
  deliveryId: string;
  messageId: string;
  from: NodeId;
  topic: string;
  body: Json;
  /** 1 on first delivery; incremented on every redelivery/duplicate. */
  attempt: number;
  /** W3C trace-context carrier captured at publish time. */
  traceCtx: Record<string, string>;
}

/**
 * Async at-least-once messaging, FIFO per (from, to) logical channel,
 * ack-based redelivery. Consumption is push-based: the harness invokes
 * `Protocol.onMessage`; the ack is issued when the handler resolves.
 *
 * Abstract class (not interface) so it doubles as a Nest DI token: protocols
 * inject it by type via the constructor, no `@Inject` needed.
 */
export abstract class TransportPort {
  public abstract publish(to: NodeId, topic: string, body: Json): Promise<void>;
}

/**
 * Shared storage S (crash-recovery). During an outage calls do not fail —
 * they block until S recovers. Callers must tolerate arbitrary latency.
 */
export abstract class StoragePort {
  public abstract read(taskId: TaskId): Promise<TaskState | null>;
  /**
   * Snapshot query: return every task whose state matches `query` by equality
   * on each listed attribute (see {@link StorageQuery}). No locking; like
   * `read`, it just blocks under an outage. Order is unspecified.
   */
  public abstract find(query: StorageQuery): Promise<TaskMatch[]>;
  /**
   * Loads the state of `taskId` under mutual exclusion. Blocks until the
   * lock is acquired. The lock is released by `save`/`release` on the handle,
   * or forcibly by the harness if the holding node crashes.
   */
  public abstract readExclusive(taskId: TaskId): Promise<ExclusiveRead>;
  public abstract save(taskId: TaskId, data: TaskState): Promise<void>;
}

export interface ExclusiveRead {
  readonly data: TaskState | null;
  /** Commit new state and release the lock. */
  save(data: TaskState): Promise<void>;
  /** Release the lock without writing (abort). */
  release(): Promise<void>;
}

/**
 * Worker Pool W. `start` is fire-and-forget: completion or failure is
 * notified later as a transport delivery from node `W` (topics
 * `task.completed` / `task.failed`), subject to at-least-once semantics.
 */
export abstract class WorkerPoolPort {
  public abstract start(taskId: TaskId): Promise<void>;
}

/** Topics used by the Worker Pool W when notifying coordinators. */
export const WORKER_TOPICS = {
  completed: 'task.completed',
  failed: 'task.failed',
} as const;

export interface WorkerEventBody extends JsonObject {
  taskId: TaskId;
}

/*
 * Domain events handed to a protocol's handlers. These deliberately expose
 * nothing about the transport: no deliveryId/messageId/attempt/traceCtx. The
 * host routes a raw {@link Delivery} to the right handler and narrows it to one
 * of these views — a `Delivery` is a structural superset of {@link Message},
 * so the same instance flows through and the harness recovers the full delivery
 * (for acking/tracing) with `as unknown as Delivery` when it needs it. Keeping
 * these delivery-free is what lets a protocol stay oblivious to at-least-once
 * plumbing while still being idempotent on the domain identity (`taskId`).
 */

/** A task previously dispatched to the Worker Pool ran to completion. */
export interface WorkerSuccessEvent {
  readonly taskId: TaskId;
}

/** A task previously dispatched to the Worker Pool failed. */
export interface WorkerFailEvent {
  readonly taskId: TaskId;
}

/** A coordinator<->coordinator message. */
export interface Message {
  readonly from: NodeId;
  readonly topic: string;
  readonly body: Json;
}
