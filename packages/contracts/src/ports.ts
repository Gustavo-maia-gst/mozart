import type { GraphId } from './graph';
import type { TaskId } from './ids';
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
  topic: string;
  body: Json;
  /** 1 on first delivery; incremented on every redelivery/duplicate. */
  attempt: number;
  /** W3C trace-context carrier captured at publish time. */
  traceCtx: Record<string, string>;
}

/**
 * The two message buses a coordinator talks over. There is no point-to-point
 * addressing: a coordinator only ever talks to *the coordinators* (one logical
 * entity, backed by N interchangeable processes) or to *the worker pool*.
 * Delivery is at-least-once with no ordering and no de-duplication guarantee —
 * the same message may arrive more than once, so handlers must be idempotent.
 *
 * Abstract class (not interface) so it doubles as a Nest DI token: protocols
 * inject it by type via the constructor, no `@Inject` needed.
 */
export abstract class TransportPort {
  /** Send a message to the coordinators (delivered to one of them). */
  public abstract sendToCoordinators(topic: string, body: Json): Promise<void>;
  /** Dispatch a task to the worker pool W for execution. */
  public abstract sendToWorkerPool(taskId: TaskId): Promise<void>;
  /**
   * Special coordinator→master signal: the graph finished coordinating. Closes
   * its lifetime span and lets the harness detect end-of-processing. Not a
   * coordinator-to-coordinator message — a direct word to the harness.
   */
  public abstract completeGraph(graphId: GraphId): Promise<void>;
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

/** Topics used by the Worker Pool W when notifying coordinators. */
export const WORKER_TOPICS = {
  completed: 'task.completed',
  failed: 'task.failed',
} as const;

/** Topics used by the harness to drive coordinators (not protocol messages). */
export const CONTROL_TOPICS = {
  /** Start an already-persisted graph; body is `{ graphId }`. */
  graphStart: 'graph.start',
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
  readonly topic: string;
  readonly body: Json;
}
