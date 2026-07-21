import type { Json, StorageQuery, TaskId, TaskMatch, TaskState } from '@mozart/contracts';

export const STORAGE_ADAPTER = Symbol('STORAGE_ADAPTER');

/**
 * True iff `data` matches `query` on every listed attribute (empty ⇒ always).
 * A scalar value matches by equality; an **array** value matches by membership
 * (an `IN` filter: `data[key]` must equal one of the elements) — so you can
 * query, e.g., `{ taskId: [a, b, c], status: 'complete' }`.
 */
export function matchesQuery(data: TaskState, query: StorageQuery): boolean {
  return Object.entries(query).every(([key, value]) =>
    Array.isArray(value) ? value.some((v) => jsonEqual(data[key], v)) : jsonEqual(data[key], value),
  );
}

/** Structural equality for JSON values (order-insensitive over object keys). */
function jsonEqual(a: Json | undefined, b: Json | undefined): boolean {
  if (a === b) return true;
  if (a === null || b === null || typeof a !== 'object' || typeof b !== 'object') return false;
  if (Array.isArray(a) || Array.isArray(b)) {
    if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) return false;
    return a.every((el, i) => jsonEqual(el, b[i]));
  }
  const ak = Object.keys(a);
  const bk = Object.keys(b);
  if (ak.length !== bk.length) return false;
  return ak.every((k) => k in b && jsonEqual(a[k], b[k]));
}

/** Thrown when a held/pending lock is force-released because its node crashed. */
export class NodeCrashedError extends Error {
  constructor(nodeId?: string) {
    super(nodeId ? `node ${nodeId} crashed` : 'node crashed');
    this.name = 'NodeCrashedError';
  }
}

/** A held exclusive lock plus the loaded state. */
export interface AdapterLease {
  readonly data: TaskState | null;
  /** Commit new state and release the lock. */
  save(data: TaskState): Promise<void>;
  /** Abort (no write) and release the lock. */
  release(): Promise<void>;
}

/**
 * Storage backend behind the S facade. Adapters implement raw persistence and
 * mutual exclusion; the lease registry, outage gate, latency and event logging
 * live in StorageService (adapter-agnostic).
 */
export interface StorageAdapter {
  read(taskId: TaskId): Promise<TaskState | null>;
  /** Return every stored task whose state matches `query` (see {@link matchesQuery}). */
  find(query: StorageQuery): Promise<TaskMatch[]>;
  save(taskId: TaskId, data: TaskState): Promise<void>;
  /** Remove every record matching `query`; returns the number removed. */
  delete(query: StorageQuery): Promise<number>;
  /**
   * Acquire the exclusive lock for `taskId`, blocking until granted. If
   * `signal` aborts while blocked, the acquisition is cancelled and the promise
   * rejects (with the signal reason) without leaking the lock.
   */
  acquire(taskId: TaskId, signal: AbortSignal): Promise<AdapterLease>;
  init?(): Promise<void>;
  /** Wipe all persisted state (used to clear shared storage at end of run). */
  clear?(): Promise<void>;
  dispose?(): Promise<void>;
}
