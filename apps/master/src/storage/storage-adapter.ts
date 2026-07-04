import type { TaskId, TaskState } from '@mozart/contracts';

export const STORAGE_ADAPTER = Symbol('STORAGE_ADAPTER');

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
  save(taskId: TaskId, data: TaskState): Promise<void>;
  /**
   * Acquire the exclusive lock for `taskId`, blocking until granted. If
   * `signal` aborts while blocked, the acquisition is cancelled and the promise
   * rejects (with the signal reason) without leaking the lock.
   */
  acquire(taskId: TaskId, signal: AbortSignal): Promise<AdapterLease>;
  init?(): Promise<void>;
  dispose?(): Promise<void>;
}
