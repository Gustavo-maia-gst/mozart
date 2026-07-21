import type { StorageQuery, TaskId, TaskMatch, TaskState } from '@mozart/contracts';
import { Mutex, type MutexInterface } from 'async-mutex';
import { type AdapterLease, matchesQuery, NodeCrashedError, type StorageAdapter } from './storage-adapter';

/** In-memory S: a map plus one mutex per task for readExclusive. */
export class InMemoryStorageAdapter implements StorageAdapter {
  private readonly store = new Map<TaskId, TaskState>();
  private readonly mutexes = new Map<TaskId, Mutex>();

  /** Wipe all state (end-of-run cleanup; mostly a no-op since the process is ending). */
  public clear(): Promise<void> {
    this.store.clear();
    this.mutexes.clear();
    return Promise.resolve();
  }

  public read(taskId: TaskId): Promise<TaskState | null> {
    return Promise.resolve(this.clone(this.store.get(taskId)));
  }

  public find(query: StorageQuery): Promise<TaskMatch[]> {
    const matches: TaskMatch[] = [];
    for (const [taskId, data] of this.store) {
      if (matchesQuery(data, query)) matches.push({ taskId, data: this.clone(data)! });
    }
    return Promise.resolve(matches);
  }

  public save(taskId: TaskId, data: TaskState): Promise<void> {
    this.store.set(taskId, this.clone(data)!);
    return Promise.resolve();
  }

  public delete(query: StorageQuery): Promise<number> {
    let deleted = 0;
    for (const [taskId, data] of this.store) {
      if (matchesQuery(data, query)) {
        this.store.delete(taskId);
        deleted++;
      }
    }
    return Promise.resolve(deleted);
  }

  public async acquire(taskId: TaskId, signal: AbortSignal): Promise<AdapterLease> {
    const mutex = this.mutexFor(taskId);
    const releaser = await this.acquireAbortable(mutex, signal);
    let released = false;
    const release = (): Promise<void> => {
      if (!released) {
        released = true;
        releaser();
      }
      return Promise.resolve();
    };
    return {
      data: this.clone(this.store.get(taskId)),
      save: async (data) => {
        this.store.set(taskId, this.clone(data)!);
        await release();
      },
      release,
    };
  }

  private mutexFor(taskId: TaskId): Mutex {
    let mutex = this.mutexes.get(taskId);
    if (!mutex) {
      mutex = new Mutex();
      this.mutexes.set(taskId, mutex);
    }
    return mutex;
  }

  /** Acquire the mutex but reject (and self-release if it lands late) on abort. */
  private acquireAbortable(mutex: Mutex, signal: AbortSignal): Promise<MutexInterface.Releaser> {
    return new Promise<MutexInterface.Releaser>((resolve, reject) => {
      let settled = false;
      const onAbort = (): void => {
        if (settled) return;
        settled = true;
        reject((signal.reason as Error) ?? new NodeCrashedError());
      };
      if (signal.aborted) {
        onAbort();
        return;
      }
      signal.addEventListener('abort', onAbort, { once: true });
      mutex
        .acquire()
        .then((releaser) => {
          signal.removeEventListener('abort', onAbort);
          if (settled) {
            releaser(); // aborted first — don't leak the lock
            return;
          }
          settled = true;
          resolve(releaser);
        })
        .catch(reject);
    });
  }

  private clone(v: TaskState | undefined): TaskState | null {
    return v === undefined ? null : (JSON.parse(JSON.stringify(v)) as TaskState);
  }
}
