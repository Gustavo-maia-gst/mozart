import { Pool } from 'pg';
import type { TaskId, TaskState } from '@mozart/contracts';
import { NodeCrashedError, type AdapterLease, type StorageAdapter } from './storage-adapter';

const UPSERT = `insert into task_state(task_id, data, version) values ($1, $2, 0)
  on conflict (task_id) do update set data = excluded.data, version = task_state.version + 1`;

/**
 * Postgres S. Exclusive locks use a dedicated pooled client running
 * `BEGIN; SELECT ... FOR UPDATE`; the master owns all connections, so an
 * explicit lease release (COMMIT/ROLLBACK) is required — advisory session
 * auto-release would never fire since the master's connection survives a slave
 * crash. Pending FOR UPDATE waits are cancelled via pg_cancel_backend.
 */
export class PostgresStorageAdapter implements StorageAdapter {
  private readonly pool: Pool;

  constructor(connectionString: string, max = 20) {
    this.pool = new Pool({ connectionString, max });
  }

  async init(): Promise<void> {
    await this.pool.query(
      `create table if not exists task_state (
         task_id text primary key,
         data jsonb not null,
         version bigint not null default 0
       )`,
    );
  }

  async dispose(): Promise<void> {
    await this.pool.end();
  }

  async read(taskId: TaskId): Promise<TaskState | null> {
    const r = await this.pool.query<{ data: TaskState }>(
      'select data from task_state where task_id = $1',
      [taskId],
    );
    return r.rows[0]?.data ?? null;
  }

  async save(taskId: TaskId, data: TaskState): Promise<void> {
    await this.pool.query(UPSERT, [taskId, data]);
  }

  async acquire(taskId: TaskId, signal: AbortSignal): Promise<AdapterLease> {
    const client = await this.pool.connect();
    const pid = (await client.query<{ pid: number }>('select pg_backend_pid() as pid')).rows[0]!.pid;
    const onAbort = (): void => {
      void this.pool.query('select pg_cancel_backend($1)', [pid]).catch(() => {});
    };
    signal.addEventListener('abort', onAbort, { once: true });

    try {
      if (signal.aborted) throw (signal.reason as Error) ?? new NodeCrashedError();
      await client.query('begin');
      // Transaction-scoped advisory lock keyed on the task id: gives mutual
      // exclusion even when the row does not exist yet (plain FOR UPDATE locks
      // nothing on a missing row). Auto-released on COMMIT/ROLLBACK = our lease.
      await client.query('select pg_advisory_xact_lock(hashtext($1))', [taskId]);
      const r = await client.query<{ data: TaskState }>(
        'select data from task_state where task_id = $1 for update',
        [taskId],
      );
      signal.removeEventListener('abort', onAbort);

      let done = false;
      const finish = async (sql: 'commit' | 'rollback'): Promise<void> => {
        if (done) return;
        done = true;
        try {
          await client.query(sql);
        } finally {
          client.release();
        }
      };
      return {
        data: r.rows[0]?.data ?? null,
        save: async (data) => {
          await client.query(UPSERT, [taskId, data]);
          await finish('commit');
        },
        release: () => finish('rollback'),
      };
    } catch (err) {
      signal.removeEventListener('abort', onAbort);
      await client.query('rollback').catch(() => {});
      client.release();
      throw err;
    }
  }
}
