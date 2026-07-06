import type { StorageQuery, TaskId, TaskMatch, TaskState } from '@mozart/contracts';
import { Pool } from 'pg';
import { type AdapterLease, NodeCrashedError, type StorageAdapter } from './storage-adapter';

const UPSERT = `insert into task_state(task_id, data, version) values ($1, $2, 0)
  on conflict (task_id) do update set data = excluded.data, version = task_state.version + 1`;

/**
 * Build the SQL WHERE clause + bound params for a {@link StorageQuery}. Scalar
 * attributes go through jsonb containment (@>), which is attribute-equality and
 * can use a GIN index on data. Array-valued attributes are IN filters:
 * `data->>key = ANY(list)` (key/list are bound params, never interpolated). An
 * empty query yields `data @> '{}'`, which matches everything.
 */
function whereClause(query: StorageQuery): { where: string; params: unknown[] } {
  const containment: StorageQuery = {};
  const conds: string[] = [];
  const params: unknown[] = [];
  for (const [key, value] of Object.entries(query)) {
    if (Array.isArray(value)) {
      params.push(key, value.map(String));
      conds.push(`data ->> $${params.length - 1} = ANY($${params.length}::text[])`);
    } else {
      containment[key] = value;
    }
  }
  params.push(containment);
  conds.push(`data @> $${params.length}`);
  return { where: conds.join(' and '), params };
}

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

  public async init(): Promise<void> {
    await this.pool.query(
      `create table if not exists task_state (
         task_id text primary key,
         data jsonb not null,
         version bigint not null default 0
       )`,
    );
  }

  public async dispose(): Promise<void> {
    await this.pool.end();
  }

  public async read(taskId: TaskId): Promise<TaskState | null> {
    const r = await this.pool.query<{ data: TaskState }>('select data from task_state where task_id = $1', [taskId]);
    return r.rows[0]?.data ?? null;
  }

  public async find(query: StorageQuery): Promise<TaskMatch[]> {
    const { where, params } = whereClause(query);
    const r = await this.pool.query<{ task_id: TaskId; data: TaskState }>(
      `select task_id, data from task_state where ${where}`,
      params,
    );
    return r.rows.map((row) => ({ taskId: row.task_id, data: row.data }));
  }

  public async save(taskId: TaskId, data: TaskState): Promise<void> {
    await this.pool.query(UPSERT, [taskId, data]);
  }

  public async delete(query: StorageQuery): Promise<number> {
    const { where, params } = whereClause(query);
    const r = await this.pool.query(`delete from task_state where ${where}`, params);
    return r.rowCount ?? 0;
  }

  public async acquire(taskId: TaskId, signal: AbortSignal): Promise<AdapterLease> {
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
      const r = await client.query<{ data: TaskState }>('select data from task_state where task_id = $1 for update', [
        taskId,
      ]);
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
