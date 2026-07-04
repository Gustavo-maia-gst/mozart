import {
  type Graph,
  type GraphId,
  graphId as idOf,
  type JsonObject,
  type Message,
  type TaskId,
  type WorkerFailEvent,
  type WorkerSuccessEvent,
} from '@mozart/contracts';
import { Injectable } from '@nestjs/common';
import { Protocol } from '../../protocol';

/** Lifecycle of a single task, persisted in S (the slave holds no state). */
type TaskStatus = 'pending' | 'running' | 'complete';

/**
 * Per-task record in S. It carries its own id and adjacency so any coordinator
 * can, from a completion alone, find the dependents and re-check their deps.
 * `kind` discriminates it from the graph meta record.
 */
interface TaskRecord extends JsonObject {
  kind: 'task';
  graphId: GraphId;
  /** Stored as an attribute (not just the key) so it is queryable via an IN filter. */
  taskId: TaskId;
  status: TaskStatus;
  /** Dependencies (in-neighbours): all must be complete before this task starts. */
  deps: TaskId[];
  /** Dependents (out-neighbours): re-checked when this task completes. */
  dependents: TaskId[];
  /** `deps.length`, kept as a scalar so roots are a cheap `depCount: 0` query. */
  depCount: number;
}

/** One record per graph, used only to fire `completeGraph` exactly once. */
interface MetaRecord extends JsonObject {
  kind: 'meta';
  graphId: GraphId;
  completed: boolean;
}

const TASK_KIND = 'task';
const META_KIND = 'meta';

/**
 * Dependency-frontier protocol (pull-based).
 *
 * The edge-driven cousin of the topological barrier: instead of running whole
 * levels behind a barrier, it advances the exact dependency frontier — a task
 * starts the moment *its own* dependencies are all complete, like the baseline,
 * but with the frontier held in S instead of in a leader's memory, so any
 * coordinator can drive any step and a crashed one is simply restarted.
 *
 * Layout in S (keyed by the namespaced task id, plus one meta key per graph):
 *  - persistGraph stores each task with its adjacency (`deps` / `dependents`)
 *    and `status: 'pending'`;
 *  - startGraph dispatches the roots (`depCount: 0`);
 *  - a completion flips its task to `complete`, then for each dependent selects
 *    that dependent's deps (an IN query) and starts it once they are all
 *    complete; a completed leaf triggers the whole-graph completion check.
 *
 * Every handler is idempotent under at-least-once delivery: dependents are
 * re-checked from durable state, only pending→running transitions write, and W
 * dedupes a duplicate start. A failed task is simply retried (re-dispatched).
 */
@Injectable()
export class DependencyFrontierProtocol extends Protocol {
  override readonly name = 'dependency-frontier';

  /** Dispatch the roots — the tasks with no dependencies. */
  public override async startGraph(graphId: GraphId): Promise<void> {
    this.log.info('graph started', { graphId });
    const roots = await this.storage.find({ kind: TASK_KIND, graphId, depCount: 0 });
    for (const { taskId } of roots) await this.startTask(taskId);
  }

  /** This protocol coordinates purely through S; it exchanges no messages. */
  public override async onMessage(event: Message): Promise<void> {
    this.log.warn('unexpected coordinator message', { topic: event.topic });
  }

  /** A task finished: mark it complete, then advance every unblocked dependent. */
  public override async onWorkerSuccess(event: WorkerSuccessEvent): Promise<void> {
    const ex = await this.storage.readExclusive(event.taskId);
    const record = ex.data as TaskRecord | null;
    if (!record) {
      await ex.release(); // unknown task → ack, no-op
      return;
    }
    if (record.status === 'complete')
      await ex.release(); // duplicate → still re-drive below
    else await ex.save({ ...record, status: 'complete' });

    for (const dependent of record.dependents) await this.tryStart(record.graphId, dependent);

    // A leaf's completion can be the graph's last event — re-derive completion.
    if (record.dependents.length === 0) return this.maybeFinish(record.graphId);
  }

  /** A failed task is retried by re-dispatching it (idempotent under redelivery). */
  public override async onWorkerFail(event: WorkerFailEvent): Promise<void> {
    const record = (await this.storage.read(event.taskId)) as TaskRecord | null;
    if (!record || record.status === 'complete') return; // unknown / already done → no-op
    this.log.warn('task failed, retrying', { taskId: event.taskId });
    await this.transport.sendToWorkerPool(event.taskId);
  }

  /** Start `dependent` iff every one of its dependencies is complete. */
  private async tryStart(graphId: GraphId, dependent: TaskId): Promise<void> {
    const record = (await this.storage.read(dependent)) as TaskRecord | null;
    if (record?.status !== 'pending') return; // already running/complete, or unknown
    // IN query: how many of `dependent`'s deps are complete?
    const done = await this.storage.find({ kind: TASK_KIND, graphId, taskId: record.deps, status: 'complete' });
    if (done.length === record.depCount) await this.startTask(dependent);
  }

  /** Move a task pending→running (under lock, never clobbering complete) and send it to W. */
  private async startTask(taskId: TaskId): Promise<void> {
    const ex = await this.storage.readExclusive(taskId);
    const record = ex.data as TaskRecord | null;
    if (!record || record.status === 'complete') {
      await ex.release();
      return;
    }
    if (record.status === 'pending') await ex.save({ ...record, status: 'running' });
    else await ex.release(); // already running — re-dispatch below to re-drive liveness
    this.log.info('start task', { taskId });
    await this.transport.sendToWorkerPool(taskId);
  }

  /** Signal end-of-graph once all tasks are complete (guarded to fire exactly once). */
  private async maybeFinish(graphId: GraphId): Promise<void> {
    const tasks = await this.storage.find({ kind: TASK_KIND, graphId });
    if (!tasks.every((t) => (t.data as TaskRecord).status === 'complete')) return;
    const ex = await this.storage.readExclusive(this.metaKey(graphId));
    if ((ex.data as MetaRecord | null)?.completed) {
      await ex.release();
      return;
    }
    await ex.save({ kind: META_KIND, graphId, completed: true } satisfies MetaRecord);
    this.log.info('graph complete', { graphId });
    await this.transport.completeGraph(graphId);
  }

  private metaKey(graphId: GraphId): string {
    return `frontier-meta:${graphId}`;
  }

  /** Persist each task with its adjacency (edges), all pending. */
  public override async persistGraph(graph: Graph): Promise<void> {
    const gid = idOf(graph);
    const records = graph.mapNodes((taskId): TaskRecord => {
      const deps = graph.inNeighbors(taskId);
      return {
        kind: TASK_KIND,
        graphId: gid,
        taskId,
        status: 'pending',
        deps,
        dependents: graph.outNeighbors(taskId),
        depCount: deps.length,
      };
    });
    for (const record of records) await this.storage.save(record.taskId, record);
    this.log.info('graph persisted', { graphId: gid, tasks: graph.order });
  }
}
