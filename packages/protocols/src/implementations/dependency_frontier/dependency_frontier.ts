import {
  Graph,
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

/** Per-task record in S: its adjacency plus status. */
interface TaskRecord extends JsonObject {
  kind: 'task';
  graphId: GraphId;
  taskId: TaskId;
  status: TaskStatus;
  /** Dependencies (in-neighbours): all must be complete before this task starts. */
  deps: TaskId[];
  /** Dependents (out-neighbours): re-checked when this task completes. */
  dependents: TaskId[];
}

const TASK_KIND = 'task';

/** Coordinator→coordinator announcement: a task completed; advance its dependents. */
const FINISHED_TOPIC = 'task.finished';
interface FinishedBody extends JsonObject {
  taskId: TaskId;
}

/**
 * Dependency-frontier protocol (pull-based) — the naive, unhardened reference
 * implementation, kept as a research baseline to measure what the hardening in
 * {@link MonotonicDependencyFrontierProtocol} actually buys.
 *
 * Frontier semantics: a task runs once all its own dependencies are complete
 * (finer than the topological barrier, same as the baseline). Completions are
 * announced with a `task.finished` message; whichever coordinator picks it up
 * advances the frontier. Implemented the obvious way, with none of the
 * distributed safety machinery:
 *  - **no exclusive locks** — plain `read` + `save`, so concurrent coordinators
 *    can race on the same record (lost updates, double dispatch);
 *  - **no edge records / delete** — a dependent's readiness is recomputed with a
 *    single `find` over its dependencies each time;
 *  - **no completion guard** — the whole-graph check runs on *every* completion
 *    (O(tasks) each) and `completeGraph` may fire more than once.
 *
 * It is correct on the happy path (single coordinator, no crashes, no
 * duplicates) and intentionally fragile otherwise — that fragility is the point
 * of the comparison against the hardened version.
 */
@Injectable()
export class DependencyFrontierProtocol extends Protocol {
  override readonly name = 'dependency-frontier';

  /** Persist each task with its adjacency, all pending. */
  public override async persistGraph(graph: Graph): Promise<void> {
    const gid = idOf(graph);
    const records = graph.mapNodes(
      (taskId): TaskRecord => ({
        kind: TASK_KIND,
        graphId: gid,
        taskId,
        status: 'pending',
        deps: graph.inNeighbors(taskId),
        dependents: graph.outNeighbors(taskId),
      }),
    );
    for (const record of records) await this.storage.save(record.taskId, record);
    this.log.info('graph persisted', { graphId: gid, tasks: graph.order });
  }

  /** Dispatch the roots — the tasks with no dependencies. */
  public override async startGraph(graphId: GraphId): Promise<void> {
    this.log.info('graph started', { graphId });
    const tasks = await this.storage.find({ kind: TASK_KIND, graphId });
    for (const { data } of tasks) {
      const record = data as TaskRecord;
      if (record.deps.length === 0) await this.startTask(record.taskId);
    }
  }

  /** A `task.finished` announcement: advance the finished task's dependents. */
  public override async onMessage(event: Message): Promise<void> {
    if (event.topic !== FINISHED_TOPIC) {
      this.log.warn('unexpected coordinator message', { topic: event.topic });
      return;
    }
    const { taskId } = event.body as FinishedBody;
    const record = (await this.storage.read(taskId)) as TaskRecord | null;
    if (!record) return;
    // One select brings every dependent record (instead of a read per dependent).
    const dependents = await this.storage.find({ kind: TASK_KIND, graphId: record.graphId, taskId: record.dependents });
    for (const { data } of dependents) await this.tryStart(data as TaskRecord);
    await this.maybeFinish(record.graphId);
  }

  /** A task finished: mark it complete, then announce it so a coordinator advances its dependents. */
  public override async onWorkerSuccess(event: WorkerSuccessEvent): Promise<void> {
    const record = (await this.storage.read(event.taskId)) as TaskRecord | null;
    if (!record) return; // unknown task → ack, no-op
    await this.storage.save(event.taskId, { ...record, status: 'complete' });
    await this.transport.sendToCoordinators(FINISHED_TOPIC, { taskId: event.taskId } satisfies FinishedBody);
  }

  /** A failed task is retried by re-dispatching it. */
  public override async onWorkerFail(event: WorkerFailEvent): Promise<void> {
    const record = (await this.storage.read(event.taskId)) as TaskRecord | null;
    if (!record || record.status === 'complete') return;
    this.log.warn('task failed, retrying', { taskId: event.taskId });
    await this.transport.sendToWorkerPool(event.taskId);
  }

  /** Start `dependent` iff every one of its dependencies is complete — one select brings them all. */
  private async tryStart(dependent: TaskRecord): Promise<void> {
    if (dependent.status !== 'pending') return;
    // Single query: the dependencies (by id) that are already complete.
    const done = await this.storage.find({
      kind: TASK_KIND,
      graphId: dependent.graphId,
      taskId: dependent.deps,
      status: 'complete',
    });
    if (done.length === dependent.deps.length) await this.startTask(dependent.taskId);
  }

  /** Move a task to running and send it to W — no lock, so a race can dispatch twice (W dedupes). */
  private async startTask(taskId: TaskId): Promise<void> {
    const record = (await this.storage.read(taskId)) as TaskRecord | null;
    if (record?.status !== 'pending') return;
    await this.storage.save(taskId, { ...record, status: 'running' });
    this.log.info('start task', { taskId });
    await this.transport.sendToWorkerPool(taskId);
  }

  /** Signal end-of-graph when every task is complete — recomputed on each completion, may double-fire. */
  private async maybeFinish(graphId: GraphId): Promise<void> {
    const tasks = await this.storage.find({ kind: TASK_KIND, graphId });
    if (tasks.every((t) => (t.data as TaskRecord).status === 'complete')) {
      this.log.info('graph complete', { graphId });
      await this.transport.completeGraph(graphId);
    }
  }
}
