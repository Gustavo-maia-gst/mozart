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
 * Per-task record in S. It carries only its id, status and dependents — the
 * *dependencies* live as separate edge records, so "is this task unblocked?" is
 * answered by the absence of edges pointing at it, not by a stored counter.
 */
interface TaskRecord extends JsonObject {
  kind: 'task';
  graphId: GraphId;
  /** Stored as an attribute (not just the key) so it is queryable via an IN filter. */
  taskId: TaskId;
  status: TaskStatus;
  /** Dependents (out-neighbours): re-checked when this task completes. */
  dependents: TaskId[];
}

/**
 * One dependency edge `source -> target` (target depends on source). It is
 * *consumed* (deleted) when its source completes; a task with no remaining
 * inbound edge has all its dependencies done and is ready to run.
 */
interface EdgeRecord extends JsonObject {
  kind: 'edge';
  graphId: GraphId;
  source: TaskId;
  target: TaskId;
}

/** One record per graph, used only to fire `completeGraph` exactly once. */
interface MetaRecord extends JsonObject {
  kind: 'meta';
  graphId: GraphId;
  completed: boolean;
}

const TASK_KIND = 'task';
const EDGE_KIND = 'edge';
const META_KIND = 'meta';

/**
 * Monotonic dependency-frontier protocol (pull-based, edge-consuming) — the
 * hardened version of {@link DependencyFrontierProtocol}.
 *
 * Same frontier semantics as its naive sibling — a task runs the moment *its
 * own* dependencies are all done — but the frontier is held in S so any
 * coordinator can drive any step and a crashed one is simply restarted.
 *
 * The frontier is encoded as edges, Kahn-style: each dependency is one
 * `source -> target` edge record. A task is ready exactly when no edge targets
 * it. On completion a task deletes its outbound edges (`delete where source =
 * me`) and starts any dependent that now has no inbound edge left.
 *
 * Layout in S (keyed by the namespaced task id; edges under `edge:src->tgt`):
 *  - persistGraph stores each task with its `dependents` and one edge per dep;
 *  - startGraph dispatches the roots (tasks that are no edge's target);
 *  - a completion flips its task to `complete`, deletes its outbound edges, then
 *    starts each dependent left with no inbound edge; a completed leaf triggers
 *    the whole-graph completion check.
 *
 * Every handler is idempotent under at-least-once delivery: edge deletion and
 * the readiness re-check run from durable state, only pending→running
 * transitions write, and W dedupes a duplicate start. A failed task is retried.
 */
@Injectable()
export class MonotonicDependencyFrontierProtocol extends Protocol {
  override readonly name = 'monotonic-dependency-frontier';

  /** Persist each task (with its dependents) and one edge record per dependency. */
  public override async persistGraph(graph: Graph): Promise<void> {
    const gid = idOf(graph);
    const tasks = graph.mapNodes(
      (taskId): TaskRecord => ({
        kind: TASK_KIND,
        graphId: gid,
        taskId,
        status: 'pending',
        dependents: graph.outNeighbors(taskId),
      }),
    );
    for (const task of tasks) await this.storage.save(task.taskId, task);
    // Edges run dep -> dependent (source must complete before target).
    for (const { source, target } of graph.mapEdges((_e, _a, source, target) => ({ source, target }))) {
      const edge: EdgeRecord = { kind: EDGE_KIND, graphId: gid, source, target };
      await this.storage.save(this.edgeKey(source, target), edge);
    }
    this.log.info('graph persisted', { graphId: gid, tasks: graph.order, edges: graph.size });
  }

  /** Dispatch the roots — the tasks no edge points at. */
  public override async startGraph(graphId: GraphId): Promise<void> {
    this.log.info('graph started', { graphId });
    const [tasks, edges] = await Promise.all([
      this.storage.find({ kind: TASK_KIND, graphId }),
      this.storage.find({ kind: EDGE_KIND, graphId }),
    ]);
    const blocked = new Set(edges.map((e) => (e.data as EdgeRecord).target));
    for (const { taskId } of tasks) if (!blocked.has(taskId)) await this.startTask(taskId);
  }

  /** This protocol coordinates purely through S; it exchanges no messages. */
  public override async onMessage(event: Message): Promise<void> {
    this.log.warn('unexpected coordinator message', { topic: event.topic });
  }

  /** A task finished: mark it complete, consume its edges, then start freed dependents. */
  public override async onWorkerSuccess(event: WorkerSuccessEvent): Promise<void> {
    const ex = await this.storage.readExclusive(event.taskId);
    const record = ex.data as TaskRecord | null;
    if (!record) {
      await ex.release(); // unknown task → ack, no-op
      return;
    }

    if (record.status === 'complete') {
      await ex.release(); // duplicate → still re-drive below
    } else {
      // Consume this node's outbound edges, then start any dependent left unblocked.
      await this.storage.delete({ kind: EDGE_KIND, graphId: record.graphId, source: event.taskId });

      await ex.save({ ...record, status: 'complete' });
    }

    const inbound = await this.storage.find({ kind: EDGE_KIND, graphId: record.graphId, target: record.dependents });
    const blocked = new Set(inbound.map((e) => (e.data as EdgeRecord).target));
    for (const dependent of record.dependents) if (!blocked.has(dependent)) await this.startTask(dependent);

    // A leaf's completion can be the graph's last event — re-derive completion.
    if (record.dependents.length === 0) await this.maybeFinish(record.graphId);
  }

  /** A failed task is retried by re-dispatching it (idempotent under redelivery). */
  public override async onWorkerFail(event: WorkerFailEvent): Promise<void> {
    const record = (await this.storage.read(event.taskId)) as TaskRecord | null;
    if (!record || record.status === 'complete') return; // unknown / already done → no-op
    this.log.warn('task failed, retrying', { taskId: event.taskId });
    await this.transport.sendToWorkerPool(event.taskId);
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

  private edgeKey(source: TaskId, target: TaskId): string {
    return `edge:${source}->${target}`;
  }

  private metaKey(graphId: GraphId): string {
    return `frontier-meta:${graphId}`;
  }
}
