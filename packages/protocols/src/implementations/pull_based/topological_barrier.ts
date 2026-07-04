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

/** Per-task record in S. `kind` discriminates it from the graph meta record. */
interface TaskRecord extends JsonObject {
  kind: 'task';
  graphId: GraphId;
  /** Topological level, computed once in {@link TopologicalBarrierProtocol.persistGraph}. */
  order: number;
  status: TaskStatus;
}

/** One record per graph, used only to fire `completeGraph` exactly once. */
interface MetaRecord extends JsonObject {
  kind: 'meta';
  graphId: GraphId;
  completed: boolean;
}

const TASK_KIND = 'task';
const META_KIND = 'meta';

/** Coordinator→coordinator message: "every task up to `order-1` is done, start `order`". */
const ADVANCE_TOPIC = 'barrier.advance';
interface AdvanceBody extends JsonObject {
  graphId: GraphId;
  order: number;
}

/**
 * Topological-barrier protocol (pull-based).
 *
 * Runs a graph one topological *level* at a time behind a global barrier: every
 * task at order N must complete before any task at order N+1 starts. That is
 * stricter than honouring each task's own dependencies (the baseline), but it's
 * trivially safe and needs no in-memory frontier — all state lives in S, so any
 * coordinator can drive any step and a crashed one is simply restarted.
 *
 * Layout in S (keyed by the namespaced task id, plus one meta key per graph):
 *  - persistGraph computes each task's topological order once and writes a
 *    {@link TaskRecord} `{ order, status: 'pending' }`;
 *  - a completion flips its task to `complete`, then re-checks the barrier for
 *    that order via a `find`; when the whole level is done it *messages* the
 *    coordinators to start the next order;
 *  - starting an order dispatches its still-unfinished tasks to W; an order with
 *    no tasks means the graph is done → {@link completeGraph} (once, via meta).
 *
 * Every handler is idempotent under at-least-once delivery: completions re-drive
 * the barrier from durable state, and (re)dispatch is safe because W dedupes a
 * duplicate start. A failed task is simply retried (re-dispatched).
 */
@Injectable()
export class TopologicalBarrierProtocol extends Protocol {
  override readonly name = 'topological-barrier';

  /** Persist every task with its topological order (the one place order is computed). */
  public override async persistGraph(graph: Graph): Promise<void> {
    const gid = idOf(graph);
    const orders = topologicalOrders(graph);
    for (const [taskId, order] of orders) {
      const record: TaskRecord = { kind: TASK_KIND, graphId: gid, order, status: 'pending' };
      await this.storage.save(taskId, record);
    }
    this.log.info('graph persisted', { graphId: gid, tasks: graph.order });
  }

  /** Kick off the barrier at the first level. */
  public override async startGraph(graphId: GraphId): Promise<void> {
    this.log.info('graph started', { graphId });
    await this.startOrder(graphId, 0);
  }

  /** The only coordinator message: advance the barrier to the next order. */
  public override async onMessage(event: Message): Promise<void> {
    if (event.topic !== ADVANCE_TOPIC) {
      this.log.warn('unexpected coordinator message', { topic: event.topic });
      return;
    }
    const { graphId, order } = event.body as AdvanceBody;
    await this.startOrder(graphId, order);
  }

  /** A task finished: mark it complete, then re-check its order's barrier. */
  public override async onWorkerSuccess(event: WorkerSuccessEvent): Promise<void> {
    const ex = await this.storage.readExclusive(event.taskId);
    const record = ex.data as TaskRecord | null;
    if (!record) {
      await ex.release(); // unknown task → ack, no-op
      return;
    }
    if (record.status === 'complete')
      await ex.release(); // duplicate → still re-drive the barrier below
    else await ex.save({ ...record, status: 'complete' });
    await this.checkBarrier(record.graphId, record.order);
  }

  /** A failed task is retried by re-dispatching it (idempotent under redelivery). */
  public override async onWorkerFail(event: WorkerFailEvent): Promise<void> {
    const record = (await this.storage.read(event.taskId)) as TaskRecord | null;
    if (!record || record.status === 'complete') return; // unknown / already done → no-op
    this.log.warn('task failed, retrying', { taskId: event.taskId });
    await this.transport.sendToWorkerPool(event.taskId);
  }

  /**
   * Dispatch every not-yet-complete task at `order`. An order with no tasks is
   * the terminator: the previous level was the last one, so the graph is done.
   * Idempotent: only pending→running transitions write, and W dedupes a
   * duplicate start, so a redelivered advance never double-runs or clobbers a
   * completion.
   */
  private async startOrder(graphId: GraphId, order: number): Promise<void> {
    const tasks = await this.storage.find({ kind: TASK_KIND, graphId, order });
    if (tasks.length === 0) {
      await this.finishGraph(graphId);
      return;
    }
    for (const { taskId } of tasks) await this.dispatch(taskId);
  }

  /** Move a task pending→running (under lock, never clobbering complete) and send it to W. */
  private async dispatch(taskId: TaskId): Promise<void> {
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

  /** If the whole order has completed, message the coordinators to start the next one. */
  private async checkBarrier(graphId: GraphId, order: number): Promise<void> {
    const tasks = await this.storage.find({ kind: TASK_KIND, graphId, order });
    const allDone = tasks.every((t) => (t.data as TaskRecord).status === 'complete');
    if (!allDone) return;
    this.log.info('order complete, advancing', { graphId, order });
    await this.transport.sendToCoordinators(ADVANCE_TOPIC, { graphId, order: order + 1 } satisfies AdvanceBody);
  }

  /** Signal end-of-graph to the harness exactly once, guarded by the meta record. */
  private async finishGraph(graphId: GraphId): Promise<void> {
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
    return `barrier-meta:${graphId}`;
  }
}

/**
 * Longest-path level per node (Kahn's algorithm): roots are 0, and every task
 * sits one past its deepest dependency. This is the barrier order — a task at
 * level k has all its dependencies at levels < k, so a per-level barrier never
 * starts a task before its dependencies.
 */
function topologicalOrders(graph: Graph): Map<TaskId, number> {
  const order = new Map<TaskId, number>();
  const indegree = new Map<TaskId, number>();
  const ready: TaskId[] = [];
  graph.forEachNode((node) => {
    const deg = graph.inDegree(node);
    indegree.set(node, deg);
    if (deg === 0) {
      order.set(node, 0);
      ready.push(node);
    }
  });
  while (ready.length > 0) {
    const node = ready.shift() as TaskId;
    const level = order.get(node) ?? 0;
    for (const next of graph.outNeighbors(node)) {
      order.set(next, Math.max(order.get(next) ?? 0, level + 1));
      const deg = (indegree.get(next) ?? 1) - 1;
      indegree.set(next, deg);
      if (deg === 0) ready.push(next);
    }
  }
  return order;
}
