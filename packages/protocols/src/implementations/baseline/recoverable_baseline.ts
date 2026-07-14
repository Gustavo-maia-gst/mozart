import {
  type Graph,
  type GraphId,
  graphId as idOf,
  type GraphJson,
  type JsonObject,
  type Message,
  parseGraph,
  serializeGraph,
  type TaskId,
  type WorkerFailEvent,
  type WorkerSuccessEvent,
} from '@mozart/contracts';
import { Injectable } from '@nestjs/common';
import { Protocol } from '../../protocol';

/** In-memory bookkeeping for one graph's execution (rebuilt from S on load). */
interface GraphRuntime {
  graph: Graph;
  remaining: number;
  done: Set<TaskId>;
  /** Remaining unmet dependencies per task; a task is ready when it hits 0. */
  depsLeft: Map<TaskId, number>;
}

const GRAPH_KIND = 'graph';
const TASKLOG_KIND = 'tasklog';

/** The persisted graph blob (one per graph). */
interface GraphRecord extends JsonObject {
  kind: 'graph';
  graphId: GraphId;
  graph: GraphJson;
}

/**
 * A redo-log entry for one task. Written *before* the action it records (a
 * write-ahead log): `running` before dispatch, `complete` before advancing
 * dependents. Recovery replays the `complete` entries to rebuild the frontier.
 */
interface TaskLog extends JsonObject {
  kind: 'tasklog';
  graphId: GraphId;
  taskId: TaskId;
  status: 'running' | 'complete';
}

/**
 * Recoverable baseline protocol — the centralized baseline made crash-tolerant.
 *
 * Still a single leader driving each graph from in-memory bookkeeping (see
 * {@link BaselineProtocol}), but every action is first written to a durable
 * redo log in S, like a relational DB's write-ahead log: `running` is logged
 * before a task is dispatched, `complete` before its dependents are advanced.
 *
 * The in-memory frontier is disposable. On any handler, the leader lazily
 * *loads* the graph it touches: if its runtime isn't in memory (fresh start, or
 * a stateless restart after SIGKILL), it rebuilds the frontier by replaying the
 * log from S and re-dispatches every ready task. Re-dispatch is safe —
 * at-least-once by design: W dedupes a still-running task, and a duplicate
 * completion is a no-op. So a crashed leader simply restarts and resumes where
 * the log left off, instead of losing the run like the plain baseline.
 */
@Injectable()
export class RecoverableBaselineProtocol extends Protocol {
  readonly name = 'baseline-recoverable';

  /** The "already loaded?" flag: presence here means the frontier is in memory. */
  private readonly runtimes = new Map<GraphId, GraphRuntime>();

  /** Persist the graph blob (setup only, on the master). */
  public async persistGraph(graph: Graph): Promise<void> {
    const id = idOf(graph);
    const record: GraphRecord = { kind: GRAPH_KIND, graphId: id, graph: serializeGraph(graph) };
    await this.storage.save(this.graphKey(id), record);
    this.log.info('graph persisted', { graphId: id, tasks: graph.order });
  }

  /** Begin (or, after a restart, resume) a graph — both go through the same load path. */
  public async startGraph(graphId: GraphId): Promise<void> {
    this.log.info('graph started', { graphId });
    await this.ensureLoaded(graphId);
  }

  /** Advance the DAG on a completion, loading (recovering) the graph first if needed. */
  public async onWorkerSuccess(event: WorkerSuccessEvent): Promise<void> {
    const log = (await this.storage.read(event.taskId)) as TaskLog | null;
    if (!log) return; // never dispatched (no WAL entry) → unknown, no-op
    const runtime = await this.ensureLoaded(log.graphId);
    if (runtime.done.has(event.taskId)) return; // duplicate → no-op
    await this.completeTask(runtime, log.graphId, event.taskId);
  }

  /** Fault-tolerant: a failed task is retried by re-dispatching it. */
  public async onWorkerFail(event: WorkerFailEvent): Promise<void> {
    this.log.warn('task failed, retrying', { taskId: event.taskId });
    await this.transport.sendToWorkerPool(event.taskId);
  }

  /** A single coordinator never exchanges coordination messages. */
  public async onMessage(event: Message): Promise<void> {
    this.log.warn('unexpected coordinator message', { topic: event.topic });
  }

  private async completeTask(runtime: GraphRuntime, graphId: GraphId, taskId: TaskId): Promise<void> {
    await this.logStatus(graphId, taskId, 'complete'); // write-ahead: log before acting
    runtime.done.add(taskId);
    runtime.remaining -= 1;
    for (const dependent of runtime.graph.outNeighbors(taskId)) {
      const left = (runtime.depsLeft.get(dependent) ?? 1) - 1;
      runtime.depsLeft.set(dependent, left);
      if (left === 0) await this.startTask(graphId, dependent);
    }
    if (runtime.remaining === 0) {
      this.log.info('graph complete', { graphId });
      await this.transport.completeGraph(graphId);
    }
  }

  /**
   * Return the in-memory runtime, rebuilding it from the durable log on the
   * first touch (fresh start or post-crash restart) and re-driving the frontier.
   */
  private async ensureLoaded(graphId: GraphId): Promise<GraphRuntime> {
    const existing = this.runtimes.get(graphId);
    if (existing) return existing;
    const runtime = await this.rebuild(graphId);
    this.runtimes.set(graphId, runtime);
    if (runtime.remaining === 0) {
      await this.transport.completeGraph(graphId); // already finished before the restart
    } else {
      for (const taskId of this.readyTasks(runtime)) await this.startTask(graphId, taskId);
    }
    return runtime;
  }

  /** Rebuild a graph's frontier from S: the graph blob plus its `complete` log entries. */
  private async rebuild(graphId: GraphId): Promise<GraphRuntime> {
    const record = (await this.storage.read(this.graphKey(graphId))) as GraphRecord | null;
    if (!record) throw new Error(`graph ${graphId} is not persisted`);
    const graph = parseGraph(record.graph);
    const runtime: GraphRuntime = { graph, remaining: graph.order, done: new Set(), depsLeft: new Map() };
    graph.forEachNode((node) => runtime.depsLeft.set(node, graph.inDegree(node)));
    const completed = await this.storage.find({ kind: TASKLOG_KIND, graphId, status: 'complete' });
    for (const { data } of completed) {
      const { taskId } = data as TaskLog;
      if (runtime.done.has(taskId)) continue;
      runtime.done.add(taskId);
      runtime.remaining -= 1;
      for (const dependent of graph.outNeighbors(taskId)) {
        runtime.depsLeft.set(dependent, (runtime.depsLeft.get(dependent) ?? 1) - 1);
      }
    }
    return runtime;
  }

  /** Tasks whose dependencies are all complete but which are not themselves done. */
  private readyTasks(runtime: GraphRuntime): TaskId[] {
    const ready: TaskId[] = [];
    runtime.graph.forEachNode((node) => {
      if (!runtime.done.has(node) && (runtime.depsLeft.get(node) ?? 0) === 0) ready.push(node);
    });
    return ready;
  }

  private async startTask(graphId: GraphId, taskId: TaskId): Promise<void> {
    await this.logStatus(graphId, taskId, 'running'); // write-ahead: log before dispatch
    this.log.info('start task', { taskId });
    await this.transport.sendToWorkerPool(taskId);
  }

  private async logStatus(graphId: GraphId, taskId: TaskId, status: TaskLog['status']): Promise<void> {
    const entry: TaskLog = { kind: TASKLOG_KIND, graphId, taskId, status };
    await this.storage.save(taskId, entry);
  }

  private graphKey(graphId: GraphId): string {
    return `graph:${graphId}`;
  }
}
