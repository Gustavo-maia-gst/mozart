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

/** In-memory bookkeeping for one graph's execution. */
interface GraphRuntime {
  /** The DAG itself: dependents are out-neighbours, dependencies in-neighbours. */
  graph: Graph;
  remaining: number;
  done: Set<TaskId>;
  /** Remaining unmet dependencies per task; a task starts when it hits 0. */
  depsLeft: Map<TaskId, number>;
}

/**
 * Centralized baseline protocol.
 *
 * The leader persists each graph as one blob, loads it fully into memory, and
 * drives it to completion purely from in-memory bookkeeping — starting a task
 * as soon as all its dependencies have reported completion. It is the simplest
 * possible reference: a single coordinator, no distribution, no fault tolerance
 * (the in-memory frontier is lost if the leader crashes). That naivety is the
 * point — it's the yardstick the distributed protocols are measured against.
 */
@Injectable()
export class BaselineProtocol extends Protocol {
  readonly name = 'baseline';

  private readonly runtimes = new Map<GraphId, GraphRuntime>();
  private readonly taskGraph = new Map<TaskId, GraphId>();

  /** Baseline's persistence layout: the entire graph (JSON) under one key. */
  public async persistGraph(graph: Graph): Promise<void> {
    const id = idOf(graph);
    await this.storage.save(this.key(id), { graph: serializeGraph(graph) } as unknown as JsonObject);
    this.log.info('graph persisted', { graphId: id, tasks: graph.order });
  }

  public async startGraph(graphId: GraphId): Promise<void> {
    const stored = (await this.storage.read(this.key(graphId))) as { graph?: GraphJson } | null;
    if (!stored?.graph) throw new Error(`graph ${graphId} is not persisted`);
    const graph = parseGraph(stored.graph);

    this.runtimes.set(graphId, this.buildRuntime(graph, graphId));
    this.log.info('graph started', { graphId });

    const roots: TaskId[] = [];
    graph.forEachNode((node) => {
      if (graph.inDegree(node) === 0) roots.push(node);
    });
    for (const root of roots) await this.startTask(root);
  }

  /**
   * Advance the DAG on a task completion. Idempotent under at-least-once: a
   * duplicated completion (or one for an unknown task) is a no-op.
   */
  public async onWorkerSuccess(event: WorkerSuccessEvent): Promise<void> {
    const runtime = this.runtimeOf(event.taskId);
    if (!runtime || runtime.done.has(event.taskId)) return; // unknown / duplicate → ack, no-op
    await this.completeTask(runtime, event.taskId);
  }

  /** No fault tolerance by design: log the failure and let the graph stall. */
  public async onWorkerFail(event: WorkerFailEvent): Promise<void> {
    this.log.warn('task failed', { taskId: event.taskId });
  }

  /** A single coordinator never exchanges coordination messages. */
  public async onMessage(event: Message): Promise<void> {
    this.log.warn('unexpected coordinator message', { topic: event.topic });
  }

  private async completeTask(runtime: GraphRuntime, taskId: TaskId): Promise<void> {
    runtime.done.add(taskId);
    runtime.remaining -= 1;
    for (const dependent of runtime.graph.outNeighbors(taskId)) {
      const left = (runtime.depsLeft.get(dependent) ?? 1) - 1;
      runtime.depsLeft.set(dependent, left);
      if (left === 0) await this.startTask(dependent);
    }
    if (runtime.remaining === 0) {
      const gid = this.taskGraph.get(taskId);
      this.log.info('graph complete', { graphId: gid ?? null });
      if (gid) await this.transport.completeGraph(gid);
    }
  }

  private buildRuntime(graph: Graph, graphId: GraphId): GraphRuntime {
    const depsLeft = new Map<TaskId, number>();
    graph.forEachNode((node) => {
      depsLeft.set(node, graph.inDegree(node));
      this.taskGraph.set(node, graphId);
    });
    return { graph, remaining: graph.order, done: new Set(), depsLeft };
  }

  private async startTask(taskId: TaskId): Promise<void> {
    this.log.info('start task', { taskId });
    await this.transport.sendToWorkerPool(taskId);
  }

  private runtimeOf(taskId: TaskId): GraphRuntime | undefined {
    const graphId = this.taskGraph.get(taskId);
    return graphId ? this.runtimes.get(graphId) : undefined;
  }

  private key(graphId: GraphId): string {
    return `graph:${graphId}`;
  }
}
