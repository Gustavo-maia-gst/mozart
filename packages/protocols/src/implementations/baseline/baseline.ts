import {
  type Graph,
  type GraphId,
  type JsonObject,
  type Message,
  type TaskId,
  type WorkerFailEvent,
  type WorkerSuccessEvent,
} from '@mozart/contracts';
import { Injectable } from '@nestjs/common';
import { Protocol } from '../../protocol';

/** In-memory bookkeeping for one graph's execution. */
interface GraphRuntime {
  remaining: number;
  done: Set<TaskId>;
  depsLeft: Map<TaskId, number>;
  dependents: Map<TaskId, TaskId[]>;
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

  /** Baseline's persistence layout: the entire graph under one key. */
  public async persistGraph(graph: Graph): Promise<void> {
    await this.storage.save(this.key(graph.id), { graph } as unknown as JsonObject);
    this.log.info('graph persisted', { graphId: graph.id, tasks: graph.tasks.length });
  }

  public async startGraph(graphId: GraphId): Promise<void> {
    const stored = (await this.storage.read(this.key(graphId))) as { graph?: Graph } | null;
    const graph = stored?.graph;
    if (!graph) throw new Error(`graph ${graphId} is not persisted`);

    this.runtimes.set(graphId, this.buildRuntime(graph, graphId));
    this.log.info('graph started', { graphId });
    for (const task of graph.tasks) {
      if (task.dependsOn.length === 0) await this.startTask(task.id);
    }
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
    this.log.warn('unexpected coordinator message', { topic: event.topic, from: event.from });
  }

  private async completeTask(runtime: GraphRuntime, taskId: TaskId): Promise<void> {
    runtime.done.add(taskId);
    runtime.remaining -= 1;
    for (const dependent of runtime.dependents.get(taskId) ?? []) {
      const left = (runtime.depsLeft.get(dependent) ?? 1) - 1;
      runtime.depsLeft.set(dependent, left);
      if (left === 0) await this.startTask(dependent);
    }
    if (runtime.remaining === 0) {
      this.log.info('graph complete', { graphId: this.taskGraph.get(taskId) ?? null });
    }
  }

  private buildRuntime(graph: Graph, graphId: GraphId): GraphRuntime {
    const depsLeft = new Map<TaskId, number>();
    const dependents = new Map<TaskId, TaskId[]>();
    for (const task of graph.tasks) {
      depsLeft.set(task.id, task.dependsOn.length);
      this.taskGraph.set(task.id, graphId);
      for (const dep of task.dependsOn) {
        const list = dependents.get(dep) ?? [];
        list.push(task.id);
        dependents.set(dep, list);
      }
    }
    return { remaining: graph.tasks.length, done: new Set(), depsLeft, dependents };
  }

  private async startTask(taskId: TaskId): Promise<void> {
    this.log.info('start task', { taskId });
    await this.workerPool.start(taskId);
  }

  private runtimeOf(taskId: TaskId): GraphRuntime | undefined {
    const graphId = this.taskGraph.get(taskId);
    return graphId ? this.runtimes.get(graphId) : undefined;
  }

  private key(graphId: GraphId): string {
    return `graph:${graphId}`;
  }
}
