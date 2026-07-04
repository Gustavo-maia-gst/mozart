import { DirectedGraph } from 'graphology';
import type { Attributes, SerializedGraph } from 'graphology-types';
import type { TaskId } from './ids';

/** Identity of a DAG. A run may execute several concurrent graphs. */
export type GraphId = string;

/** Node (task) attributes carried on the graph. */
export interface TaskAttributes extends Attributes {
  /** Nominal cost (ms); used by W as the task duration when set. */
  costMs?: number;
}

/** Graph-level attributes — carries the DAG's own identity. */
export interface GraphAttributes extends Attributes {
  id: GraphId;
}

/**
 * A DAG of tasks as a directed graph. An edge `A -> B` means **"B depends on
 * A"**: A must complete before B may start. So a task's dependencies are its
 * in-neighbours, its dependents are its out-neighbours, and the roots (ready
 * immediately) are the in-degree-0 nodes. Node keys are task ids; the graph's
 * own id lives in its {@link GraphAttributes}.
 */
export type Graph = DirectedGraph<TaskAttributes, Attributes, GraphAttributes>;

/** JSON form of a {@link Graph} (graphology's export shape) — safe over IPC/S. */
export type GraphJson = SerializedGraph<TaskAttributes, Attributes, GraphAttributes>;

/** Plain task description used to build a {@link Graph} (YAML/tests). */
export interface GraphTask {
  id: TaskId;
  /** Ids of tasks this one depends on (its in-neighbours). */
  dependsOn?: TaskId[];
  costMs?: number;
}

/**
 * Builds a {@link Graph} from a flat task list, wiring an edge from each
 * dependency to the task that depends on it (`dep -> task`).
 */
export function buildGraph(id: GraphId, tasks: GraphTask[]): Graph {
  const graph: Graph = new DirectedGraph<TaskAttributes, Attributes, GraphAttributes>();
  graph.setAttribute('id', id);
  for (const task of tasks) {
    graph.addNode(task.id, task.costMs !== undefined ? { costMs: task.costMs } : {});
  }
  for (const task of tasks) {
    for (const dep of task.dependsOn ?? []) graph.addDirectedEdge(dep, task.id);
  }
  return graph;
}

/** The graph's id (its {@link GraphAttributes}). */
export function graphId(graph: Graph): GraphId {
  return graph.getAttribute('id');
}

/**
 * Mean fan-in / fan-out across one or more DAGs.
 *
 * Averaging in-degree over *all* nodes equals averaging out-degree (both are
 * edges/nodes), so that wouldn't tell the two apart. Instead we restrict each
 * denominator to the nodes that actually have that kind of edge:
 * - `meanFanIn`  = edges / (# nodes with ≥1 dependency)   → mean join width
 * - `meanFanOut` = edges / (# nodes with ≥1 dependent)    → mean branch width
 *
 * Both are 0 for an edgeless graph. Aggregated across all graphs passed in.
 */
export function fanStats(graphs: Graph[]): { meanFanIn: number; meanFanOut: number } {
  let edges = 0;
  let withIn = 0;
  let withOut = 0;
  for (const graph of graphs) {
    edges += graph.size;
    graph.forEachNode((node) => {
      if (graph.inDegree(node) > 0) withIn += 1;
      if (graph.outDegree(node) > 0) withOut += 1;
    });
  }
  return { meanFanIn: withIn ? edges / withIn : 0, meanFanOut: withOut ? edges / withOut : 0 };
}

/**
 * Critical-path cost of a DAG (ms): the maximum, over every path, of the sum of
 * each task's {@link TaskAttributes.costMs} (absent cost counts as 0). It is the
 * theoretical floor on the graph's makespan — the time to finish it with
 * infinite workers and zero coordination overhead. Computed by memoized longest
 * weighted path from the roots (in-degree-0 nodes); the graph is a DAG so the
 * recursion terminates.
 */
export function criticalPathCost(graph: Graph): number {
  const memo = new Map<TaskId, number>();
  const longestTo = (node: TaskId): number => {
    const cached = memo.get(node);
    if (cached !== undefined) return cached;
    const own = graph.getNodeAttribute(node, 'costMs') ?? 0;
    let best = 0;
    graph.forEachInNeighbor(node, (dep) => {
      best = Math.max(best, longestTo(dep));
    });
    const total = own + best;
    memo.set(node, total);
    return total;
  };
  let max = 0;
  graph.forEachNode((node) => {
    max = Math.max(max, longestTo(node));
  });
  return max;
}

/** Serialize a graph to its JSON form for IPC/storage. */
export function serializeGraph(graph: Graph): GraphJson {
  return graph.export();
}

/** Rehydrate a graph from its JSON form. */
export function parseGraph(data: GraphJson): Graph {
  return DirectedGraph.from(data) as Graph;
}
