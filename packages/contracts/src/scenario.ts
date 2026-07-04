import { z } from 'zod';
import { buildGraph, type Graph, type GraphId, type GraphJson, serializeGraph } from './graph';

/** Action types the latency model knows about out of the box. */
export const KNOWN_LATENCY_ACTIONS = [
  'transport.deliver',
  'storage.read',
  'storage.readExclusive',
  'storage.save',
  'storage.find',
  'worker.taskDuration',
] as const;

export type LatencyActionType = string;

export const distributionSchema = z.discriminatedUnion('distribution', [
  z.object({ distribution: z.literal('constant'), value: z.number().min(0) }),
  z.object({ distribution: z.literal('normal'), mean: z.number(), stddev: z.number().min(0) }),
  z.object({
    distribution: z.literal('lognormal'),
    /** Mean of the underlying normal (log-space), as in d3.randomLogNormal. */
    mu: z.number(),
    sigma: z.number().min(0),
  }),
  z.object({ distribution: z.literal('uniform'), min: z.number().min(0), max: z.number().min(0) }),
]);

export type DistributionConfig = z.infer<typeof distributionSchema>;

const nodeIdSchema = z.string().min(1);
const taskIdSchema = z.string().min(1);
const graphIdSchema = z.string().min(1);

export const graphTaskSchema = z.object({
  id: taskIdSchema,
  dependsOn: z.array(taskIdSchema).default([]),
  /** Nominal cost (ms) used as the mean of the sampled task duration when set. */
  costMs: z.number().positive().optional(),
});

/**
 * One DAG. Task ids (and their `dependsOn`) are graph-local here; they get
 * namespaced to `<graphId>-<taskId>` when the runtime graphs are built, so ids
 * stay globally unique across the several graphs a run may execute at once
 * (see {@link graphsFromScenario}).
 */
export const graphSchema = z.object({
  id: graphIdSchema,
  tasks: z.array(graphTaskSchema).min(1),
  /** Delay (ms) from run activation before this graph is started. 0 = at once. */
  startAfterMs: z.number().min(0).default(0),
});

export const faultSchema = z.discriminatedUnion('action', [
  z.object({
    action: z.literal('killNode'),
    at: z.number().min(0),
    node: nodeIdSchema,
    restartAfterMs: z.number().positive().optional(),
  }),
  z.object({
    action: z.literal('storageOutage'),
    at: z.number().min(0),
    /** 'all' = outage for every node; a node id = outage seen only by that node. */
    scope: z.union([z.literal('all'), nodeIdSchema]),
    durationMs: z.number().positive(),
  }),
  z.object({
    action: z.literal('partitionNode'),
    at: z.number().min(0),
    node: nodeIdSchema,
    durationMs: z.number().positive(),
    direction: z.enum(['in', 'out', 'both']).default('both'),
  }),
  z.object({
    action: z.literal('duplicateDeliveries'),
    at: z.number().min(0),
    /** Extra copies emitted on the next delivery on the coordinators queue. */
    extraCopies: z.number().int().positive().default(1),
  }),
  z.object({
    action: z.literal('failTask'),
    /** The next `worker.start` for this task fails (emits task.failed). */
    taskId: taskIdSchema,
  }),
]);

export type FaultSpec = z.infer<typeof faultSchema>;

export const scenarioSchema = z.object({
  name: z.string().min(1),
  seed: z.union([z.string(), z.number()]).transform(String),
  protocol: z.string().min(1),
  /** Each coordinator's `name` drives its OTel service (defaults to `id`). */
  nodes: z.array(z.object({ id: nodeIdSchema, name: z.string().min(1).optional() })).min(1),
  /** One or more concurrent DAGs to coordinate in this run. */
  graphs: z.array(graphSchema).min(1),
  storage: z.discriminatedUnion('adapter', [
    z.object({ adapter: z.literal('in-memory') }),
    z.object({
      adapter: z.literal('postgres'),
      /** Falls back to env MOZART_PG_URL when omitted. */
      url: z.string().optional(),
    }),
  ]),
  transport: z.object({ ackTimeoutMs: z.number().positive().default(2000) }).default({ ackTimeoutMs: 2000 }),
  /** Latency per action type; unlisted actions default to constant 0. */
  latency: z.record(z.string(), distributionSchema).default({}),
  faults: z.array(faultSchema).default([]),
  endCondition: z.discriminatedUnion('type', [z.object({ type: z.literal('timeout'), ms: z.number().positive() })]),
});

/** The validated, plain scenario document (what the YAML parses into). */
export type ScenarioData = z.infer<typeof scenarioSchema>;
export type GraphSpec = z.infer<typeof graphSchema>;

/** Slice of the scenario a slave receives at handshake. */
export interface ScenarioInfo {
  runId: string;
  /** The receiving node's own id. */
  nodeId: string;
  protocol: string;
  /** All coordinator node ids (excluding `W`). */
  nodes: string[];
  /**
   * The concurrent DAGs to coordinate, with runtime (namespaced) task ids, in
   * their JSON form — a live {@link Graph} does not survive the IPC channel, so
   * slaves rehydrate each with `parseGraph`.
   */
  graphs: GraphJson[];
}

/**
 * A parsed scenario with behaviour. Wraps the plain {@link ScenarioData} and is
 * the one place scenario-derived values are computed — notably {@link graphs},
 * whose task ids are namespaced to `<graphId>-<taskId>` so they stay globally
 * unique across the concurrent DAGs a run executes. Consumers depend on this,
 * never on the raw document.
 */
export class Scenario {
  private cachedGraphs?: Graph[];

  constructor(private readonly data: ScenarioData) {}

  /**
   * Runtime graphs, as directed graphs (edge `dep -> task`), with task ids
   * namespaced `<graphId>-<taskId>` so they stay globally unique across the
   * concurrent DAGs a run executes.
   */
  get graphs(): Graph[] {
    if (!this.cachedGraphs) {
      this.cachedGraphs = this.data.graphs.map((graph) =>
        buildGraph(
          graph.id,
          graph.tasks.map((task) => ({
            id: `${graph.id}-${task.id}`,
            dependsOn: task.dependsOn.map((dep) => `${graph.id}-${dep}`),
            ...(task.costMs !== undefined ? { costMs: task.costMs } : {}),
          })),
        ),
      );
    }
    return this.cachedGraphs;
  }

  get name(): string {
    return this.data.name;
  }
  get seed(): string {
    return this.data.seed;
  }
  get protocol(): string {
    return this.data.protocol;
  }
  get latency(): ScenarioData['latency'] {
    return this.data.latency;
  }
  get faults(): FaultSpec[] {
    return this.data.faults;
  }
  get storage(): ScenarioData['storage'] {
    return this.data.storage;
  }
  get ackTimeoutMs(): number {
    return this.data.transport.ackTimeoutMs;
  }
  get endConditionMs(): number {
    return this.data.endCondition.ms;
  }

  /**
   * When each graph should be started, as an offset (ms) from run activation.
   * The harness drives the start; a graph is never started before it (and every
   * other graph) has been persisted.
   */
  graphStartSchedule(): { graphId: GraphId; startAfterMs: number }[] {
    return this.data.graphs.map((g) => ({ graphId: g.id, startAfterMs: g.startAfterMs }));
  }

  /** Coordinator node ids (the scenario's `nodes`, excluding the implicit W). */
  coordinatorIds(): string[] {
    return this.data.nodes.map((n) => n.id);
  }

  /** A coordinator's display name (drives its OTel service); defaults to its id. */
  nodeName(nodeId: string): string {
    return this.data.nodes.find((n) => n.id === nodeId)?.name ?? nodeId;
  }

  /** The per-node slice handed to a slave at handshake. */
  infoFor(nodeId: string, runId: string): ScenarioInfo {
    return {
      runId,
      nodeId,
      protocol: this.data.protocol,
      nodes: this.coordinatorIds(),
      graphs: this.graphs.map(serializeGraph),
    };
  }
}
