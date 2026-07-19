import { z } from 'zod';
import { buildGraph, type Graph, type GraphId, type GraphJson, serializeGraph } from './graph';

/** Action types the latency model knows about out of the box. */
export const KNOWN_LATENCY_ACTIONS = [
  'transport.deliver',
  'storage.read',
  'storage.readExclusive',
  'storage.save',
  'storage.delete',
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

/** 1 → "A", 26 → "Z", 27 → "AA" (spreadsheet-style column names). */
function toAlpha(n: number): string {
  let s = '';
  for (let x = n; x > 0; x = Math.floor((x - 1) / 26)) {
    s = String.fromCharCode(65 + ((x - 1) % 26)) + s;
  }
  return s;
}

const nodeObjSchema = z.object({ id: nodeIdSchema, name: z.string().min(1).optional() });

/**
 * Coordinators: either an explicit list, or just a count — in which case ids are
 * `n1..nN` and names `nodeA..nodeZ..nodeAA` (so `nodes: 3` ≡
 * `[{id: n1, name: nodeA}, {id: n2, name: nodeB}, {id: n3, name: nodeC}]`).
 */
const nodesSchema = z.union([
  z.array(nodeObjSchema).min(1),
  z
    .number()
    .int()
    .positive()
    .transform((count) =>
      Array.from({ length: count }, (_, i) => ({ id: `n${i + 1}`, name: `node${toAlpha(i + 1)}` })),
    ),
]);
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

const timedFaultSchema = z.discriminatedUnion('action', [
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

/**
 * Master choke points a conditional fault can attach to. `*Message` hooks fire
 * on delivery to a coordinator; the rest fire on the corresponding RPC from a
 * slave. `WorkerSuccessMessage`/`WorkerFailureMessage` are sugar over
 * `ReceiveMessage` filtered to the worker-pool's own topics.
 */
export const FAULT_HOOKS = [
  'SendMessage',
  'SendToWorker',
  'StorageRead',
  'StorageReadExclusive',
  'StorageSave',
  'StorageFind',
  'StorageDelete',
  'ReceiveMessage',
  'WorkerSuccessMessage',
  'WorkerFailureMessage',
] as const;

export type FaultHook = (typeof FAULT_HOOKS)[number];

/** Every valid dynamic key, e.g. `failAfterWorkerSuccessMessage`. */
const CONDITIONAL_KILL_KEYS = FAULT_HOOKS.flatMap((hook) => [`failBefore${hook}`, `failAfter${hook}`] as const);

/**
 * Throws (SyntaxError) if `expr` isn't a valid JS expression body. `expr` is a
 * trusted research-harness scenario input, evaluated master-side only —
 * compiling it via `new Function` is deliberate.
 */
function compileFilterCheck(expr: string): void {
  new Function('message', 'topic', 'node', 'attempt', `return (${expr});`);
}

/**
 * A conditional fault: kills the node involved in a specific action, gated by
 * a JS expression evaluated against that action's message. Declared as a
 * single dynamic key `fail(Before|After)<Hook>: "<expression>"`, e.g.
 * `failAfterWorkerSuccessMessage: "message.taskId === 'g0-b'"`.
 *
 * `before` = the node dies before the effect happens (the save never writes,
 * the message is never sent/delivered). `after` = the effect happens, then
 * the node dies before seeing the result/ack — the classic non-atomicity
 * window at-least-once delivery is meant to paper over.
 */
const conditionalKillSchema = z
  .object({
    restartAfterMs: z.number().positive().default(500),
    /** How many times this trigger may fire before it's spent. */
    times: z.number().int().positive().default(1),
  })
  .catchall(z.string())
  .superRefine((val, ctx) => {
    const matches = CONDITIONAL_KILL_KEYS.filter((k) => k in val);
    const [matchedKey] = matches;
    if (matches.length !== 1 || matchedKey === undefined) {
      ctx.addIssue(
        `expected exactly one fail(Before|After)<Hook> key (e.g. failAfterWorkerSuccessMessage), found ${matches.length}`,
      );
      return;
    }
    const filter = val[matchedKey];
    if (typeof filter !== 'string' || filter.trim() === '') {
      ctx.addIssue(`${matchedKey} must be a non-empty filter expression string`);
      return;
    }
    try {
      compileFilterCheck(filter);
    } catch (err) {
      ctx.addIssue(`invalid filter expression in ${matchedKey}: ${String(err)}`);
    }
  })
  .transform((val) => {
    const key = CONDITIONAL_KILL_KEYS.find((k) => k in val) as string;
    const [, rawPhase, rawHook] = /^fail(Before|After)(.+)$/.exec(key) as RegExpExecArray;
    return {
      action: 'conditionalKill' as const,
      phase: (rawPhase === 'Before' ? 'before' : 'after') as 'before' | 'after',
      hook: rawHook as FaultHook,
      filter: val[key] as string,
      restartAfterMs: val.restartAfterMs,
      times: val.times,
    };
  });

export const faultSchema = z.union([timedFaultSchema, conditionalKillSchema]);

export type FaultSpec = z.infer<typeof faultSchema>;
export type ConditionalKillFault = Extract<FaultSpec, { action: 'conditionalKill' }>;

export const scenarioSchema = z.object({
  name: z.string().min(1),
  seed: z.union([z.string(), z.number()]).transform(String),
  protocol: z.string().min(1),
  /** Each coordinator's `name` drives its OTel service (defaults to `id`). */
  nodes: nodesSchema,
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

/**
 * Protocols whose leader holds the frontier in memory, so the run must use a
 * single coordinator regardless of the declared node count (see
 * {@link Scenario.coordinatorIds}).
 */
const CENTRALIZED_PROTOCOLS = new Set(['baseline', 'baseline-recoverable']);

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

  /**
   * Coordinator node ids (the scenario's `nodes`, excluding the implicit W).
   *
   * The centralized protocols keep their frontier in one leader's memory, so
   * more than one coordinator would fork the state and break it. For those we
   * ignore the declared node count and collapse to a single coordinator — every
   * consumer (spawn, ready-check, the peer list handed to slaves) routes through
   * here, so the whole run sees just one.
   */
  coordinatorIds(): string[] {
    const ids = this.data.nodes.map((n) => n.id);
    return CENTRALIZED_PROTOCOLS.has(this.data.protocol) ? ids.slice(0, 1) : ids;
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
