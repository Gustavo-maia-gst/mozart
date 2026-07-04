import { z } from 'zod';

/** Action types the latency model knows about out of the box. */
export const KNOWN_LATENCY_ACTIONS = [
  'transport.deliver',
  'storage.read',
  'storage.readExclusive',
  'storage.save',
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

export const dagSchema = z.object({
  tasks: z
    .array(
      z.object({
        id: taskIdSchema,
        dependsOn: z.array(taskIdSchema).default([]),
        /** Nominal cost (ms) used as the mean of the sampled task duration when set. */
        costMs: z.number().positive().optional(),
      }),
    )
    .min(1),
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
    from: nodeIdSchema,
    to: nodeIdSchema,
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
  nodes: z.array(z.object({ id: nodeIdSchema })).min(1),
  dag: dagSchema,
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

export type Scenario = z.infer<typeof scenarioSchema>;
export type DagSpec = z.infer<typeof dagSchema>;

/** Slice of the scenario a slave receives at handshake. */
export interface ScenarioInfo {
  runId: string;
  /** The receiving node's own id. */
  nodeId: string;
  protocol: string;
  /** All coordinator node ids (excluding `W`). */
  nodes: string[];
  dag: DagSpec;
}
