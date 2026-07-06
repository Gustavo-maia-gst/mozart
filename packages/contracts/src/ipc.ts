import { z } from 'zod';
import type { GraphId } from './graph';
import { type Json, jsonObjectSchema, jsonSchema } from './json';
import type { Delivery, StorageQuery, TaskMatch, TaskState } from './ports';
import type { ScenarioInfo } from './scenario';

/**
 * Single frame on the master<->slave IPC channel (child_process JSON channel).
 *
 * - `req`  — RPC request (slave -> master), answered by a `res` with
 *   `correlId === frameId`.
 * - `res`  — RPC response (master -> slave).
 * - `push` — master-initiated event (deliveries, lifecycle); not acked at the
 *   IPC level — harness-level acks are explicit `transport.ack` reqs.
 */
export interface IpcFrame {
  frameId: string;
  kind: 'req' | 'res' | 'push';
  /** req: method name. push: push type. */
  method?: string;
  /** res only: frameId of the request being answered. */
  correlId?: string;
  /** res only: false means `payload` is an { error } object. */
  ok?: boolean;
  payload: unknown;
  /** W3C trace-context carrier (traceparent/tracestate). */
  traceCtx: Record<string, string>;
  /** Sender wall-clock ms — receivers record their own arrival time to measure OS jitter. */
  sentAt: number;
}

export const ipcFrameSchema = z.object({
  frameId: z.string().min(1),
  kind: z.enum(['req', 'res', 'push']),
  method: z.string().optional(),
  correlId: z.string().optional(),
  ok: z.boolean().optional(),
  payload: z.unknown(),
  traceCtx: z.record(z.string(), z.string()),
  sentAt: z.number(),
});

// ---------------------------------------------------------------------------
// RPC methods (slave -> master)
// ---------------------------------------------------------------------------

export const rpcPayloadSchemas = {
  'node.ready': z.object({}),
  'transport.toCoordinators': z.object({ topic: z.string().min(1), body: jsonSchema }),
  'transport.toWorkerPool': z.object({ taskId: z.string().min(1) }),
  'transport.ack': z.object({ deliveryId: z.string().min(1) }),
  'transport.completeGraph': z.object({ graphId: z.string().min(1) }),
  'storage.read': z.object({ taskId: z.string().min(1) }),
  'storage.find': z.object({ query: jsonObjectSchema }),
  'storage.readExclusive': z.object({ taskId: z.string().min(1) }),
  'storage.save': z.object({ taskId: z.string().min(1), data: jsonObjectSchema }),
  'storage.delete': z.object({ query: jsonObjectSchema }),
  'storage.lease.save': z.object({ leaseId: z.string().min(1), data: jsonObjectSchema }),
  'storage.lease.release': z.object({ leaseId: z.string().min(1) }),
} as const;

export type RpcMethod = keyof typeof rpcPayloadSchemas;

/** Request/response value types per RPC method. */
export interface RpcContracts {
  'node.ready': { req: Record<string, never>; res: { scenario: ScenarioInfo } };
  'transport.toCoordinators': { req: { topic: string; body: Json }; res: Record<string, never> };
  'transport.toWorkerPool': { req: { taskId: string }; res: Record<string, never> };
  'transport.ack': { req: { deliveryId: string }; res: Record<string, never> };
  'transport.completeGraph': { req: { graphId: GraphId }; res: Record<string, never> };
  'storage.read': { req: { taskId: string }; res: { data: TaskState | null } };
  'storage.find': { req: { query: StorageQuery }; res: { matches: TaskMatch[] } };
  'storage.readExclusive': {
    req: { taskId: string };
    res: { leaseId: string; data: TaskState | null };
  };
  'storage.save': { req: { taskId: string; data: TaskState }; res: Record<string, never> };
  'storage.delete': { req: { query: StorageQuery }; res: { deleted: number } };
  'storage.lease.save': { req: { leaseId: string; data: TaskState }; res: Record<string, never> };
  'storage.lease.release': { req: { leaseId: string }; res: Record<string, never> };
}

// Compile-time check: every schema key has a contract and vice versa.
type _AssertContracts = RpcMethod extends keyof RpcContracts
  ? keyof RpcContracts extends RpcMethod
    ? true
    : never
  : never;
const _assertContracts: _AssertContracts = true;
void _assertContracts;

// ---------------------------------------------------------------------------
// Pushes (master -> slave)
// ---------------------------------------------------------------------------

export interface PushContracts {
  /** Transport delivery (protocol messages and W events alike). */
  delivery: Delivery;
  /** Graceful shutdown request: run onDeactivate, then exit(0). */
  'protocol.deactivate': Record<string, never>;
}

export type PushType = keyof PushContracts;

export interface RpcErrorPayload {
  error: { code: string; message: string };
}
