import type { NodeId } from './ids';
import type { JsonObject } from './json';
import type { Delivery, StoragePort, TransportPort, WorkerPoolPort } from './ports';
import type { ScenarioInfo } from './scenario';

/** Structured logger surfaced to protocol code; entries are attached to the active span. */
export interface ProtocolLogger {
  debug(message: string, attrs?: JsonObject): void;
  info(message: string, attrs?: JsonObject): void;
  warn(message: string, attrs?: JsonObject): void;
  error(message: string, attrs?: JsonObject): void;
}

/** Everything a protocol may touch. There are no other side-effect channels. */
export interface ProtocolContext {
  readonly nodeId: NodeId;
  readonly scenario: ScenarioInfo;
  readonly transport: TransportPort;
  readonly storage: StoragePort;
  readonly workers: WorkerPoolPort;
  readonly log: ProtocolLogger;
}

/**
 * Contract implemented by every coordination protocol under test.
 *
 * ## Ack contract (read this twice)
 * `onMessage` **resolving** is the harness-level ack. Rejecting — or the node
 * dying mid-handler — leaves the delivery un-acked, and the transport WILL
 * redeliver it (at-least-once). Handlers must therefore be idempotent:
 * duplicated deliveries of the same `messageId` must not corrupt state.
 *
 * ## Statelessness
 * Nodes are crash-stop and restart with zero local state. Anything that must
 * survive a crash lives in storage S. In-memory fields are at most caches.
 */
export interface ProtocolSpi {
  readonly name: string;
  /** Called once per process activation (including after a restart). */
  onActivate(ctx: ProtocolContext): Promise<void>;
  /** Resolve => ack. Reject => no ack => redelivery. */
  onMessage(delivery: Delivery, ctx: ProtocolContext): Promise<void>;
  /** Graceful shutdown only — NEVER runs on a crash (SIGKILL). */
  onDeactivate?(ctx: ProtocolContext): Promise<void>;
}
