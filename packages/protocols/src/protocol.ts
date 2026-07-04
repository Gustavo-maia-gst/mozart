import {
  type Delivery,
  type Graph,
  type GraphId,
  PROTOCOL_LOGGER,
  type ProtocolLogger,
  STORAGE_PORT,
  type StoragePort,
  TRANSPORT_PORT,
  type TransportPort,
  WORKER_POOL_PORT,
  type WorkerPoolPort,
} from '@mozart/contracts';
import { Inject } from '@nestjs/common';

/**
 * Base class every coordination protocol extends. Nest instantiates the
 * concrete subclass and injects the world it may touch (storage, transport,
 * worker pool, logger) as properties — there is no hand-rolled context blob.
 *
 * A protocol defines three operations:
 *  - `persistGraph` — how a graph is stored in S before it runs (setup only);
 *  - `startGraph`   — begin executing an already-persisted graph;
 *  - `onMessage`    — handle every inbound message (worker events + coordination).
 *
 * The harness drives them; the ack contract still holds (a resolved `onMessage`
 * acks, a rejection triggers redelivery), so handlers must be idempotent.
 */
export abstract class Protocol {
  @Inject(TRANSPORT_PORT) protected readonly transport!: TransportPort;
  @Inject(STORAGE_PORT) protected readonly storage!: StoragePort;
  @Inject(WORKER_POOL_PORT) protected readonly workers!: WorkerPoolPort;
  @Inject(PROTOCOL_LOGGER) protected readonly log!: ProtocolLogger;

  abstract readonly name: string;

  abstract persistGraph(graph: Graph): Promise<void>;
  abstract startGraph(graphId: GraphId): Promise<void>;
  abstract onMessage(message: Delivery): Promise<void>;
}
