import {
  type Delivery,
  type Graph,
  type GraphId,
  ProtocolLogger,
  StoragePort,
  TransportPort,
  WorkerPoolPort,
} from '@mozart/contracts';
import { Injectable } from '@nestjs/common';

/**
 * Base class every coordination protocol extends.
 *
 * It injects the world a protocol may touch (transport / storage / workers /
 * logger) via the constructor and exposes them as `protected` — so a concrete
 * protocol just extends this class and uses `this.storage` etc., with no
 * constructor and no `@Inject` (the ports are abstract classes, used as DI
 * tokens by type).
 *
 * A protocol defines three operations:
 *  - `persistGraph` — how a graph is stored in S before it runs (setup only);
 *  - `startGraph`   — begin executing an already-persisted graph;
 *  - `onMessage`    — handle every inbound message (worker events + coordination).
 *
 * Ack contract: a resolved `onMessage` acks; a rejection triggers redelivery, so
 * handlers must be idempotent.
 */
@Injectable()
export abstract class Protocol {
  constructor(
    protected readonly transport: TransportPort,
    protected readonly storage: StoragePort,
    protected readonly workers: WorkerPoolPort,
    protected readonly log: ProtocolLogger,
  ) {}

  abstract readonly name: string;
  abstract persistGraph(graph: Graph): Promise<void>;
  abstract startGraph(graphId: GraphId): Promise<void>;
  abstract onMessage(message: Delivery): Promise<void>;
}
