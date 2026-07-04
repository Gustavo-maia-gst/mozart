import {
  type Graph,
  type GraphId,
  type Message,
  ProtocolLogger,
  StoragePort,
  TransportPort,
  type WorkerFailEvent,
  WorkerPoolPort,
  type WorkerSuccessEvent,
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
 * Setup:
 *  - `persistGraph` — how a graph is stored in S before it runs (setup only);
 *  - `startGraph`   — begin executing an already-persisted graph.
 *
 * Inbound events — the host routes each delivery by topic to exactly one of:
 *  - `onWorkerSuccess` — a task dispatched to the Worker Pool completed;
 *  - `onWorkerFail`    — a task dispatched to the Worker Pool failed;
 *  - `onMessage`       — a coordinator<->coordinator message (any other topic).
 *
 * Ack contract: a resolved handler acks; a rejection triggers redelivery, so
 * handlers must be idempotent.
 */
@Injectable()
export abstract class Protocol {
  constructor(
    protected readonly transport: TransportPort,
    protected readonly storage: StoragePort,
    protected readonly workerPool: WorkerPoolPort,
    protected readonly log: ProtocolLogger,
  ) {}

  abstract readonly name: string;

  public abstract persistGraph(graph: Graph): Promise<void>;

  public abstract startGraph(graphId: GraphId): Promise<void>;

  /** A task previously dispatched to the Worker Pool completed successfully. */
  public abstract onWorkerSuccess(event: WorkerSuccessEvent): Promise<void>;

  /** A task dispatched to the Worker Pool failed. */
  public abstract onWorkerFail(event: WorkerFailEvent): Promise<void>;

  /** A coordinator<->coordinator message (any non-worker topic). */
  public abstract onMessage(event: Message): Promise<void>;
}
