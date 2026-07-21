import {
  type Graph,
  type GraphId,
  type Message,
  ProtocolLogger,
  StoragePort,
  TransportPort,
  type WorkerFailEvent,
  type WorkerSuccessEvent,
} from '@mozart/contracts';
import { Injectable } from '@nestjs/common';

/**
 * Base class every coordination protocol extends.
 *
 * It injects the world a protocol may touch (transport / storage / logger) via
 * the constructor and exposes them as `protected` — so a concrete protocol just
 * extends this class and uses `this.storage` etc., with no constructor and no
 * `@Inject` (the ports are abstract classes, used as DI tokens by type). Work is
 * dispatched to W and messages to peers both through `this.transport`.
 *
 * Setup:
 *  - `persistGraph` — how a graph is stored in S before it runs (setup only);
 *  - `onStartup`    — recover/re-drive from S right after (re)instantiation;
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
    protected readonly log: ProtocolLogger,
  ) {}

  abstract readonly name: string;

  /**
   * Called once, right after the protocol is (re)instantiated and before any
   * `graph.start` or delivery is processed — on a fresh start AND on a stateless
   * restart after a crash. The master only ever sends `graph.start` once, so a
   * restarted coordinator would otherwise never revive a graph whose tasks were
   * ready but not yet dispatched when it crashed. Stateless protocols override
   * this to rebuild and re-drive their frontier from S; the default is a no-op
   * (on a fresh start nothing is persisted yet, so overrides no-op then too).
   */
  public onStartup(): Promise<void> {
    return Promise.resolve();
  }

  public abstract startGraph(graphId: GraphId): Promise<void>;

  /** A coordinator<->coordinator message (any non-worker topic). */
  public abstract onMessage(event: Message): Promise<void>;

  /** A task previously dispatched to the Worker Pool completed successfully. */
  public abstract onWorkerSuccess(event: WorkerSuccessEvent): Promise<void>;

  /** A task dispatched to the Worker Pool failed. */
  public abstract onWorkerFail(event: WorkerFailEvent): Promise<void>;

  public abstract persistGraph(graph: Graph): Promise<void>;
}
