import type { Graph, GraphId } from './graph';
import type { Delivery } from './ports';
import type { ProtocolContext, ProtocolSpi } from './spi';

/**
 * Base class every coordination protocol extends. It defines the three
 * graph-centric operations a protocol must implement, and bridges them onto the
 * harness lifecycle (`ProtocolSpi`) so the slave host can drive it unchanged.
 *
 * The bound `ctx` (transport / storage / workers / log) is available to all
 * methods; it is set at activation.
 */
export abstract class Protocol implements ProtocolSpi {
  abstract readonly name: string;

  /** Harness context, bound at activation. */
  protected ctx!: ProtocolContext;

  /**
   * Persist `graph` into S in whatever layout this protocol needs. Called once
   * per owned graph, before execution — it is setup, not part of the run.
   */
  abstract persistGraph(graph: Graph): Promise<void>;

  /** Begin executing an already-persisted graph. */
  abstract startGraph(graphId: GraphId): Promise<void>;

  /**
   * Entrypoint for every inbound message (worker completion/failure events and
   * inter-coordinator messages). At-least-once: must be idempotent.
   */
  abstract onMessage(message: Delivery): Promise<void>;

  // --- ProtocolSpi bridge (harness lifecycle) --------------------------------

  /** Binds the context, then persists and starts each graph in the run. */
  async onActivate(ctx: ProtocolContext): Promise<void> {
    this.ctx = ctx;
    for (const graph of ctx.scenario.graphs) {
      await this.persistGraph(graph);
      await this.startGraph(graph.id);
    }
  }
}
