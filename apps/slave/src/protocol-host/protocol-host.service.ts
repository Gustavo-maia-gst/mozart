import { type Delivery, type Graph, type PushType, type ScenarioInfo } from '@mozart/contracts';
import type { IpcClient } from '@mozart/ipc';
import { Protocol } from '@mozart/protocols';
import {
  annotateSpan,
  ATTR,
  runInTraceScope,
  runWithExtractedContext,
  setTaskId,
  SpanKind,
  Trace,
} from '@mozart/telemetry';
import { Inject, Injectable, Logger } from '@nestjs/common';
import { IPC_CLIENT, NODE_ID } from '../tokens';

interface BufferedPush {
  type: PushType;
  payload: unknown;
  traceCtx: Record<string, string>;
}

/**
 * Drives the injected protocol's lifecycle on the slave: handshake, then for
 * each graph in the run persist + start it, and route deliveries to onMessage
 * under the ack-on-resolve contract. The protocol reaches the world through its
 * own injected ports — this host only sequences the calls and traces them.
 */
@Injectable()
export class ProtocolHostService {
  private readonly logger = new Logger(ProtocolHostService.name);
  private scenario?: ScenarioInfo;
  private ready = false;
  private readonly buffer: BufferedPush[] = [];

  constructor(
    @Inject(IPC_CLIENT) private readonly ipc: IpcClient,
    @Inject(NODE_ID) private readonly nodeId: string,
    private readonly protocol: Protocol,
  ) {}

  async start(): Promise<void> {
    // Register the push handler first; anything arriving before the handshake
    // completes is buffered and drained once the scenario is known.
    this.ipc.onPush((type, payload, frame) => this.onPush(type, payload, frame.traceCtx));
    const { scenario } = await this.ipc.call('node.ready', {});
    this.scenario = scenario;
    this.ready = true;
    for (const p of this.buffer.splice(0)) this.onPush(p.type, p.payload, p.traceCtx);
  }

  private onPush(type: PushType, payload: unknown, traceCtx: Record<string, string>): void {
    if (!this.ready) {
      this.buffer.push({ type, payload, traceCtx });
      return;
    }
    // Dispatch under the pushed trace context (one tree across processes) and an
    // ambient scope carrying this node's id.
    void runWithExtractedContext(traceCtx, () =>
      runInTraceScope({ nodeId: this.nodeId }, () => this.dispatch(type, payload)),
    );
  }

  private async dispatch(type: PushType, payload: unknown): Promise<void> {
    if (type === 'protocol.activate') {
      await this.runActivate();
    } else if (type === 'protocol.deactivate') {
      process.exit(0); // graceful shutdown; telemetry flush happens in main
    } else if (type === 'delivery') {
      await this.runDelivery(payload as Delivery);
    }
  }

  /** Persist and start every graph in the run. */
  private async runActivate(): Promise<void> {
    for (const graph of this.scenario?.graphs ?? []) {
      await this.persistGraph(graph);
      await this.startGraph(graph);
    }
  }

  /** Ack-on-resolve: a resolved handler acks; a rejection leaves it for redelivery. */
  private async runDelivery(delivery: Delivery): Promise<void> {
    const taskId = (delivery.body as { taskId?: unknown }).taskId;
    if (typeof taskId === 'string') setTaskId(taskId);
    try {
      await this.onMessage(delivery);
      await this.ipc.call('transport.ack', { deliveryId: delivery.deliveryId });
    } catch (err) {
      // No ack => the transport will redeliver.
      this.logger.warn(`onMessage failed (no ack, will redeliver): ${String(err)}`);
    }
  }

  @Trace({ name: 'protocol.persistGraph' })
  private async persistGraph(graph: Graph): Promise<void> {
    annotateSpan({ 'mozart.graph_id': graph.id });
    await this.protocol.persistGraph(graph);
  }

  @Trace({ name: 'protocol.startGraph' })
  private async startGraph(graph: Graph): Promise<void> {
    annotateSpan({ 'mozart.graph_id': graph.id });
    await this.protocol.startGraph(graph.id);
  }

  @Trace({ name: 'protocol.onMessage', kind: SpanKind.CONSUMER })
  private async onMessage(delivery: Delivery): Promise<void> {
    annotateSpan({
      [ATTR.topic]: delivery.topic,
      [ATTR.messageId]: delivery.messageId,
      [ATTR.attempt]: delivery.attempt,
    });
    await this.protocol.onMessage(delivery);
  }
}
