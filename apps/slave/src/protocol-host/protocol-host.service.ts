import type {
  Delivery,
  JsonObject,
  ProtocolContext,
  ProtocolLogger,
  ProtocolSpi,
  PushType,
  ScenarioInfo,
} from '@mozart/contracts';
import type { IpcClient } from '@mozart/ipc';
import { resolveProtocol } from '@mozart/protocols';
import { ATTR, runWithExtractedContext, TRACER_NAME, withSpan } from '@mozart/telemetry';
import { Inject, Injectable, Logger } from '@nestjs/common';
import { ModuleRef } from '@nestjs/core';
import { type Attributes, SpanKind, trace } from '@opentelemetry/api';
import { StorageClient, TransportClient, WorkerPoolClient } from '../harness-client/ports';
import { IPC_CLIENT, NODE_ID, PROTOCOL_NAME } from '../tokens';

const tracer = trace.getTracer(TRACER_NAME);

interface BufferedPush {
  type: PushType;
  payload: unknown;
  traceCtx: Record<string, string>;
}

/**
 * Drives the protocol lifecycle on the slave: handshake, activation, delivery
 * dispatch with the ack-on-resolve contract, and graceful deactivation.
 */
@Injectable()
export class ProtocolHostService {
  private readonly logger = new Logger(ProtocolHostService.name);
  private protocol?: ProtocolSpi;
  private ctx?: ProtocolContext;
  private ready = false;
  private readonly buffer: BufferedPush[] = [];

  // biome-ignore lint/complexity/useMaxParams: deps injection
  constructor(
    @Inject(IPC_CLIENT) private readonly ipc: IpcClient,
    @Inject(NODE_ID) private readonly nodeId: string,
    @Inject(PROTOCOL_NAME) private readonly protocolName: string,
    private readonly moduleRef: ModuleRef,
    private readonly transport: TransportClient,
    private readonly storage: StorageClient,
    private readonly workers: WorkerPoolClient,
  ) {}

  async start(): Promise<void> {
    // Register the push handler first; anything arriving before we finish the
    // handshake is buffered and drained once the context exists.
    this.ipc.onPush((type, payload, frame) => this.onPush(type, payload, frame.traceCtx));
    const { scenario } = await this.ipc.call('node.ready', {});
    this.protocol = await this.moduleRef.create(resolveProtocol(this.protocolName));
    this.ctx = this.buildContext(scenario);
    this.ready = true;
    for (const p of this.buffer.splice(0)) this.onPush(p.type, p.payload, p.traceCtx);
  }

  private onPush(type: PushType, payload: unknown, traceCtx: Record<string, string>): void {
    if (!this.ready) {
      this.buffer.push({ type, payload, traceCtx });
      return;
    }
    // Run the whole dispatch under the pushed trace context, so every span the
    // handler opens descends from the master-side producer span (one tree).
    void runWithExtractedContext(traceCtx, () => this.dispatch(type, payload));
  }

  private async dispatch(type: PushType, payload: unknown): Promise<void> {
    if (type === 'protocol.activate') {
      await this.runActivate();
    } else if (type === 'protocol.deactivate') {
      await this.runDeactivate();
    } else if (type === 'delivery') {
      await this.runDelivery(payload as Delivery);
    }
  }

  private async runActivate(): Promise<void> {
    await withSpan(tracer, 'protocol.onActivate', { attributes: this.baseAttrs() }, () =>
      this.protocol!.onActivate(this.ctx!),
    );
  }

  private async runDeactivate(): Promise<void> {
    try {
      if (this.protocol?.onDeactivate) {
        await withSpan(tracer, 'protocol.onDeactivate', { attributes: this.baseAttrs() }, () =>
          this.protocol!.onDeactivate!(this.ctx!),
        );
      }
    } finally {
      // Graceful exit; flush telemetry happens via SDK shutdown in main.
      process.exit(0);
    }
  }

  /** Ack-on-resolve: a resolved handler acks; a rejection leaves it for redelivery. */
  private async runDelivery(delivery: Delivery): Promise<void> {
    try {
      // dispatch() already runs under the pushed trace context (== delivery.traceCtx).
      await withSpan(
        tracer,
        'protocol.onMessage',
        {
          kind: SpanKind.CONSUMER,
          attributes: {
            ...this.baseAttrs(),
            [ATTR.topic]: delivery.topic,
            [ATTR.messageId]: delivery.messageId,
            [ATTR.attempt]: delivery.attempt,
          },
        },
        () => this.protocol!.onMessage(delivery, this.ctx!),
      );
      await this.ipc.call('transport.ack', { deliveryId: delivery.deliveryId });
    } catch (err) {
      // No ack => the transport will redeliver.
      this.logger.warn(`onMessage failed (no ack, will redeliver): ${String(err)}`);
    }
  }

  private buildContext(scenario: ScenarioInfo): ProtocolContext {
    return {
      nodeId: this.nodeId,
      scenario,
      transport: this.transport,
      storage: this.storage,
      workers: this.workers,
      log: this.makeLogger(),
    };
  }

  private baseAttrs() {
    return { [ATTR.nodeId]: this.nodeId, [ATTR.protocol]: this.protocolName };
  }

  private makeLogger(): ProtocolLogger {
    const method = { debug: 'debug', info: 'log', warn: 'warn', error: 'error' } as const;
    const emit =
      (level: keyof typeof method) =>
      (message: string, attrs?: JsonObject): void => {
        trace.getActiveSpan()?.addEvent(message, attrs as Attributes | undefined);
        (this.logger[method[level]] as (m: string) => void)(`[${this.nodeId}] ${message}`);
      };
    return { debug: emit('debug'), info: emit('info'), warn: emit('warn'), error: emit('error') };
  }
}
