import {
  type Delivery,
  type JsonObject,
  type ProtocolContext,
  type ProtocolSpi,
  WORKER_TOPICS,
} from '@mozart/contracts';
import { Injectable } from '@nestjs/common';

const PING = 'ping';
const PONG = 'pong';

/**
 * Smoke-test protocol. Not a real coordination protocol — it just exercises
 * every harness surface: storage save + readExclusive (under lock), transport
 * publish in both directions, worker.start and completion handling, and
 * idempotent handling of duplicated/redelivered messages.
 */
@Injectable()
export class EchoProtocol implements ProtocolSpi {
  readonly name = 'echo';

  async onActivate(ctx: ProtocolContext): Promise<void> {
    await ctx.storage.save(`echo:${ctx.nodeId}`, { hello: ctx.nodeId, pings: 0 });
    ctx.log.info('activate', { peers: ctx.scenario.nodes.length - 1 });

    for (const peer of this.peers(ctx)) {
      await ctx.transport.publish(peer, PING, { from: ctx.nodeId });
    }
    // The first node kicks off the DAG.
    const first = ctx.scenario.dag.tasks[0];
    if (first && ctx.nodeId === ctx.scenario.nodes[0]) {
      await ctx.workers.start(first.id);
    }
  }

  async onMessage(delivery: Delivery, ctx: ProtocolContext): Promise<void> {
    switch (delivery.topic) {
      case PING:
        await this.handlePing(delivery, ctx);
        return;
      case PONG:
        ctx.log.info('pong', { from: delivery.from });
        return;
      case WORKER_TOPICS.completed:
        await this.handleCompleted(delivery, ctx);
        return;
      case WORKER_TOPICS.failed:
        ctx.log.warn('task failed', { body: delivery.body as JsonObject });
        return;
      default:
        ctx.log.warn('unknown topic', { topic: delivery.topic });
    }
  }

  /** Idempotent under at-least-once: dedupe by messageId in storage S. */
  private async handlePing(delivery: Delivery, ctx: ProtocolContext): Promise<void> {
    const lease = await ctx.storage.readExclusive(`echo:${ctx.nodeId}`);
    const state = (lease.data ?? { hello: ctx.nodeId, pings: 0, seen: {} }) as {
      hello: string;
      pings: number;
      seen?: Record<string, boolean>;
    };
    const seen = state.seen ?? {};
    if (seen[delivery.messageId]) {
      await lease.release(); // duplicate/redelivery — no-op, but still ack
      ctx.log.debug('duplicate ping ignored', { messageId: delivery.messageId });
      return;
    }
    seen[delivery.messageId] = true;
    await lease.save({ ...state, pings: state.pings + 1, seen });
    await ctx.transport.publish(delivery.from, PONG, { from: ctx.nodeId });
  }

  private async handleCompleted(delivery: Delivery, ctx: ProtocolContext): Promise<void> {
    const body = delivery.body as { taskId?: string };
    const tasks = ctx.scenario.dag.tasks;
    const idx = tasks.findIndex((t) => t.id === body.taskId);
    ctx.log.info('task completed', { taskId: body.taskId ?? null });
    const next = idx >= 0 ? tasks[idx + 1] : undefined;
    if (next && ctx.nodeId === ctx.scenario.nodes[0]) {
      await ctx.workers.start(next.id);
    }
  }

  private peers(ctx: ProtocolContext): string[] {
    return ctx.scenario.nodes.filter((n) => n !== ctx.nodeId);
  }
}
