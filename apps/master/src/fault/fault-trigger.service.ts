import type { ConditionalKillFault, FaultHook, NodeId } from '@mozart/contracts';
import { Inject, Injectable, Logger } from '@nestjs/common';
import type { Scheduler } from '../clock/clock';
import { EventLogService } from '../event-log/event-log.service';
import { IpcHostService } from '../ipc-server/ipc-host.service';
import { ProcessManagerService } from '../ipc-server/process-manager.service';
import { MetricsService } from '../metrics/metrics.service';
import { SCHEDULER } from '../tokens';
import { TransportService } from '../transport/transport.service';

/** The action's message/context available to a fault's filter expression. */
export interface FaultCtx {
  message: unknown;
  topic?: string;
  node: NodeId;
  attempt?: number;
}

/** Returns true if it killed `ctx.node` (the caller must not run the guarded effect). */
export type FaultTriggerFn = (hook: FaultHook, phase: 'before' | 'after', ctx: FaultCtx) => boolean;

interface CompiledTrigger {
  readonly hook: FaultHook;
  readonly phase: 'before' | 'after';
  readonly restartAfterMs: number;
  readonly source: string;
  readonly test: (ctx: FaultCtx) => boolean;
  timesLeft: number;
}

/**
 * Conditional (message-filtered) fault triggers: kills the node involved in a
 * specific master choke point when its filter expression matches, exposing the
 * non-atomicity windows the temporal (`at`-scheduled) faults can't reach. Wires
 * itself into {@link IpcHostService} and {@link TransportService} via their
 * settable `trigger` field at construction — both live downstream of this
 * service's own module, so this stays a plain one-way dependency (no DI cycle).
 */
@Injectable()
export class FaultTriggerService {
  private readonly logger = new Logger(FaultTriggerService.name);
  private readonly triggers: CompiledTrigger[] = [];

  // biome-ignore lint/complexity/useMaxParams: deps injection
  constructor(
    @Inject(SCHEDULER) private readonly scheduler: Scheduler,
    private readonly pm: ProcessManagerService,
    private readonly events: EventLogService,
    private readonly metrics: MetricsService,
    private readonly ipcHost: IpcHostService,
    private readonly transport: TransportService,
  ) {
    this.ipcHost.trigger = (hook, phase, ctx) => this.fire(hook, phase, ctx);
    this.transport.trigger = (hook, phase, ctx) => this.fire(hook, phase, ctx);
  }

  /**
   * Register a conditional fault (called once per declared fault, at arm
   * time). `fault.filter` is a trusted research-harness scenario input,
   * evaluated master-side only — compiling it via `new Function` is deliberate.
   */
  public register(fault: ConditionalKillFault): void {
    const compiled = new Function('message', 'topic', 'node', 'attempt', `return (${fault.filter});`) as (
      message: unknown,
      topic: string | undefined,
      node: NodeId,
      attempt: number | undefined,
    ) => unknown;

    this.triggers.push({
      hook: fault.hook,
      phase: fault.phase,
      restartAfterMs: fault.restartAfterMs,
      source: fault.filter,
      timesLeft: fault.times,
      test: (ctx) => {
        try {
          return Boolean(compiled(ctx.message, ctx.topic, ctx.node, ctx.attempt));
        } catch (err) {
          this.logger.warn(`fault filter threw, treating as no-match: ${String(err)}`);
          return false;
        }
      },
    });
  }

  private fire(hook: FaultHook, phase: 'before' | 'after', ctx: FaultCtx): boolean {
    const trigger = this.triggers.find((t) => t.hook === hook && t.phase === phase && t.timesLeft > 0 && t.test(ctx));
    if (!trigger) return false;
    trigger.timesLeft -= 1;

    this.logger.log(`conditional fault matched: ${phase} ${hook} on ${ctx.node} (${trigger.source})`);
    this.events.record({
      type: 'fault.injected',
      nodeId: ctx.node,
      data: { action: 'conditionalKill', hook, phase, filter: trigger.source },
    });
    this.metrics.countFault('conditionalKill');
    this.pm.kill(ctx.node);
    this.scheduler.after(trigger.restartAfterMs, () => this.pm.restart(ctx.node));
    return true;
  }
}
