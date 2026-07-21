import { criticalPathCost, graphId, type Scenario } from '@mozart/contracts';
import { annotateSpan, ATTR, Trace } from '@mozart/telemetry';
import { Inject, Injectable, Logger } from '@nestjs/common';
import type { Clock, Scheduler } from '../clock/clock';
import { EventLogService } from '../event-log/event-log.service';
import { FaultInjectorService } from '../fault/fault-injector.service';
import { ProcessManagerService } from '../ipc-server/process-manager.service';
import { MetricsService } from '../metrics/metrics.service';
import { StorageService } from '../storage/storage.service';
import { CLOCK, RUN_ID, SCENARIO, SCHEDULER } from '../tokens';
import { TransportService } from '../transport/transport.service';
import { WorkerPoolService } from '../worker-pool/worker-pool.service';
import { ActivationService } from './activation.service';

const READY_TIMEOUT_MS = 10_000;
const DRAIN_MS = 300;
/** Max time to wait for slaves to flush + exit on SIGTERM before SIGKILL. */
const SHUTDOWN_GRACE_MS = 2_000;

export interface RunOptions {
  dryRun?: boolean;
}

export interface RunSummary {
  runId: string;
  scenario: string;
  logPath: string;
  events: Record<string, number>;
}

/** Orchestrates a run: spawn slaves, activate, wait for the end condition, shut down. */
@Injectable()
export class RunService {
  private readonly logger = new Logger(RunService.name);

  // biome-ignore lint/complexity/useMaxParams: deps injection
  constructor(
    @Inject(SCENARIO) private readonly scenario: Scenario,
    @Inject(RUN_ID) private readonly runId: string,
    @Inject(SCHEDULER) private readonly scheduler: Scheduler,
    @Inject(CLOCK) private readonly clock: Clock,
    private readonly events: EventLogService,
    private readonly pm: ProcessManagerService,
    private readonly faults: FaultInjectorService,
    private readonly worker: WorkerPoolService,
    private readonly metrics: MetricsService,
    private readonly transport: TransportService,
    private readonly activation: ActivationService,
    private readonly storage: StorageService,
  ) {}

  public async run(opts: RunOptions = {}): Promise<RunSummary> {
    this.events.open();
    this.events.record({ type: 'run.started', data: { scenario: this.scenario.name } });
    this.logger.log(`run ${this.runId} — scenario "${this.scenario.name}"`);

    if (!opts.dryRun) await this.execute();

    this.events.record({ type: 'run.finished' });
    const summary: RunSummary = {
      runId: this.runId,
      scenario: this.scenario.name,
      logPath: this.events.logPath,
      events: this.events.countsByType(),
    };
    await this.events.close();
    return summary;
  }

  private async execute(): Promise<void> {
    this.pm.spawnAll();
    await this.pm.awaitAllReady(READY_TIMEOUT_MS);
    this.logger.log('all nodes ready — activating protocol');

    // The `run` span brackets the processing itself — activation through the last
    // graph completion — deliberately excluding the spawn/handshake lead-in and
    // the shutdown drain, so its extent matches the first start → last end.
    await this.coordinate();

    await this.pm.shutdown(SHUTDOWN_GRACE_MS); // SIGTERM + wait for flush/exit, SIGKILL stragglers
    await this.sleep(DRAIN_MS); // let trailing events land

    // Wipe shared storage now that every slave is gone — keeps a persistent
    // backend (postgres) from carrying this run's state into the next. The event
    // log (JSONL) is the durable record, so nothing analysable is lost.
    // Best-effort: a cleanup failure must not fail an otherwise-complete run.
    await this.storage.clear().catch((err: unknown) => {
      this.logger.warn(`storage clear failed: ${String(err)}`);
    });
  }

  /**
   * The single root span for the run. Because `activateAll` and `scheduleGraphStarts`
   * run while it is active, their pushes carry its context to every node — so every
   * graph, worker execution and redelivery descends from this one span and Jaeger
   * shows the whole run as ONE trace (all graphs mixed). Ends as soon as every graph
   * reports completion; the end-condition is only the safety cap for graphs that
   * never finish (e.g. an injected failTask).
   */
  @Trace({ name: 'run' })
  private async coordinate(): Promise<void> {
    annotateSpan({ [ATTR.runId]: this.runId });
    // Persist ALL graphs first (master-side, protocol-defined layout); only then
    // start any — a start must never race a persist.
    await this.activation.persistAllGraphs();
    const activatedAt = this.clock.now();
    this.faults.arm(); // fault `at` offsets count from activation
    this.scheduleGraphStarts(); // start offsets also count from activation

    this.metrics.observeCriticalPath(this.criticalPathMs());

    const outcome = await this.transport.awaitAllGraphsComplete(this.scenario.endConditionMs);
    this.logger.log(outcome === 'complete' ? 'all graphs complete' : 'end condition (timeout) reached');

    // Makespan: activation → last worker completion. Undefined if nothing ran to
    // completion (e.g. the DAG never finished) — then we record nothing.
    const lastCompletion = this.worker.lastCompletionAt();
    if (lastCompletion !== undefined) this.metrics.observeMakespan(lastCompletion - activatedAt);
  }

  /**
   * Theoretical makespan floor from activation: the latest ideal finish across
   * all graphs, each being its start offset plus its critical-path cost (longest
   * cost-weighted path). `makespan - criticalPath` is the coordination overhead.
   */
  private criticalPathMs(): number {
    const startById = new Map(this.scenario.graphStartSchedule().map((g) => [g.graphId, g.startAfterMs]));
    let floor = 0;
    for (const graph of this.scenario.graphs) {
      floor = Math.max(floor, (startById.get(graphId(graph)) ?? 0) + criticalPathCost(graph));
    }
    return floor;
  }

  /**
   * Trigger each graph's start per the scenario schedule: graphs with a 0 offset
   * start immediately (right after activation persists them), the rest are timed
   * off the seeded scheduler so the staggering is reproducible.
   */
  private scheduleGraphStarts(): void {
    for (const { graphId, startAfterMs } of this.scenario.graphStartSchedule()) {
      if (startAfterMs <= 0) this.pm.startGraph(graphId);
      else this.scheduler.after(startAfterMs, () => this.pm.startGraph(graphId));
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise<void>((resolve) => this.scheduler.after(ms, resolve));
  }
}
