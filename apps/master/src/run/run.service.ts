import type { Scenario } from '@mozart/contracts';
import { ATTR, TRACER_NAME, withSpan } from '@mozart/telemetry';
import { Inject, Injectable, Logger } from '@nestjs/common';
import { trace } from '@opentelemetry/api';
import type { Scheduler } from '../clock/clock';
import { EventLogService } from '../event-log/event-log.service';
import { FaultInjectorService } from '../fault/fault-injector.service';
import { ProcessManagerService } from '../ipc-server/process-manager.service';
import { RUN_ID, SCENARIO, SCHEDULER } from '../tokens';

const tracer = trace.getTracer(TRACER_NAME);
const READY_TIMEOUT_MS = 10_000;
const DRAIN_MS = 300;

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
    private readonly events: EventLogService,
    private readonly pm: ProcessManagerService,
    private readonly faults: FaultInjectorService,
  ) {}

  async run(opts: RunOptions = {}): Promise<RunSummary> {
    this.events.open();
    this.events.record({ type: 'run.started', data: { scenario: this.scenario.name } });
    this.logger.log(`run ${this.runId} — scenario "${this.scenario.name}"`);

    if (!opts.dryRun) {
      await withSpan(tracer, `run ${this.scenario.name}`, { attributes: { [ATTR.runId]: this.runId } }, () =>
        this.execute(),
      );
    }

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
    // activate under the run's active span so the whole run is one trace tree.
    this.pm.activateAll();
    this.faults.arm(); // fault `at` offsets count from activation

    await this.sleep(this.scenario.endCondition.ms);

    this.logger.log('end condition reached — shutting down');
    this.pm.shutdown();
    await this.sleep(DRAIN_MS); // let deactivate/exit + trailing events land
  }

  private sleep(ms: number): Promise<void> {
    return new Promise<void>((resolve) => this.scheduler.after(ms, resolve));
  }
}
