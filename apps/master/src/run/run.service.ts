import { Inject, Injectable, Logger } from '@nestjs/common';
import type { Scenario } from '@mozart/contracts';
import { EventLogService } from '../event-log/event-log.service';
import { RUN_ID, SCENARIO } from '../tokens';

export interface RunOptions {
  dryRun?: boolean;
}

export interface RunSummary {
  runId: string;
  scenario: string;
  logPath: string;
  events: Record<string, number>;
}

/**
 * Orchestrates a run's lifecycle. This skeleton covers config/event-log wiring;
 * spawning slaves, arming faults and end conditions are layered in later.
 */
@Injectable()
export class RunService {
  private readonly logger = new Logger(RunService.name);

  constructor(
    @Inject(SCENARIO) private readonly scenario: Scenario,
    @Inject(RUN_ID) private readonly runId: string,
    private readonly events: EventLogService,
  ) {}

  async run(opts: RunOptions = {}): Promise<RunSummary> {
    this.events.open();
    this.events.record({ type: 'run.started', data: { scenario: this.scenario.name } });
    this.logger.log(`run ${this.runId} — scenario "${this.scenario.name}" (protocol=${this.scenario.protocol})`);

    if (opts.dryRun) {
      this.logger.log(
        `dry-run: ${this.scenario.nodes.length} node(s), ${this.scenario.dag.tasks.length} task(s), ${this.scenario.faults.length} fault(s)`,
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
}
