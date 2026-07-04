import type { Scenario } from '@mozart/contracts';
import { LatencyModel } from '@mozart/latency';
import { type DynamicModule, Global, Module } from '@nestjs/common';
import { ClockModule } from '../clock/clock';
import type { EnvConfig } from '../config/env';
import { ENV_CONFIG, LATENCY_MODEL, RUN_ID, SCENARIO } from '../tokens';

export interface CoreParams {
  scenario: Scenario;
  runId: string;
  env: EnvConfig;
}

/**
 * Global module providing the run-scoped singletons every feature module needs:
 * the parsed scenario, run id, env config, and the seeded latency model.
 */
@Global()
@Module({})
export class CoreModule {
  static forRun(params: CoreParams): DynamicModule {
    return {
      module: CoreModule,
      imports: [ClockModule],
      providers: [
        { provide: SCENARIO, useValue: params.scenario },
        { provide: RUN_ID, useValue: params.runId },
        { provide: ENV_CONFIG, useValue: params.env },
        {
          provide: LATENCY_MODEL,
          useFactory: () => new LatencyModel(params.scenario.seed, params.scenario.latency),
        },
      ],
      exports: [SCENARIO, RUN_ID, ENV_CONFIG, LATENCY_MODEL, ClockModule],
    };
  }
}
