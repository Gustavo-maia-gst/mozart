import { randomUUID } from 'node:crypto';
import { parseArgs } from 'node:util';
import { initTelemetry } from '@mozart/telemetry';
import { loadEnv } from './config/env';
import { loadScenario } from './scenario/scenario';

/**
 * Resolves the scenario file to run. An explicit arg is used as a path when it
 * looks like one (has a slash or a `.y(a)ml` extension), else as a name under
 * `scenarios/`. With no arg it defaults to the protocol's own file. Returns
 * undefined when neither a scenario nor a protocol was provided.
 */
function resolveScenarioPath(arg: string | undefined, protocol: string | undefined): string | undefined {
  const name = arg ?? protocol;
  if (!name) return undefined;
  if (name.includes('/') || name.endsWith('.yaml') || name.endsWith('.yml')) return name;
  return `scenarios/${name}.yaml`;
}

async function bootstrap(): Promise<void> {
  const { values, positionals } = parseArgs({
    allowPositionals: true,
    options: {
      protocol: { type: 'string', short: 'p' },
      scenario: { type: 'string', short: 's' },
      'dry-run': { type: 'boolean', default: false },
    },
  });

  // Accept `master <protocol> [scenario]` positionally, or the equivalent
  // `--protocol`/`--scenario` flags. The scenario defaults to the protocol's
  // own file, and the passed protocol (if any) overrides the document.
  const protocol = values.protocol ?? positionals[0];
  const scenarioArg = values.scenario ?? positionals[1];
  const scenarioPath = resolveScenarioPath(scenarioArg, protocol);
  if (!scenarioPath) {
    throw new Error('usage: master <protocol> [scenario] | --scenario <path.yaml> [--dry-run]');
  }

  const env = loadEnv();
  const scenario = loadScenario(scenarioPath, protocol);
  const runId = `${scenario.name}-${randomUUID().slice(0, 8)}`;

  // Telemetry first: instrumentation must patch libraries before the app graph
  // (which pulls in pg) is imported.
  const telemetry = initTelemetry({
    serviceName: 'harness',
    attributes: { 'mozart.run_id': runId },
    otlpUrl: env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
    processor: env.MOZART_OTEL_PROCESSOR,
  });

  const { NestFactory } = await import('@nestjs/core');
  const { AppModule } = await import('./app.module.js');
  const { RunService } = await import('./run/run.service.js');

  const app = await NestFactory.createApplicationContext(AppModule.forRun({ scenario, runId, env }), {
    logger: ['log', 'warn', 'error'],
  });

  try {
    const summary = await app.get(RunService).run({ dryRun: values['dry-run'] });

    console.log('\n=== run summary ===');

    console.log(JSON.stringify(summary, null, 2));

    console.log('Jaeger: http://localhost:16686/search?service=harness');
  } finally {
    await app.close();
    await telemetry.shutdown();
  }
}

bootstrap().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
