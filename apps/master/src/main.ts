import { randomUUID } from 'node:crypto';
import { parseArgs } from 'node:util';
import { initTelemetry } from '@mozart/telemetry';
import { loadEnv } from './config/env';
import { loadScenario } from './scenario/scenario';

async function bootstrap(): Promise<void> {
  const { values } = parseArgs({
    options: {
      scenario: { type: 'string', short: 's' },
      'dry-run': { type: 'boolean', default: false },
    },
  });
  if (!values.scenario) {
    throw new Error('usage: master --scenario <path.yaml> [--dry-run]');
  }

  const env = loadEnv();
  const scenario = loadScenario(values.scenario);
  const runId = `${scenario.name}-${randomUUID().slice(0, 8)}`;

  // Telemetry first: instrumentation must patch libraries before the app graph
  // (which pulls in pg) is imported.
  const telemetry = initTelemetry({
    serviceName: 'mozart-master',
    attributes: { 'mozart.run_id': runId },
    otlpUrl: env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
    processor: env.MOZART_OTEL_PROCESSOR,
  });

  const { NestFactory } = await import('@nestjs/core');
  const { AppModule } = await import('./app.module.js');
  const { RunService } = await import('./run/run.service.js');

  const app = await NestFactory.createApplicationContext(
    AppModule.forRun({ scenario, runId, env }),
    { logger: ['log', 'warn', 'error'] },
  );

  try {
    const summary = await app.get(RunService).run({ dryRun: values['dry-run'] });

    console.log('\n=== run summary ===');

    console.log(JSON.stringify(summary, null, 2));

    console.log('Jaeger: http://localhost:16686/search?service=mozart-master');
  } finally {
    await app.close();
    await telemetry.shutdown();
  }
}

bootstrap().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
