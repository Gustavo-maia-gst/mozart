import { randomUUID } from 'node:crypto';
import { parseArgs } from 'node:util';
import { fanStats } from '@mozart/contracts';
import { initTelemetry } from '@mozart/telemetry';
import { loadEnv } from './config/env';
import { latencyResourceAttrs } from './metrics/resource-attrs';
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

// biome-ignore lint/complexity/noExcessiveLinesPerFunction: bootstrap
async function bootstrap(): Promise<void> {
  const { values, positionals } = parseArgs({
    allowPositionals: true,
    options: {
      protocol: { type: 'string', short: 'p' },
      scenario: { type: 'string', short: 's' },
      nodes: { type: 'string', short: 'n' },
      'dry-run': { type: 'boolean', default: false },
    },
  });

  // Optional node-count override (e.g. `--nodes 5`) for scaling sweeps.
  let nodes: number | undefined;
  if (values.nodes !== undefined) {
    nodes = Number(values.nodes);
    if (!Number.isInteger(nodes) || nodes < 1) {
      throw new Error(`--nodes must be a positive integer, got "${values.nodes}"`);
    }
  }

  // Accept `master <protocol> [scenario]` positionally, or the equivalent
  // `--protocol`/`--scenario` flags. The scenario defaults to the protocol's
  // own file, and the passed protocol (if any) overrides the document.
  const protocol = values.protocol ?? positionals[0];
  const scenarioArg = values.scenario ?? positionals[1];
  const scenarioPath = resolveScenarioPath(scenarioArg, protocol);
  if (!scenarioPath) {
    throw new Error('usage: master <protocol> [scenario] | --scenario <path.yaml> [--nodes N] [--dry-run]');
  }

  const env = loadEnv();
  const scenario = loadScenario(scenarioPath, { protocol, nodes });
  const runId = `${scenario.name}-${randomUUID().slice(0, 8)}`;
  const { meanFanIn: fanIn, meanFanOut: fanOut } = fanStats(scenario.graphs);

  // Telemetry first: instrumentation must patch libraries before the app graph
  // (which pulls in pg) is imported.
  const telemetry = initTelemetry({
    serviceName: 'harness',
    // These ride as resource attributes and are promoted to Prometheus labels
    // (see prometheus.yml) — the axes for "under comms mean Y / storage mean Z,
    // compare protocols".
    attributes: {
      'mozart.run_id': runId,
      'mozart.protocol': scenario.protocol,
      'mozart.scenario': scenario.name,
      'mozart.seed': scenario.seed,
      // Effective coordinator count (baseline collapses to 1) — the scaling axis.
      'mozart.nodes': String(scenario.coordinatorIds().length),
      // Mean join/branch width of the DAGs — the fan-in/fan-out sweep axes.
      'mozart.fanin.mean': fanIn.toFixed(2),
      'mozart.fanout.mean': fanOut.toFixed(2),
      ...latencyResourceAttrs(scenario.latency),
    },
    otlpUrl: env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
    metricsOtlpUrl: env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT,
    metricExportIntervalMs: env.MOZART_METRIC_EXPORT_INTERVAL_MS,
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

    console.log('Jaeger:  http://localhost:2016/search?service=harness');
    console.log('Grafana: http://localhost:2000/d/mozart-protocol-comparison');
  } finally {
    await app.close();
    await telemetry.shutdown();
  }
}

bootstrap().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
