import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { existsSync, readdirSync, statSync } from 'node:fs';
import { extname, join } from 'node:path';
import { parseArgs } from 'node:util';
import { fanStats } from '@mozart/contracts';
import { initTelemetry } from '@mozart/telemetry';
import { loadEnv } from './config/env';
import { latencyResourceAttrs, storageBackendLatencyAttrs } from './metrics/resource-attrs';
import { loadScenario } from './scenario/scenario';

/**
 * Resolves the scenario file (or directory) to run. An explicit arg is used as
 * a path when it looks like one (has a slash or a `.y(a)ml` extension), else
 * as a name under `scenarios/`. With no arg it defaults to the protocol's own
 * file. Returns undefined when neither a scenario nor a protocol was provided.
 */
function resolveScenarioPath(arg: string | undefined, protocol: string | undefined): string | undefined {
  const name = arg ?? protocol;
  if (!name) return undefined;
  if (name.includes('/') || name.endsWith('.yaml') || name.endsWith('.yml')) return name;
  return `scenarios/${name}.yaml`;
}

/** Every `.yaml`/`.yml` file directly under `dir`, sorted for a deterministic run order. */
function scenarioFilesIn(dir: string): string[] {
  return readdirSync(dir)
    .filter((f) => extname(f) === '.yaml' || extname(f) === '.yml')
    .sort((a, b) => a.localeCompare(b))
    .map((f) => join(dir, f));
}

/**
 * Re-invokes this same script (`node dist/main.js --scenario <file>`) as a
 * child process. One process per scenario, run sequentially: OTel's global
 * tracer/meter providers are "first registration wins", so looping several
 * scenarios' `initTelemetry()` calls in one process would silently break
 * telemetry from the second run on — a fresh process sidesteps that.
 */
function runInChildProcess(scenarioPath: string, extraArgs: string[]): Promise<number> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [process.argv[1] as string, '--scenario', scenarioPath, ...extraArgs], {
      stdio: 'inherit',
    });
    child.on('error', reject);
    child.on('exit', (code) => resolve(code ?? 1));
  });
}

/** Runs every scenario file under `dir` sequentially; throws if any of them failed. */
async function runDirectory(dir: string, extraArgs: string[]): Promise<void> {
  const files = scenarioFilesIn(dir);
  if (files.length === 0) throw new Error(`no .yaml/.yml scenarios found in ${dir}`);

  const results: { file: string; code: number }[] = [];
  for (const file of files) {
    console.log(`\n=== ${file} (${results.length + 1}/${files.length}) ===`);
    const code = await runInChildProcess(file, extraArgs);
    results.push({ file, code });
  }

  console.log('\n=== directory run summary ===');
  for (const { file, code } of results) console.log(`${code === 0 ? 'ok  ' : 'FAIL'}  ${file}`);

  const failed = results.filter((r) => r.code !== 0);
  if (failed.length > 0) throw new Error(`${failed.length}/${files.length} scenario(s) failed`);
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
    throw new Error('usage: master <protocol> [scenario] | --scenario <path.yaml|dir> [--nodes N] [--dry-run]');
  }

  // A directory runs every scenario file it contains, one child process each
  // (see runInChildProcess for why: OTel's global providers can't be
  // re-registered in-process between runs).
  if (existsSync(scenarioPath) && statSync(scenarioPath).isDirectory()) {
    const extraArgs = [
      ...(nodes !== undefined ? ['--nodes', String(nodes)] : []),
      ...(values['dry-run'] ? ['--dry-run'] : []),
    ];
    await runDirectory(scenarioPath, extraArgs);
    return;
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
      // Real (unsimulated) postgres storage latency surfaces as a "postgresql"
      // marker on the storage-latency label, so the dashboard axis distinguishes
      // real-backend runs from simulated-latency ones.
      ...storageBackendLatencyAttrs(scenario),
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
