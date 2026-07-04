import { z } from 'zod';

/**
 * Environment config = *where things are* (the scenario file is *what to run*).
 * Validated with zod; unknown keys are ignored.
 */
export const envSchema = z.object({
  /** Postgres connection string for the postgres storage adapter. */
  MOZART_PG_URL: z.string().default('postgres://mozart:mozart@localhost:5432/mozart'),
  /** OTLP traces endpoint (Jaeger OTLP http, mapped to host :2018). */
  OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: z.string().default('http://localhost:2018/v1/traces'),
  /**
   * OTLP metrics endpoint → Prometheus's OTLP receiver (mapped to host :2090).
   * Note the path (/api/v1/otlp/v1/metrics) differs from traces.
   */
  OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: z.string().default('http://localhost:2090/api/v1/otlp/v1/metrics'),
  /** Directory for per-run event logs. */
  MOZART_LOG_DIR: z.string().default('runs'),
  /**
   * Path to the slave entrypoint the master forks. Defaults to the built dist;
   * set to the tsx entrypoint for dev runs.
   */
  MOZART_SLAVE_ENTRYPOINT: z.string().optional(),
  MOZART_OTEL_PROCESSOR: z.enum(['batch', 'simple']).optional(),
  /**
   * Metric export interval (ms). A run lasts seconds and metrics flush on
   * shutdown, so the OTel default (60s) yields a single sample per series —
   * enough for cumulative totals, but rate()/over-time panels need several. A
   * short interval spreads samples across the run so those panels populate.
   */
  MOZART_METRIC_EXPORT_INTERVAL_MS: z.coerce.number().int().positive().default(10),
});

export type EnvConfig = z.infer<typeof envSchema>;

export function loadEnv(source: NodeJS.ProcessEnv = process.env): EnvConfig {
  return envSchema.parse(source);
}
