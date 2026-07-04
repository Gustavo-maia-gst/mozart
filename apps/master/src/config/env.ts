import { z } from 'zod';

/**
 * Environment config = *where things are* (the scenario file is *what to run*).
 * Validated with zod; unknown keys are ignored.
 */
export const envSchema = z.object({
  /** Postgres connection string for the postgres storage adapter. */
  MOZART_PG_URL: z.string().default('postgres://mozart:mozart@localhost:5432/mozart'),
  /** OTLP traces endpoint. */
  OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: z.string().default('http://localhost:4318/v1/traces'),
  /** Directory for per-run event logs. */
  MOZART_LOG_DIR: z.string().default('runs'),
  /**
   * Path to the slave entrypoint the master forks. Defaults to the built dist;
   * set to the tsx entrypoint for dev runs.
   */
  MOZART_SLAVE_ENTRYPOINT: z.string().optional(),
  MOZART_OTEL_PROCESSOR: z.enum(['batch', 'simple']).optional(),
});

export type EnvConfig = z.infer<typeof envSchema>;

export function loadEnv(source: NodeJS.ProcessEnv = process.env): EnvConfig {
  return envSchema.parse(source);
}
