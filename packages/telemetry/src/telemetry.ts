import { type Meter, type Tracer, metrics, trace } from '@opentelemetry/api';
import { AggregationTemporalityPreference, OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-http';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { resourceFromAttributes } from '@opentelemetry/resources';
import {
  AggregationType,
  InstrumentType,
  MeterProvider,
  PeriodicExportingMetricReader,
} from '@opentelemetry/sdk-metrics';
import {
  BatchSpanProcessor,
  NodeTracerProvider,
  SimpleSpanProcessor,
  type SpanProcessor,
} from '@opentelemetry/sdk-trace-node';
import { ATTR_SERVICE_NAME } from '@opentelemetry/semantic-conventions';

export const TRACER_NAME = 'mozart';

export interface InitTelemetryOptions {
  serviceName: string;
  /** Extra resource attributes, e.g. mozart.node_id / mozart.run_id / mozart.protocol. */
  attributes?: Record<string, string>;
  /**
   * 'batch' (default) bounds export overhead but risks losing the last
   * <batchDelayMs of spans on SIGKILL. 'simple' exports synchronously (use for
   * debugging). Overridable via env MOZART_OTEL_PROCESSOR.
   */
  processor?: 'batch' | 'simple';
  /** OTLP http endpoint; defaults to env or http://localhost:4318/v1/traces. */
  otlpUrl?: string;
  batchDelayMs?: number;
  /**
   * OTLP http endpoint for metrics. NOTE: the path differs from traces
   * (/v1/metrics), and the default targets Prometheus's OTLP receiver, not
   * Jaeger. Defaults to env OTEL_EXPORTER_OTLP_METRICS_ENDPOINT or Prometheus.
   */
  metricsOtlpUrl?: string;
  /**
   * Periodic metric export interval. Deliberately long: for short-lived runs the
   * authoritative export is the forceFlush on shutdown, not this timer.
   */
  metricExportIntervalMs?: number;
}

export interface Telemetry {
  tracer: Tracer;
  meter: Meter;
  shutdown(): Promise<void>;
}

/**
 * Must be called before any instrumented module is imported. Registers a
 * global tracer provider with the default W3C propagator and AsyncLocalStorage
 * context manager (so `context.active()` follows async flow within a process),
 * plus a global meter provider that pushes OTLP metrics (cumulative) — grouping
 * dimensions ride along as resource attributes (opts.attributes).
 */
export function initTelemetry(opts: InitTelemetryOptions): Telemetry {
  // `||` (not `??`): an empty string env/opt must fall through to the default,
  // otherwise the OTLP exporter throws on an invalid ('') URL.
  const url = opts.otlpUrl || process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT || 'http://localhost:4318/v1/traces';

  // Shared resource: serviceName + all grouping attrs flow into both traces and
  // metrics identically (so PromQL can group by mozart.protocol/run_id/…).
  const resource = resourceFromAttributes({
    [ATTR_SERVICE_NAME]: opts.serviceName,
    ...opts.attributes,
  });

  // --- Traces ---------------------------------------------------------------
  const exporter = new OTLPTraceExporter({ url });
  const kind = opts.processor ?? (process.env.MOZART_OTEL_PROCESSOR as 'simple' | undefined) ?? 'batch';
  const processor: SpanProcessor =
    kind === 'simple'
      ? new SimpleSpanProcessor(exporter)
      : new BatchSpanProcessor(exporter, { scheduledDelayMillis: opts.batchDelayMs ?? 200 });

  const provider = new NodeTracerProvider({ resource, spanProcessors: [processor] });
  provider.register();

  // --- Metrics --------------------------------------------------------------
  const metricsUrl =
    opts.metricsOtlpUrl ||
    process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT ||
    'http://localhost:9090/api/v1/otlp/v1/metrics';

  const metricExporter = new OTLPMetricExporter({
    url: metricsUrl,
    // Prometheus's OTLP receiver only accepts cumulative; each run's unique
    // mozart.run_id makes it a distinct series, so no cross-run reset problem.
    temporalityPreference: AggregationTemporalityPreference.CUMULATIVE,
  });
  const metricReader = new PeriodicExportingMetricReader({
    exporter: metricExporter,
    exportIntervalMillis: opts.metricExportIntervalMs ?? 60_000,
  });
  const meterProvider = new MeterProvider({
    resource,
    readers: [metricReader],
    // Exponential (base-2) histograms for every histogram instrument: accurate
    // p95/p99 across orders of magnitude with no hand-tuned buckets. Counters /
    // up-down counters are unaffected.
    views: [
      {
        instrumentType: InstrumentType.HISTOGRAM,
        aggregation: { type: AggregationType.EXPONENTIAL_HISTOGRAM, options: { maxSize: 160 } },
      },
    ],
  });
  metrics.setGlobalMeterProvider(meterProvider);

  return {
    tracer: trace.getTracer(TRACER_NAME),
    meter: metrics.getMeter(TRACER_NAME),
    shutdown: async () => {
      // Flush metrics first: the final cumulative values must land before exit.
      // Swallow metric errors (no backend ⇒ export fails) so a run never breaks.
      await meterProvider.forceFlush().catch(() => {});
      await meterProvider.shutdown().catch(() => {});
      await provider.shutdown();
    },
  };
}
