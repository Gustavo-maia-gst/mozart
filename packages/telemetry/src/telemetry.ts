import { type Tracer, trace } from '@opentelemetry/api';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { resourceFromAttributes } from '@opentelemetry/resources';
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
  /** Extra resource attributes, e.g. mozart.node_id / mozart.run_id. */
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
}

export interface Telemetry {
  tracer: Tracer;
  shutdown(): Promise<void>;
}

/**
 * Must be called before any instrumented module is imported. Registers a
 * global tracer provider with the default W3C propagator and AsyncLocalStorage
 * context manager (so `context.active()` follows async flow within a process).
 */
export function initTelemetry(opts: InitTelemetryOptions): Telemetry {
  // `||` (not `??`): an empty string env/opt must fall through to the default,
  // otherwise the OTLP exporter throws on an invalid ('') URL.
  const url = opts.otlpUrl || process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT || 'http://localhost:4318/v1/traces';

  const exporter = new OTLPTraceExporter({ url });
  const kind = opts.processor ?? (process.env.MOZART_OTEL_PROCESSOR as 'simple' | undefined) ?? 'batch';
  const processor: SpanProcessor =
    kind === 'simple'
      ? new SimpleSpanProcessor(exporter)
      : new BatchSpanProcessor(exporter, { scheduledDelayMillis: opts.batchDelayMs ?? 200 });

  const provider = new NodeTracerProvider({
    resource: resourceFromAttributes({
      [ATTR_SERVICE_NAME]: opts.serviceName,
      ...opts.attributes,
    }),
    spanProcessors: [processor],
  });
  provider.register();

  return {
    tracer: trace.getTracer(TRACER_NAME),
    shutdown: () => provider.shutdown(),
  };
}
