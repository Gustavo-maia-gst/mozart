import {
  SpanKind,
  SpanStatusCode,
  trace,
  type Attributes,
  type Link,
  type Span,
  type Tracer,
} from '@opentelemetry/api';

/** Mozart span/attribute keys (mixed with standard messaging.* where they exist). */
export const ATTR = {
  nodeId: 'mozart.node_id',
  runId: 'mozart.run_id',
  protocol: 'mozart.protocol',
  channel: 'mozart.channel',
  taskId: 'mozart.task_id',
  messageId: 'messaging.message_id',
  deliveryId: 'mozart.delivery_id',
  attempt: 'mozart.attempt',
  topic: 'messaging.destination.name',
} as const;

export interface SpanOptions {
  kind?: SpanKind;
  attributes?: Attributes;
  links?: Link[];
}

/**
 * Runs `fn` inside an active span, recording exceptions and setting status.
 * The span becomes the active context for anything `fn` awaits.
 */
export async function withSpan<T>(
  tracer: Tracer,
  name: string,
  opts: SpanOptions,
  fn: (span: Span) => Promise<T>,
): Promise<T> {
  return tracer.startActiveSpan(
    name,
    { kind: opts.kind ?? SpanKind.INTERNAL, attributes: opts.attributes, links: opts.links },
    async (span) => {
      try {
        const result = await fn(span);
        span.setStatus({ code: SpanStatusCode.OK });
        return result;
      } catch (err) {
        span.recordException(err as Error);
        span.setStatus({
          code: SpanStatusCode.ERROR,
          message: err instanceof Error ? err.message : String(err),
        });
        throw err;
      } finally {
        span.end();
      }
    },
  );
}

/** Trace/span id of the currently active span, for stamping event-log records. */
export function activeIds(): { traceId?: string; spanId?: string } {
  const ctx = trace.getActiveSpan()?.spanContext();
  return ctx ? { traceId: ctx.traceId, spanId: ctx.spanId } : {};
}

export { SpanKind } from '@opentelemetry/api';
