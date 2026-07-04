import { trace } from '@opentelemetry/api';
import { InMemorySpanExporter, NodeTracerProvider, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-node';
import { beforeAll, describe, expect, it } from 'vitest';
import { injectActiveContext, runWithExtractedContext } from './propagation';

const exporter = new InMemorySpanExporter();

beforeAll(() => {
  const provider = new NodeTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
  provider.register();
});

describe('trace-context propagation across an envelope carrier', () => {
  it('links a child span on the extracting side to the injecting parent', async () => {
    const tracer = trace.getTracer('test');
    const carrier: Record<string, string> = {};

    // Producer side: active span injects into the carrier.
    tracer.startActiveSpan('producer', (span) => {
      injectActiveContext(carrier);
      span.end();
    });
    expect(carrier.traceparent).toBeDefined();

    // Consumer side: run with the extracted context, open a child span.
    runWithExtractedContext(carrier, () => {
      const child = tracer.startSpan('consumer');
      child.end();
    });

    const spans = exporter.getFinishedSpans();
    const producer = spans.find((s) => s.name === 'producer');
    const consumer = spans.find((s) => s.name === 'consumer');
    expect(producer).toBeDefined();
    expect(consumer).toBeDefined();
    // Same trace, and the consumer's parent is the producer span.
    expect(consumer!.spanContext().traceId).toBe(producer!.spanContext().traceId);
    expect(consumer!.parentSpanContext?.spanId).toBe(producer!.spanContext().spanId);
  });
});
