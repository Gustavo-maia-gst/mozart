import { metrics } from '@opentelemetry/api';
import {
  AggregationTemporality,
  type DataPoint,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from '@opentelemetry/sdk-metrics';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { MetricsService } from './metrics.service';

// Attribute keys a metric point must NEVER carry — high cardinality lives in the
// event log / traces, not in metric labels.
const FORBIDDEN = ['taskId', 'task_id', 'messageId', 'deliveryId', 'channel', 'nodeId', 'node_id'];

describe('MetricsService', () => {
  let exporter: InMemoryMetricExporter;
  let provider: MeterProvider;

  beforeEach(() => {
    exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
    provider = new MeterProvider({
      readers: [new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 60_000 })],
    });
    metrics.setGlobalMeterProvider(provider);
  });

  afterEach(async () => {
    await provider.shutdown();
    metrics.disable(); // allow the next test to re-install a fresh global provider
  });

  /** Force an export and index every emitted data point by instrument name. */
  async function collect(): Promise<Map<string, DataPoint<unknown>[]>> {
    await provider.forceFlush();
    const byName = new Map<string, DataPoint<unknown>[]>();
    for (const rm of exporter.getMetrics()) {
      for (const scope of rm.scopeMetrics) {
        for (const m of scope.metrics) {
          byName.set(m.descriptor.name, m.dataPoints as DataPoint<unknown>[]);
        }
      }
    }
    return byName;
  }

  it('counts messages per bounded type', async () => {
    const metricsSvc = new MetricsService();
    metricsSvc.countMessage('published');
    metricsSvc.countMessage('published');
    metricsSvc.countMessage('acked');

    const points = (await collect()).get('mozart.messages') ?? [];
    const byType = new Map(points.map((p) => [p.attributes.type, p.value]));
    expect(byType.get('published')).toBe(2);
    expect(byType.get('acked')).toBe(1);
  });

  it('records storage op durations keyed by op only', async () => {
    const metricsSvc = new MetricsService();
    metricsSvc.observeStorageOpDuration('read', 5);
    metricsSvc.observeStorageOpDuration('read', 7);
    metricsSvc.observeStorageOpDuration('save', 3);

    const points = (await collect()).get('mozart.storage.op.duration') ?? [];
    const read = points.find((p) => p.attributes.op === 'read');
    expect((read?.value as { count: number }).count).toBe(2);
    expect(points.map((p) => p.attributes.op).sort()).toEqual(['read', 'save']);
  });

  it('tracks held leases as an up/down counter', async () => {
    const metricsSvc = new MetricsService();
    metricsSvc.leaseAcquired();
    metricsSvc.leaseAcquired();
    metricsSvc.leaseReleased();

    const points = (await collect()).get('mozart.storage.leases.held') ?? [];
    expect(points[0]?.value).toBe(1);
  });

  it('never attaches high-cardinality attributes to any point', async () => {
    const metricsSvc = new MetricsService();
    metricsSvc.countMessage('delivered');
    metricsSvc.countStorageOp('save');
    metricsSvc.countWorkerTask('completed');
    metricsSvc.countFault('killNode');
    metricsSvc.countNodeLifecycle('spawned');
    metricsSvc.observeDeliverDuration(10);
    metricsSvc.observeWorkerTaskDuration(50);
    metricsSvc.observeAckLatency(12);
    metricsSvc.observeLockWait(3);
    metricsSvc.observeMakespan(200);

    for (const points of (await collect()).values()) {
      for (const p of points) {
        for (const key of Object.keys(p.attributes)) {
          expect(FORBIDDEN).not.toContain(key);
        }
      }
    }
  });
});
