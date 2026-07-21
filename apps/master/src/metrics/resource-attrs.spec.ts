import type { Scenario } from '@mozart/contracts';
import { describe, expect, it } from 'vitest';
import { latencyResourceAttrs, storageBackendLatencyAttrs } from './resource-attrs';

// The helper only reads the latency map; a plain cast keeps the test focused.
const latency = (m: Record<string, unknown>): Scenario['latency'] => m as Scenario['latency'];

// storageBackendLatencyAttrs reads only `storage.adapter` and the `latency` map.
const scenario = (adapter: 'postgres' | 'in-memory', lat: Record<string, unknown>): Scenario =>
  ({ storage: { adapter }, latency: lat } as unknown as Scenario);

describe('latencyResourceAttrs', () => {
  it('flattens each distribution to a mean (and stddev where it has one)', () => {
    expect(
      latencyResourceAttrs(
        latency({
          'transport.deliver': { distribution: 'constant', value: 10 },
          'storage.save': { distribution: 'normal', mean: 8, stddev: 2 },
          'storage.read': { distribution: 'uniform', min: 4, max: 6 },
          'worker.taskDuration': { distribution: 'lognormal', mu: 3, sigma: 0.5 },
        }),
      ),
    ).toEqual({
      'mozart.latency.transport_deliver.mean': '10',
      'mozart.latency.transport_deliver.stddev': '0',
      'mozart.latency.storage_save.mean': '8',
      'mozart.latency.storage_save.stddev': '2',
      'mozart.latency.storage_read.mean': '5', // (4+6)/2, uniform has no stddev
      'mozart.latency.worker_taskDuration.mean': '3',
      'mozart.latency.worker_taskDuration.stddev': '0.5',
    });
  });

  it('is empty for a scenario with no configured latency', () => {
    expect(latencyResourceAttrs(latency({}))).toEqual({});
  });
});

describe('storageBackendLatencyAttrs', () => {
  it('marks unsimulated storage latency as "postgresql" for the postgres adapter', () => {
    expect(storageBackendLatencyAttrs(scenario('postgres', {}))).toEqual({
      'mozart.latency.storage_read.mean': 'postgresql',
      'mozart.latency.storage_save.mean': 'postgresql',
    });
  });

  it('leaves a simulated storage latency alone (only fills unconfigured actions)', () => {
    expect(
      storageBackendLatencyAttrs(scenario('postgres', { 'storage.save': { distribution: 'constant', value: 5 } })),
    ).toEqual({ 'mozart.latency.storage_read.mean': 'postgresql' }); // save is simulated → left to latencyResourceAttrs
  });

  it('is empty for the in-memory adapter', () => {
    expect(storageBackendLatencyAttrs(scenario('in-memory', {}))).toEqual({});
  });
});
