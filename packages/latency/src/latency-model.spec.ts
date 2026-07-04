import type { DistributionConfig } from '@mozart/contracts';
import { describe, expect, it } from 'vitest';
import { LatencyModel } from './latency-model';

const mean = (xs: number[]): number => xs.reduce((a, b) => a + b, 0) / xs.length;

describe('LatencyModel', () => {
  it('is deterministic: same seed => identical sequence', () => {
    const config: Record<string, DistributionConfig> = {
      'storage.read': { distribution: 'normal', mean: 10, stddev: 3 },
    };
    const a = new LatencyModel('seed-1', config);
    const b = new LatencyModel('seed-1', config);
    const seqA = Array.from({ length: 100 }, () => a.sample('storage.read'));
    const seqB = Array.from({ length: 100 }, () => b.sample('storage.read'));
    expect(seqA).toEqual(seqB);
  });

  it('different seeds => different sequences', () => {
    const config: Record<string, DistributionConfig> = {
      'storage.read': { distribution: 'normal', mean: 10, stddev: 3 },
    };
    const a = new LatencyModel('seed-1', config);
    const b = new LatencyModel('seed-2', config);
    expect(a.sample('storage.read')).not.toBe(b.sample('storage.read'));
  });

  it('streams are independent per action type', () => {
    // Adding/consuming one action must not shift another's sequence.
    const config: Record<string, DistributionConfig> = {
      a: { distribution: 'normal', mean: 10, stddev: 3 },
      b: { distribution: 'normal', mean: 10, stddev: 3 },
    };
    const withB = new LatencyModel('s', config);
    const aWithBDraws = [withB.sample('b'), withB.sample('b'), withB.sample('a')].at(-1);

    const onlyA = new LatencyModel('s', config);
    const aAlone = onlyA.sample('a');

    expect(aWithBDraws).toBe(aAlone);
  });

  it('constant distribution returns the exact value', () => {
    const m = new LatencyModel('s', { x: { distribution: 'constant', value: 42 } });
    expect(m.sample('x')).toBe(42);
    expect(m.sample('x')).toBe(42);
  });

  it('unknown action type => 0', () => {
    const m = new LatencyModel('s', {});
    expect(m.sample('never.configured')).toBe(0);
  });

  it('clamps negatives to 0', () => {
    // Mean 0, huge stddev => guaranteed to draw negatives, all clamped.
    const m = new LatencyModel('s', {
      x: { distribution: 'normal', mean: 0, stddev: 100 },
    });
    const samples = Array.from({ length: 500 }, () => m.sample('x'));
    expect(Math.min(...samples)).toBeGreaterThanOrEqual(0);
  });

  it('normal sample mean is within tolerance over many draws', () => {
    const m = new LatencyModel('s', {
      x: { distribution: 'normal', mean: 50, stddev: 5 },
    });
    const samples = Array.from({ length: 20_000 }, () => m.sample('x'));
    expect(mean(samples)).toBeGreaterThan(48);
    expect(mean(samples)).toBeLessThan(52);
  });

  it('uniform stays within [min, max]', () => {
    const m = new LatencyModel('s', {
      x: { distribution: 'uniform', min: 5, max: 15 },
    });
    const samples = Array.from({ length: 1000 }, () => m.sample('x'));
    expect(Math.min(...samples)).toBeGreaterThanOrEqual(5);
    expect(Math.max(...samples)).toBeLessThanOrEqual(15);
  });
});
