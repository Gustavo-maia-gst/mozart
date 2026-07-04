import type { DistributionConfig } from '@mozart/contracts';
import { randomLogNormal, randomNormal, randomUniform } from 'd3-random';
import seedrandom from 'seedrandom';

type Sampler = () => number;

/**
 * Samples per-action latencies (ms) from configured distributions.
 *
 * Reproducibility: each action type gets its own RNG stream, seeded from
 * `${seed}:${actionType}`. Adding a new action type therefore never perturbs
 * the draw sequence of existing ones — runs stay comparable across scenarios
 * that differ only by an added action.
 *
 * Unknown action types default to constant 0 (no injected latency).
 */
export class LatencyModel {
  private readonly samplers = new Map<string, Sampler>();

  constructor(
    private readonly seed: string,
    private readonly config: Record<string, DistributionConfig>,
  ) {}

  /** Returns a non-negative latency in ms for the given action type. */
  sample(actionType: string): number {
    return this.samplerFor(actionType)();
  }

  private samplerFor(actionType: string): Sampler {
    let sampler = this.samplers.get(actionType);
    if (!sampler) {
      sampler = this.buildSampler(actionType);
      this.samplers.set(actionType, sampler);
    }
    return sampler;
  }

  private buildSampler(actionType: string): Sampler {
    const dist = this.config[actionType];
    if (!dist) {
      return () => 0;
    }
    // Independent, deterministic stream per action type.
    const rng = seedrandom(`${this.seed}:${actionType}`);
    const nonNegative = (raw: () => number): Sampler => {
      return () => Math.max(0, raw());
    };

    switch (dist.distribution) {
      case 'constant':
        return () => dist.value;
      case 'normal':
        return nonNegative(randomNormal.source(rng)(dist.mean, dist.stddev));
      case 'lognormal':
        // d3.randomLogNormal takes the mu/sigma of the underlying normal.
        return nonNegative(randomLogNormal.source(rng)(dist.mu, dist.sigma));
      case 'uniform':
        return nonNegative(randomUniform.source(rng)(dist.min, dist.max));
    }
  }
}
