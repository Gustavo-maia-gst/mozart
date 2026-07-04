import type { DistributionConfig, Scenario } from '@mozart/contracts';

/**
 * Flattens a scenario's latency config into scalar resource attributes so
 * Prometheus can promote them to labels — this is what makes "under comms mean Y
 * and storage mean Z, compare protocols" a simple dashboard filter. Dots in
 * action names become underscores, e.g. `transport.deliver` →
 * `mozart.latency.transport_deliver.mean` (+ `.stddev` when the distribution
 * has one). All values are strings (OTel/Prometheus label values).
 */
export function latencyResourceAttrs(latency: Scenario['latency']): Record<string, string> {
  const attrs: Record<string, string> = {};
  for (const [action, dist] of Object.entries(latency)) {
    const key = `mozart.latency.${action.replaceAll('.', '_')}`;
    const { mean, stddev } = summarize(dist);
    attrs[`${key}.mean`] = String(mean);
    if (stddev !== undefined) attrs[`${key}.stddev`] = String(stddev);
  }
  return attrs;
}

/** The distribution's summary stat used as its representative "mean" label. */
function summarize(dist: DistributionConfig): { mean: number; stddev?: number } {
  switch (dist.distribution) {
    case 'constant':
      return { mean: dist.value, stddev: 0 };
    case 'normal':
      return { mean: dist.mean, stddev: dist.stddev };
    case 'lognormal':
      // Mean of the underlying (log-space) normal — the configured location.
      return { mean: dist.mu, stddev: dist.sigma };
    case 'uniform':
      return { mean: (dist.min + dist.max) / 2 };
  }
}
