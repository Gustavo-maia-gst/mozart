import { describe, expect, it } from 'vitest';
import { buildGraph, criticalPathCost } from './graph';

describe('criticalPathCost', () => {
  it('sums costMs along the most expensive path, not the longest hop count', () => {
    // a -> b -> d  (1 + 2 + 4 = 7)
    // a -> c -> d  (1 + 10 + 4 = 15)  <- critical
    const g = buildGraph('g', [
      { id: 'a', costMs: 1 },
      { id: 'b', dependsOn: ['a'], costMs: 2 },
      { id: 'c', dependsOn: ['a'], costMs: 10 },
      { id: 'd', dependsOn: ['b', 'c'], costMs: 4 },
    ]);
    expect(criticalPathCost(g)).toBe(15);
  });

  it('treats absent costMs as 0', () => {
    const g = buildGraph('g', [{ id: 'a' }, { id: 'b', dependsOn: ['a'], costMs: 5 }]);
    expect(criticalPathCost(g)).toBe(5);
  });

  it('is the max across disconnected components', () => {
    const g = buildGraph('g', [
      { id: 'x', costMs: 3 },
      { id: 'y', costMs: 8 },
    ]);
    expect(criticalPathCost(g)).toBe(8);
  });
});
