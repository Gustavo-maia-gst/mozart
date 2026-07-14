import type { Type } from '@nestjs/common';
import { BaselineProtocol } from './implementations/baseline/baseline';
import { RecoverableBaselineProtocol } from './implementations/baseline/recoverable_baseline';
import { DependencyFrontierProtocol } from './implementations/pull_based/dependency_frontier';
import { MonotonicDependencyFrontierProtocol } from './implementations/pull_based/monotonic_dependency_frontier';
import { TopologicalBarrierProtocol } from './implementations/pull_based/topological_barrier';
import type { Protocol } from './protocol';

/** Name -> protocol class. Slaves resolve their protocol by MOZART_PROTOCOL. */
export const PROTOCOLS: Record<string, Type<Protocol>> = {
  baseline: BaselineProtocol,
  'baseline-recoverable': RecoverableBaselineProtocol,
  'topological-barrier': TopologicalBarrierProtocol,
  'dependency-frontier': DependencyFrontierProtocol,
  'monotonic-dependency-frontier': MonotonicDependencyFrontierProtocol,
};

export function resolveProtocol(name: string): Type<Protocol> {
  const protocol = PROTOCOLS[name];
  if (!protocol) {
    throw new Error(`unknown protocol "${name}"; known: ${Object.keys(PROTOCOLS).join(', ')}`);
  }
  return protocol;
}
