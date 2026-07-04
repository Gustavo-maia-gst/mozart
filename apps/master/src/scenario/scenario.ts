import { readFileSync } from 'node:fs';
import { Scenario, scenarioSchema } from '@mozart/contracts';
import { parse } from 'yaml';

/**
 * Loads and validates a scenario YAML file into a {@link Scenario}. When
 * `protocol` is given it overrides the document's `protocol` field — letting
 * one scenario be replayed against different protocols from the CLI.
 */
export function loadScenario(path: string, protocol?: string): Scenario {
  const raw = parse(readFileSync(path, 'utf8')) as Record<string, unknown>;
  const merged = protocol ? { ...raw, protocol } : raw;
  return new Scenario(scenarioSchema.parse(merged));
}
