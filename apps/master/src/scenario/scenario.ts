import { readFileSync } from 'node:fs';
import { Scenario, scenarioSchema } from '@mozart/contracts';
import { parse } from 'yaml';

/** CLI overrides applied on top of the scenario document before validation. */
export interface ScenarioOverrides {
  /** Replaces the document's `protocol` — replay one scenario across protocols. */
  protocol?: string;
  /** Replaces `nodes` with a plain count (expands to n1/nodeA…) — scaling sweeps. */
  nodes?: number;
}

/**
 * Loads and validates a scenario YAML file into a {@link Scenario}. Any
 * {@link ScenarioOverrides} are merged onto the raw document before validation,
 * letting the CLI replay one scenario against different protocols or node counts.
 */
export function loadScenario(path: string, overrides: ScenarioOverrides = {}): Scenario {
  const raw = parse(readFileSync(path, 'utf8')) as Record<string, unknown>;
  const merged = { ...raw };
  if (overrides.protocol !== undefined) merged.protocol = overrides.protocol;
  if (overrides.nodes !== undefined) merged.nodes = overrides.nodes;
  return new Scenario(scenarioSchema.parse(merged));
}
