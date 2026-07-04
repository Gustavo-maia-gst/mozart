import { readFileSync } from 'node:fs';
import {
  graphsFromScenario,
  type Scenario,
  type ScenarioInfo,
  scenarioSchema,
} from '@mozart/contracts';
import { parse } from 'yaml';

/** Loads and validates a scenario YAML file. Throws with a readable zod error. */
export function loadScenario(path: string): Scenario {
  const raw: unknown = parse(readFileSync(path, 'utf8'));
  return scenarioSchema.parse(raw);
}

/** Coordinator node ids (the scenario's `nodes`, excluding the implicit W). */
export function coordinatorIds(scenario: Scenario): string[] {
  return scenario.nodes.map((n) => n.id);
}

/** Builds the per-node scenario slice handed to a slave at handshake. */
export function scenarioInfoFor(scenario: Scenario, runId: string, nodeId: string): ScenarioInfo {
  return {
    runId,
    nodeId,
    protocol: scenario.protocol,
    nodes: coordinatorIds(scenario),
    dag: scenario.dag,
    graphs: graphsFromScenario(scenario),
  };
}
