#!/usr/bin/env tsx
/**
 * gen-scenario-schema — emit a JSON Schema for scenario YAMLs from the zod
 * `scenarioSchema` (the single source of truth). Editors (redhat.vscode-yaml)
 * pick it up via the `# yaml-language-server: $schema=...` modeline that
 * csv-to-scenario writes, giving real validation + autocomplete instead of the
 * wrong schema-store guess (CrowdSec, k8s, …).
 *
 * Run whenever `scenarioSchema` changes:
 *   pnpm run schema:gen
 *
 * Uses `io: 'input'` so the schema describes the YAML as authored (pre-zod
 * transforms/defaults), which is what the editor is validating.
 */

import { writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { scenarioSchema } from '@mozart/contracts';
import { z } from 'zod';

const OUT = join('scenarios', 'scenario.schema.json');

const schema = {
  title: 'Mozart Scenario',
  ...z.toJSONSchema(scenarioSchema, { io: 'input' }),
};

writeFileSync(OUT, `${JSON.stringify(schema, null, 2)}\n`);
process.stderr.write(`gen-scenario-schema: wrote ${OUT}\n`);
