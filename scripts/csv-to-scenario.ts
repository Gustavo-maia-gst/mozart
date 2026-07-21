#!/usr/bin/env tsx
/**
 * csv-to-scenario — turn one or more task CSVs into a Mozart scenario YAML.
 *
 * Each CSV is one DAG. Its edges come from a required `<file>.deps.json` sidecar
 * sitting next to it (the real production topology, preserved). Sidecar shape:
 *
 *   { "edges": [[source, target], ...], "taskIds": [id, ...] }
 *
 * where `[source, target]` means "target depends on source", and `taskIds` is
 * every node (roots/leaves included). The CSV supplies per-task `cost_ms`; its
 * ids must match the sidecar's. A CSV with no sidecar is an error.
 *
 * `--seed` sets the scenario's `seed:` field (reproducible latency sampling at
 * run time); it does not affect the edges, which are taken verbatim.
 *
 * CSV shape (header required, columns may be in any order). The id column may be
 * `taskId` or `nodeId`; `cost_ms` is optional:
 *
 *   taskId,cost_ms
 *   172313656,440.6
 *   ...
 *
 * Prefer `--out` over a shell redirect: it writes the file directly (so pnpm's
 * banner never leaks into it) and derives the correct relative `$schema`
 * modeline path from the destination, so editors validate against the real
 * scenario schema (generate it once with `pnpm run schema:gen`).
 *
 * Usage:
 *   tsx scripts/csv-to-scenario.ts graph-a.csv graph-b.csv --out scenarios/x.yaml
 *   pnpm scenario:from-csv g1.csv g2.csv --name big-run --seed 1
 *
 * Flags (all optional):
 *   --out <path>      write here instead of stdout (also fixes the modeline path)
 *   --name <s>        scenario name           (default: derived from files)
 *   --protocol <s>    protocol id             (default: baseline)
 *   --seed <s|n>      scenario seed:          (default: current time)
 *   --timeout <n>     endCondition timeout ms  (default: 60000)
 *   --schema <path>   explicit $schema modeline path (overrides the derived one)
 */

import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { basename, dirname, extname, relative } from 'node:path';

interface Task {
  id: string;
  costMs?: number;
}
interface GraphSpec {
  id: string;
  tasks: { id: string; dependsOn: string[]; costMs?: number }[];
}

/** Sidecar with real edges: `[source, target]` = "target depends on source". */
interface DepsFile {
  edges: [number | string, number | string][];
  taskIds: (number | string)[];
}

/** JSON Schema for scenario YAMLs, emitted by `pnpm run schema:gen`. */
const SCHEMA_FILE = 'scenarios/scenario.schema.json';

interface Options {
  files: string[];
  name?: string;
  protocol: string;
  /** The scenario's `seed:` (reproducible latency sampling); not used for edges. */
  seed: string;
  timeoutMs: number;
  /** Destination file; when set the script writes here (vs stdout). */
  out?: string;
  /** Explicit `$schema` modeline path; overrides the value derived from `out`. */
  schema?: string;
}

/** Each flag's setter. */
const FLAGS: Record<string, (o: Options, v: string) => void> = {
  '--name': (o, v) => (o.name = v),
  '--protocol': (o, v) => (o.protocol = v),
  '--seed': (o, v) => (o.seed = v),
  '--timeout': (o, v) => (o.timeoutMs = Number(v)),
  '--out': (o, v) => (o.out = v),
  '--schema': (o, v) => (o.schema = v),
};

function parseArgs(argv: string[]): Options {
  const files: string[] = [];
  const opts: Options = {
    files,
    protocol: 'baseline',
    seed: String(Date.now()), // pass --seed for reproducible latency sampling
    timeoutMs: 60_000,
  };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    const setter = FLAGS[arg];
    if (setter) {
      const v = argv[++i];
      if (v === undefined) throw new Error(`missing value for ${arg}`);
      setter(opts, v);
    } else {
      if (arg.startsWith('--')) throw new Error(`unknown flag: ${arg}`);
      files.push(arg);
    }
  }
  if (files.length === 0) throw new Error('at least one CSV file is required');
  return opts;
}

/** Split a CSV line, trimming whitespace and stripping surrounding quotes. */
function splitCsvLine(line: string): string[] {
  return line.split(',').map((c) =>
    c
      .trim()
      .replace(/^"(.*)"$/, '$1')
      .trim(),
  );
}

function parseCsv(path: string): Task[] {
  const text = readFileSync(path, 'utf8');
  const lines = text.split(/\r?\n/).filter((l) => l.trim() !== '');
  if (lines.length < 2) throw new Error(`${path}: expected a header and at least one row`);
  const header = splitCsvLine(lines[0]);
  // Id column is `taskId` or `nodeId`; cost_ms is optional.
  const idCol = header.indexOf('taskId') !== -1 ? header.indexOf('taskId') : header.indexOf('nodeId');
  if (idCol === -1) throw new Error(`${path}: missing id column "taskId"/"nodeId" (header: ${header.join(',')})`);
  const cols = { id: idCol, cost: header.indexOf('cost_ms') };

  const tasks: Task[] = [];
  const seen = new Set<string>();
  for (let r = 1; r < lines.length; r++) {
    const task = parseRow(splitCsvLine(lines[r]), cols, path, r + 1);
    if (seen.has(task.id)) throw new Error(`${path}: duplicate task id "${task.id}"`);
    seen.add(task.id);
    tasks.push(task);
  }
  return tasks;
}

function parseRow(cells: string[], cols: { id: number; cost: number }, path: string, row: number): Task {
  const id = cells[cols.id];
  if (!id) throw new Error(`${path}: row ${row} has an empty task id`);
  const rawCost = cols.cost === -1 ? '' : (cells[cols.cost] ?? '');
  const costMs = rawCost === '' ? undefined : Number(rawCost);
  if (costMs !== undefined && (!Number.isFinite(costMs) || costMs <= 0))
    throw new Error(`${path}: row ${row} has an invalid cost_ms "${rawCost}"`);
  return { id, costMs };
}

/** Sanitize a file path into a graph id usable as a YAML/task-id prefix. */
function graphIdFor(path: string, taken: Set<string>): string {
  const stem = basename(path, extname(path)).replace(/[^A-Za-z0-9_-]/g, '_') || 'g';
  let id = stem;
  let n = 1;
  while (taken.has(id)) id = `${stem}_${n++}`;
  taken.add(id);
  return id;
}

/** Path of the real-edges sidecar for a CSV: `foo.csv` → `foo.deps.json`. */
function depsPathFor(csvPath: string): string {
  const ext = extname(csvPath);
  return `${csvPath.slice(0, csvPath.length - ext.length)}.deps.json`;
}

/**
 * Build a DAG from the real edges in the `.deps.json` sidecar. `edges` are
 * `[source, target]` = "target depends on source"; the CSV supplies costs. Ids
 * must match between the two files. Validates the node sets agree and that the
 * result is acyclic.
 */
function depsFromJson(id: string, tasks: Task[], depsPath: string): GraphSpec {
  const parsed = JSON.parse(readFileSync(depsPath, 'utf8')) as DepsFile;
  if (!Array.isArray(parsed.edges) || !Array.isArray(parsed.taskIds))
    throw new Error(`${depsPath}: expected { edges: [[source,target],...], taskIds: [...] }`);

  const csvIds = new Set(tasks.map((t) => t.id));
  const jsonIds = new Set(parsed.taskIds.map(String));
  // The two files must describe the same node set — a mismatch means the CSV and
  // the sidecar were produced from different extractions (ids out of sync).
  const onlyCsv = [...csvIds].filter((x) => !jsonIds.has(x));
  const onlyJson = [...jsonIds].filter((x) => !csvIds.has(x));
  if (onlyCsv.length > 0 || onlyJson.length > 0)
    throw new Error(
      `${depsPath}: task ids do not match the CSV — ` +
        `${onlyCsv.length} only in CSV (e.g. ${onlyCsv.slice(0, 3).join(', ') || '—'}), ` +
        `${onlyJson.length} only in json (e.g. ${onlyJson.slice(0, 3).join(', ') || '—'})`,
    );

  const depsOf = new Map<string, string[]>(tasks.map((t) => [t.id, []]));
  for (const [source, target] of parsed.edges) {
    const s = String(source);
    const t = String(target);
    if (!csvIds.has(s) || !csvIds.has(t))
      throw new Error(`${depsPath}: edge [${s}, ${t}] references an unknown task id`);
    (depsOf.get(t) as string[]).push(s);
  }

  const out: GraphSpec['tasks'] = tasks.map((task) => ({
    id: task.id,
    dependsOn: [...new Set(depsOf.get(task.id))].sort(),
    ...(task.costMs !== undefined ? { costMs: task.costMs } : {}),
  }));
  assertAcyclic(out, depsPath);
  return { id, tasks: out };
}

/** Kahn's algorithm: throws with a sample of the offending ids if a cycle exists. */
function assertAcyclic(tasks: GraphSpec['tasks'], source: string): void {
  const indegree = new Map<string, number>(tasks.map((t) => [t.id, t.dependsOn.length]));
  const queue = tasks.filter((t) => t.dependsOn.length === 0).map((t) => t.id);
  const dependents = new Map<string, string[]>(tasks.map((t) => [t.id, []]));
  for (const t of tasks) for (const dep of t.dependsOn) dependents.get(dep)?.push(t.id);

  let settled = 0;
  for (let i = 0; i < queue.length; i++) {
    settled++;
    for (const dependent of dependents.get(queue[i]) ?? []) {
      const left = (indegree.get(dependent) ?? 0) - 1;
      indegree.set(dependent, left);
      if (left === 0) queue.push(dependent);
    }
  }
  if (settled !== tasks.length) {
    const stuck = tasks.filter((t) => (indegree.get(t.id) ?? 0) > 0).map((t) => t.id);
    throw new Error(
      `${source}: edges contain a cycle (${tasks.length - settled} tasks, e.g. ${stuck.slice(0, 3).join(', ')})`,
    );
  }
}

// --- YAML emission (hand-rolled — the values are simple ids/numbers/arrays) ---

// Task ids are always quoted: purely-numeric ids (e.g. "172313656") would
// otherwise be parsed back as YAML numbers and fail the scenario schema (ids
// must be strings); quoting also guards ids that look like true/null/etc.
const q = (id: string): string => `"${id.replace(/"/g, '\\"')}"`;
const inlineDeps = (deps: string[]): string => `[${deps.map(q).join(', ')}]`;

function emitGraph(g: GraphSpec): string {
  const lines = [`  - id: ${q(g.id)}`, `    tasks:`];
  for (const t of g.tasks) {
    const parts = [`id: ${q(t.id)}`];
    if (t.dependsOn.length > 0) parts.push(`dependsOn: ${inlineDeps(t.dependsOn)}`);
    if (t.costMs !== undefined) parts.push(`costMs: ${t.costMs}`);
    lines.push(`      - { ${parts.join(', ')} }`);
  }
  return lines.join('\n');
}

/**
 * The `$schema` modeline editors use to validate the YAML. Explicit `--schema`
 * wins; otherwise it's derived from `--out` (relative to the destination dir).
 * Returns undefined for stdout with no `--schema` — a made-up relative path
 * would point nowhere, so we emit none and hint on stderr instead.
 */
function schemaModeline(opts: Options): string | undefined {
  const path = opts.schema ?? (opts.out ? relative(dirname(opts.out), SCHEMA_FILE) || SCHEMA_FILE : undefined);
  return path ? `# yaml-language-server: $schema=${path}` : undefined;
}

function emitScenario(graphs: GraphSpec[], opts: Options): string {
  const totalTasks = graphs.reduce((n, g) => n + g.tasks.length, 0);
  const modeline = schemaModeline(opts);
  return [
    ...(modeline ? [modeline] : []),
    `# Generated by scripts/csv-to-scenario.ts from ${opts.files.length} CSV(s): ${opts.files.join(', ')}`,
    `# ${graphs.length} graph(s), ${totalTasks} task(s). Edges: real, from .deps.json sidecar(s).`,
    `name: ${opts.name ?? graphs.map((g) => g.id).join('+')}`,
    `seed: ${opts.seed}`,
    `protocol: ${opts.protocol}`,
    `nodes: [{ id: n1, name: coordinator }]`,
    `storage: { adapter: postgres }`,
    `transport: { ackTimeoutMs: 2000 }`,
    `latency:`,
    // Communication latency: right-skewed (longer upper tail than lower), the
    // shape real network/queue latency has. Lognormal with mu/sigma in log-space
    // chosen for mean ≈ 50ms, stddev ≈ 20ms (median ≈ 46ms).
    `  transport.deliver: { distribution: lognormal, mu: 3.84, sigma: 0.385 }`,
    `  storage.read: { distribution: constant, value: 5 }`,
    `  storage.save: { distribution: constant, value: 5 }`,
    `endCondition: { type: timeout, ms: ${opts.timeoutMs} }`,
    ``,
    // Graphs last — they can be huge, so the tunable knobs above stay reachable
    // without scrolling past hundreds of tasks.
    `graphs:`,
    graphs.map(emitGraph).join('\n\n'),
    ``,
  ].join('\n');
}

function main(): void {
  const opts = parseArgs(process.argv.slice(2));
  const takenIds = new Set<string>();
  const graphs = opts.files.map((file) => {
    const depsPath = depsPathFor(file);
    if (!existsSync(depsPath)) throw new Error(`${file}: missing required edges sidecar ${depsPath}`);
    return depsFromJson(graphIdFor(file, takenIds), parseCsv(file), depsPath);
  });
  const yaml = emitScenario(graphs, opts);
  if (opts.out) {
    writeFileSync(opts.out, yaml);
    process.stderr.write(`csv-to-scenario: wrote ${opts.out}\n`);
  } else {
    process.stdout.write(yaml);
    if (!opts.schema)
      process.stderr.write('csv-to-scenario: no $schema modeline (pass --out or --schema for editor validation)\n');
  }
}

try {
  main();
} catch (err) {
  process.stderr.write(`csv-to-scenario: ${(err as Error).message}\n`);
  process.exit(1);
}
