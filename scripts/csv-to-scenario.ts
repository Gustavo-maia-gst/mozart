#!/usr/bin/env tsx
/**
 * csv-to-scenario — turn one or more task CSVs into a Mozart scenario YAML.
 *
 * Each CSV is one DAG. The CSV carries no edges, only a topological `order`
 * column, so we synthesize a valid DAG: a task at order N depends on a random
 * subset of the tasks at strictly-lower orders (fan-in of a few units up to
 * dozens, clamped to what's available). Lower→higher edges are always acyclic,
 * so the result is guaranteed to be a DAG. Order-0 tasks are the roots.
 *
 * Output is non-deterministic by default (the seed is the wall clock). Pass
 * `--seed` to reproduce a run: the same inputs + seed produce the same YAML,
 * and that seed also lands in the scenario's `seed:` field.
 *
 * CSV shape (header required, columns may be in any order):
 *
 *   nodeId,order,cost_ms
 *   a,0,50
 *   b,1,30
 *   ...
 *
 * Prefer `--out` over a shell redirect: it writes the file directly (so pnpm's
 * banner never leaks into it) and derives the correct relative `$schema`
 * modeline path from the destination, so editors validate against the real
 * scenario schema (generate it once with `pnpm run schema:gen`).
 *
 * Usage:
 *   tsx scripts/csv-to-scenario.ts graph-a.csv graph-b.csv --out scenarios/x.yaml
 *   pnpm scenario:from-csv g1.csv g2.csv --name big-run --min-fanin 4 --max-fanin 40
 *
 * Flags (all optional):
 *   --out <path>      write here instead of stdout (also fixes the modeline path)
 *   --name <s>        scenario name           (default: derived from files)
 *   --protocol <s>    protocol id             (default: baseline)
 *   --seed <s|n>      seed for edges + YAML seed:  (default: current time)
 *   --min-fanin <n>   min deps per non-root    (default: 1)
 *   --max-fanin <n>   max deps per non-root    (default: 30)
 *   --timeout <n>     endCondition timeout ms  (default: 60000)
 *   --schema <path>   explicit $schema modeline path (overrides the derived one)
 */

import { basename, dirname, extname, relative } from 'node:path';
import { readFileSync, writeFileSync } from 'node:fs';

interface Task {
  id: string;
  order: number;
  costMs?: number;
}
interface GraphSpec {
  id: string;
  tasks: { id: string; dependsOn: string[]; costMs?: number }[];
}

/** JSON Schema for scenario YAMLs, emitted by `pnpm run schema:gen`. */
const SCHEMA_FILE = 'scenarios/scenario.schema.json';

interface Options {
  files: string[];
  name?: string;
  protocol: string;
  /** Drives both the YAML `seed:` and edge synthesis; defaults to the wall clock. */
  seed: string;
  minFanIn: number;
  maxFanIn: number;
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
  '--min-fanin': (o, v) => (o.minFanIn = Number(v)),
  '--max-fanin': (o, v) => (o.maxFanIn = Number(v)),
  '--timeout': (o, v) => (o.timeoutMs = Number(v)),
  '--out': (o, v) => (o.out = v),
  '--schema': (o, v) => (o.schema = v),
};

function parseArgs(argv: string[]): Options {
  const files: string[] = [];
  const opts: Options = {
    files,
    protocol: 'baseline',
    seed: String(Date.now()), // non-deterministic by default; pass --seed for reproducible output
    minFanIn: 1,
    maxFanIn: 30,
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
  if (opts.minFanIn < 1) throw new Error('--min-fanin must be >= 1');
  if (opts.maxFanIn < opts.minFanIn) throw new Error('--max-fanin must be >= --min-fanin');
  return opts;
}

/** Hash an arbitrary seed string to a uint32 for the PRNG (FNV-1a). */
function hashSeed(s: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

/** Deterministic PRNG (mulberry32) — reproducible edge synthesis. */
function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const randInt = (rng: () => number, lo: number, hi: number): number => lo + Math.floor(rng() * (hi - lo + 1));

/** Fisher–Yates shuffle in place using the seeded rng. */
function shuffle<T>(arr: T[], rng: () => number): T[] {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(rng() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
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
  const col = (name: string): number => {
    const idx = header.indexOf(name);
    if (idx === -1) throw new Error(`${path}: missing required column "${name}" (header: ${header.join(',')})`);
    return idx;
  };
  const cols = { id: col('nodeId'), order: col('order'), cost: header.indexOf('cost_ms') /* optional */ };

  const tasks: Task[] = [];
  const seen = new Set<string>();
  for (let r = 1; r < lines.length; r++) {
    const task = parseRow(splitCsvLine(lines[r]), cols, path, r + 1);
    if (seen.has(task.id)) throw new Error(`${path}: duplicate nodeId "${task.id}"`);
    seen.add(task.id);
    tasks.push(task);
  }
  return tasks;
}

function parseRow(cells: string[], cols: { id: number; order: number; cost: number }, path: string, row: number): Task {
  const id = cells[cols.id];
  if (!id) throw new Error(`${path}: row ${row} has an empty nodeId`);
  const order = Number(cells[cols.order]);
  if (!Number.isFinite(order)) throw new Error(`${path}: row ${row} has a non-numeric order "${cells[cols.order]}"`);
  const rawCost = cols.cost === -1 ? '' : (cells[cols.cost] ?? '');
  const costMs = rawCost === '' ? undefined : Number(rawCost);
  if (costMs !== undefined && (!Number.isFinite(costMs) || costMs <= 0))
    throw new Error(`${path}: row ${row} has an invalid cost_ms "${rawCost}"`);
  return { id, order, costMs };
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

/**
 * Synthesize a DAG from ordered tasks: each non-root task depends on a random
 * subset (size in [minFanIn, maxFanIn], clamped to pool) of the tasks at
 * strictly-lower orders.
 */
function synthesize(id: string, tasks: Task[], opts: Options, rng: () => number): GraphSpec {
  const sorted = [...tasks].sort((a, b) => a.order - b.order);
  const earlier: string[] = []; // ids of all tasks at a strictly-lower order than the current one
  let pendingOrder: number | null = null;
  let pendingBuffer: string[] = []; // tasks at the current order, held back until the order advances

  const out: GraphSpec['tasks'] = [];
  const flushPending = (): void => {
    earlier.push(...pendingBuffer);
    pendingBuffer = [];
  };

  for (const task of sorted) {
    if (pendingOrder === null) pendingOrder = task.order;
    if (task.order !== pendingOrder) {
      flushPending(); // same-order tasks never depend on each other
      pendingOrder = task.order;
    }
    let dependsOn: string[] = [];
    if (earlier.length > 0) {
      const want = randInt(rng, opts.minFanIn, opts.maxFanIn);
      const k = Math.min(want, earlier.length);
      dependsOn = shuffle([...earlier], rng)
        .slice(0, k)
        .sort();
    }
    out.push({ id: task.id, dependsOn, ...(task.costMs !== undefined ? { costMs: task.costMs } : {}) });
    pendingBuffer.push(task.id);
  }
  return { id, tasks: out };
}

// --- YAML emission (hand-rolled — the values are simple ids/numbers/arrays) ---

const inlineDeps = (deps: string[]): string => `[${deps.join(', ')}]`;

function emitGraph(g: GraphSpec): string {
  const lines = [`  - id: ${g.id}`, `    tasks:`];
  for (const t of g.tasks) {
    const parts = [`id: ${t.id}`];
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
    `# ${graphs.length} graph(s), ${totalTasks} task(s). Edges synthesized (seed ${opts.seed}, ` +
      `fan-in ${opts.minFanIn}..${opts.maxFanIn}). Pass --seed to reproduce.`,
    `name: ${opts.name ?? graphs.map((g) => g.id).join('+')}`,
    `seed: ${opts.seed}`,
    `protocol: ${opts.protocol}`,
    `nodes: [{ id: n1, name: coordinator }]`,
    `storage: { adapter: in-memory }`,
    `transport: { ackTimeoutMs: 2000 }`,
    `latency:`,
    `  transport.deliver: { distribution: normal, mean: 15, stddev: 5 }`,
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
  const rng = mulberry32(hashSeed(opts.seed));
  const takenIds = new Set<string>();
  const graphs = opts.files.map((file) => {
    const id = graphIdFor(file, takenIds);
    return synthesize(id, parseCsv(file), opts, rng);
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
