# Mozart

A test harness for research on **fault-tolerant coordination of concurrent task-DAGs**
in distributed systems. Coordination protocols (a centralized baseline, pull-based, and
push-based/dataflow families) are implemented against a fixed interaction and failure
model; the harness isolates them from the outside world, injects faults, and instruments
100% of interactions so their correctness and coordination cost can be measured and compared.

This repository is **milestone 1: the harness itself** — the "world" the protocols run in.
A centralized `baseline` protocol ships as the reference; distributed protocols come next.

## Model (fixed for all protocols)

- **Worker Pool `W`** — async `start(taskId)`; completion/failure arrives later as a
  transport event. Simulated, with configurable task durations.
- **Shared Storage `S`** — sync `read` / `readExclusive` (mutual exclusion) / `save`.
  Crash-recovery: outages block callers, then recover with state intact. Two adapters:
  in-memory and Postgres (`SELECT … FOR UPDATE` + transaction-scoped advisory lock).
- **Transport** — persistent Pub/Sub-style fabric: at-least-once, FIFO per `(from,to)`
  channel, explicit acks with visibility-timeout redelivery. Produces duplicates on demand.
- **Failures** — coordinators are crash-stop (SIGKILL, restart with no local state);
  message duplication; storage outages; network partitions. No byzantine faults.

## Architecture

A silent **master** process is the harness kernel and owns all state (transport queues,
`W`, `S`, faults, event log). Stateless **slave** processes run the protocol-under-test and
reach the world only through the master via `child_process` IPC. Crash = SIGKILL of a slave.

```
master (kernel)  ── forks/SIGKILLs ──▶  slave n1 ─┐
  transport · W · S · faults · log                ├─ protocol (stateless) via ports
  ◀── RPC (storage/worker/publish/ack) ───────────┘
  ─── push (deliveries, activate) ────────────────▶
```

Packages: `contracts` (ports/SPI/schemas), `ipc` (typed RPC over Node IPC), `telemetry`
(OpenTelemetry + envelope propagation), `latency` (seeded distributions), `protocols`.
See [CLAUDE.md](./CLAUDE.md) for the module map and invariants.

## Quick start

```bash
docker compose up -d          # Jaeger (UI :16686) + Postgres (:5432)
pnpm install && pnpm build
pnpm test                     # unit + component tests
pnpm test:integration         # + Postgres storage tests (needs the DB)
pnpm demo                     # runs scenarios/baseline.yaml
```

`pnpm demo` boots the `baseline` coordinator over a diamond DAG (`a -> {b,c} -> d`), which
it loads into memory and drives to completion. Watch it in the event log
(`runs/<runId>/events.jsonl`) and as one cross-process trace in Jaeger
(`http://localhost:16686`, service `mozart-master`).

## Scenarios

A scenario file (`scenarios/*.yaml`) declares _what to run_: nodes, the DAG, per-action
latency distributions (mean/variance), a fault schedule, an RNG seed, and the end
condition. Environment config declares _where things are_ (OTLP endpoint, Postgres URL,
slave entrypoint). See `scenarios/baseline.yaml`.

## Writing a protocol

Extend the abstract `Protocol` class and register it in
`packages/protocols/src/registry.ts`. Nest injects the ports as properties
(`this.storage` / `this.transport` / `this.workers` / `this.log`), so a protocol only
implements three operations:

- `persistGraph(graph)` — how the graph is stored in S before it runs;
- `startGraph(graphId)` — begin executing an already-persisted graph;
- `onMessage(message)` — handle inbound messages (worker events + coordination).

**`onMessage` resolving is the ack** — a rejection (or a crash) triggers redelivery, so
handlers must be idempotent. See `packages/protocols/src/baseline.ts`.
