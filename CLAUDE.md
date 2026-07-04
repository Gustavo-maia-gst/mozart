# Mozart — Harness para Protocolos de Coordenação de DAGs Distribuídos

Harness de execução para pesquisa em SD: coordenar execução tolerante a falhas de DAGs
concorrentes. Protocolos (baseline centralizado, pull-based, push-based/dataflow) rodam
como processos slave stateless contra um kernel (master) que simula o mundo externo.

## Comandos

```bash
# grafana (UI :2000, dashboards provisionados de grafana/) + prometheus (:2090,
# OTLP metrics receiver) + jaeger (UI :2016, OTLP http :2018) + postgres (:5432)
docker compose up -d
pnpm install
pnpm build                  # tsc -b (project references)
pnpm test                   # vitest, unit
pnpm test:integration      # MOZART_INTEGRATION=1 — inclui testes que precisam do postgres
pnpm lint
pnpm demo                   # roda scenarios/baseline.yaml
```

## Arquitetura

- **`apps/master`** — kernel do harness (NestJS). Dono de TODO o estado: transport
  (filas Pub/Sub-like: at-least-once, FIFO por canal, ack + redelivery), Worker Pool W
  simulado, Storage S (port + adapters in-memory/postgres), injetor de falhas,
  process manager (fork/SIGKILL/restart de slaves), event log JSONL (`runs/<id>/`).
  O master é _silencioso_: não participa do protocolo.
- **`apps/slave`** — runner genérico de protocolo (NestJS). Stateless. Todo efeito
  colateral (transport/W/S) atravessa IPC até o master.
- **`packages/contracts`** — ports (TransportPort/StoragePort/WorkerPoolPort), tokens de DI
  (STORAGE_PORT/…/PROTOCOL_LOGGER/PROTOCOL), Graph, IpcFrame, eventos, schema zod de cenário.
- **`packages/ipc`** — RPC tipado fino sobre o canal IPC nativo do `child_process.fork`.
- **`packages/telemetry`** — bootstrap OTel + inject/extract de trace-context em envelopes.
- **`packages/latency`** — RNG seedado, um stream por tipo de ação, distribuições d3-random.
- **`packages/protocols`** — classe abstrata `Protocol` (portas injetadas por propriedade via
  Nest) + implementações. `baseline` é o protocolo centralizado de referência.

Direção de dependência: `apps → packages`; `packages → contracts` apenas.

## Invariantes (não viole)

1. **Todo efeito passa pelo master.** Slave nunca toca rede/disco/banco diretamente.
2. **Slaves são stateless.** Estado de protocolo vive no Storage S. Crash = SIGKILL,
   restart do zero. Nunca adicione estado local significativo ao slave.
3. **Handlers de protocolo devem ser idempotentes.** Entrega é at-least-once;
   `onMessage` resolver ⇒ ack; rejeitar/morrer ⇒ redelivery.
4. **Trace-context sempre explícito no envelope** (`traceCtx`). Nunca confie em
   `context.active()` sobreviver a residência em fila/timer.
5. **Canais/redeliveries são chaveados por `nodeId`**, nunca por handle de processo
   (restart re-anexa).
6. **O event log JSONL é a fonte de verdade** pra corretude (carimba trace/span
   id via `activeIds`). O harness é _quase silencioso em traces_: emite `run`
   (span-raiz único da run — a ativação roda sob ele, então TODOS os grafos e
   passos descem daí e viram UM trace só, não um por grafo), `transport.redeliver`
   (métrica de redelivery) e `worker.execute` (um span por task, aberto no
   `worker.start` e fechado no complete/fail — cobre a duração simulada). O resto
   da árvore vive nos coordinators (um service OTel por coordinator, nomeado no
   YAML). Spans de slave são best-effort (SIGKILL pode perder a janela do
   BatchSpanProcessor).
7. Payloads IPC/mensagens são JSON puro (tipo `Json` em contracts): sem Date/Map/
   BigInt/undefined/NaN. Zod valida nas duas bordas.

## Convenções

- TypeScript estrito, sem `any` não-justificado. CJS (sem `"type": "module"`);
  deps ESM-only (d3-random) funcionam via require(esm) do Node ≥22.12.
- Testes: vitest, specs ao lado do código (`*.spec.ts`); integração PG atrás de
  `MOZART_INTEGRATION=1`; fake timers via tokens `Clock`/`Scheduler` injetados.
- Cenários YAML em `scenarios/` = _o que rodar_ (nós, DAG, latências, faults, seed);
  env via `@nestjs/config` = _onde as coisas estão_ (OTLP, PG URL, entrypoint slave).
- Build é `tsc -b` (project references). Se você apagar `dist/` manualmente, o cache
  incremental fica stale — rode `pnpm clean && pnpm build` (ou `tsc -b --force`).
- Testes de fork/e2e rodam contra `dist/` (não tsx), então **buildar antes**; eles
  se auto-skipam via `describe.runIf(distReady)` se o dist não existir.
- OTLP: string vazia em `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` cai no default
  (`localhost:4318`); sem Jaeger o export falha em silêncio (não quebra o run).
