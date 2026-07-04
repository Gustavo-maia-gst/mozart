import { type ChildProcess, fork } from 'node:child_process';
import { join } from 'node:path';
import { CONTROL_TOPICS, type GraphId, type NodeId, type Scenario } from '@mozart/contracts';
import { childFrameChannel, NodeLink, type RpcHandlers } from '@mozart/ipc';
import { traceContextHooks } from '@mozart/telemetry';
import { Inject, Injectable, Logger } from '@nestjs/common';
import type { EnvConfig } from '../config/env';
import { EventLogService } from '../event-log/event-log.service';
import { MetricsService } from '../metrics/metrics.service';
import { StorageService } from '../storage/storage.service';
import { ENV_CONFIG, RUN_ID, SCENARIO } from '../tokens';
import { TransportService } from '../transport/transport.service';
import { IpcHostService } from './ipc-host.service';
import { NodeRegistry } from './node-registry';

interface Slave {
  child: ChildProcess;
  injectedKill: boolean;
}

/**
 * Forks, kills (SIGKILL) and restarts the stateless slave processes, and wires
 * each to a NodeLink. Owns crash handling: on exit it releases the node's
 * storage locks and unregisters its link (redelivery then retries to the
 * rebound link after a restart).
 */
@Injectable()
export class ProcessManagerService {
  private readonly logger = new Logger(ProcessManagerService.name);
  private readonly slaves = new Map<NodeId, Slave>();
  private readonly handlers: RpcHandlers;
  private readonly ready = new Set<NodeId>();
  private readyCheck?: () => void;

  // biome-ignore lint/complexity/useMaxParams: deps injection
  constructor(
    @Inject(SCENARIO) private readonly scenario: Scenario,
    @Inject(RUN_ID) private readonly runId: string,
    @Inject(ENV_CONFIG) private readonly env: EnvConfig,
    private readonly ipcHost: IpcHostService,
    private readonly registry: NodeRegistry,
    private readonly storage: StorageService,
    private readonly events: EventLogService,
    private readonly metrics: MetricsService,
    private readonly transport: TransportService,
  ) {
    this.handlers = this.ipcHost.buildHandlers();
    this.ipcHost.onNodeReady = (nodeId) => this.markReady(nodeId);
  }

  public spawnAll(): void {
    for (const nodeId of this.scenario.coordinatorIds()) this.spawn(nodeId);
  }

  public spawn(nodeId: NodeId): void {
    const entrypoint = this.env.MOZART_SLAVE_ENTRYPOINT ?? join(process.cwd(), 'apps/slave/dist/main.js');
    const name = this.scenario.nodeName(nodeId);
    const child = fork(entrypoint, [], {
      serialization: 'json',
      execArgv: [],
      env: {
        ...process.env,
        MOZART_NODE_ID: nodeId,
        MOZART_NODE_NAME: name,
        MOZART_PROTOCOL: this.scenario.protocol,
        MOZART_RUN_ID: this.runId,
        OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: this.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
        OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: this.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT,
        ...(this.env.MOZART_OTEL_PROCESSOR ? { MOZART_OTEL_PROCESSOR: this.env.MOZART_OTEL_PROCESSOR } : {}),
      },
    });

    const link = new NodeLink(nodeId, childFrameChannel(child), this.handlers, traceContextHooks());
    this.registry.register(link);
    this.slaves.set(nodeId, { child, injectedKill: false });
    link.onClose(() => void this.onExit(nodeId));
    this.events.record({ type: 'node.spawned', nodeId });
    this.metrics.countNodeLifecycle('spawned');
  }

  /** SIGKILL a node. `injected` distinguishes fault kills from shutdown kills. */
  public kill(nodeId: NodeId, injected = true): void {
    const slave = this.slaves.get(nodeId);
    if (!slave) return;
    slave.injectedKill = injected;
    if (injected) {
      this.events.record({ type: 'node.killed', nodeId });
      this.metrics.countNodeLifecycle('killed');
    }
    slave.child.kill('SIGKILL');
  }

  public restart(nodeId: NodeId): void {
    this.events.record({ type: 'node.restarted', nodeId });
    this.metrics.countNodeLifecycle('restarted');
    this.spawn(nodeId); // fresh, stateless; link rebound under same nodeId
  }

  /**
   * Start one (already-persisted) graph by sending `graph.start` to the
   * coordinators, under the graph's lifetime span so the message (and everything
   * it triggers) nests beneath it.
   */
  public startGraph(graphId: GraphId): void {
    this.transport.beginGraph(graphId, () => {
      this.transport.sendToCoordinators(CONTROL_TOPICS.graphStart, { graphId }, 'harness');
    });
  }

  /**
   * Graceful shutdown: SIGTERM every slave (it flushes telemetry and exits on
   * its own), wait for them to exit up to `graceMs`, then SIGKILL any survivor.
   * The wait is what lets the slave's final trace batch reach Jaeger — an
   * immediate SIGKILL would drop the last spans (orphaned worker.execute).
   */
  public async shutdown(graceMs: number): Promise<void> {
    const slaves = [...this.slaves.values()];
    const exits = slaves.map((s) => new Promise<void>((resolve) => s.child.once('exit', () => resolve())));
    for (const s of slaves) s.child.kill('SIGTERM');
    await Promise.race([Promise.all(exits), new Promise<void>((resolve) => setTimeout(resolve, graceMs))]);
    for (const nodeId of [...this.slaves.keys()]) this.kill(nodeId, false); // SIGKILL stragglers
  }

  public awaitAllReady(timeoutMs: number): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`nodes not ready within ${timeoutMs}ms`)), timeoutMs);
      this.readyCheck = () => {
        if (this.ready.size >= this.scenario.coordinatorIds().length) {
          clearTimeout(timer);
          this.readyCheck = undefined;
          resolve();
        }
      };
      this.readyCheck();
    });
  }

  private markReady(nodeId: NodeId): void {
    this.ready.add(nodeId);
    this.events.record({ type: 'node.ready', nodeId });
    this.metrics.countNodeLifecycle('ready');
    this.readyCheck?.();
  }

  private async onExit(nodeId: NodeId): Promise<void> {
    const slave = this.slaves.get(nodeId);
    const injected = slave?.injectedKill ?? false;
    this.slaves.delete(nodeId);
    this.registry.unregister(nodeId);
    this.ready.delete(nodeId);
    this.events.record({ type: 'node.exited', nodeId, data: { injected } });
    this.metrics.countNodeLifecycle('exited');
    // Free any locks the dead node held or was waiting on.
    await this.storage.releaseNode(nodeId).catch((err: unknown) => {
      this.logger.warn(`releaseNode(${nodeId}) failed: ${String(err)}`);
    });
  }
}
