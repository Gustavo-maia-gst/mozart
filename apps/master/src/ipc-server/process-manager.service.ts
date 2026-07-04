import { fork, type ChildProcess } from 'node:child_process';
import { join } from 'node:path';
import { Inject, Injectable, Logger } from '@nestjs/common';
import { childFrameChannel, NodeLink, type RpcHandlers } from '@mozart/ipc';
import { traceContextHooks } from '@mozart/telemetry';
import type { NodeId, Scenario } from '@mozart/contracts';
import type { EnvConfig } from '../config/env';
import { EventLogService } from '../event-log/event-log.service';
import { coordinatorIds } from '../scenario/scenario';
import { StorageService } from '../storage/storage.service';
import { ENV_CONFIG, RUN_ID, SCENARIO } from '../tokens';
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

  constructor(
    @Inject(SCENARIO) private readonly scenario: Scenario,
    @Inject(RUN_ID) private readonly runId: string,
    @Inject(ENV_CONFIG) private readonly env: EnvConfig,
    private readonly ipcHost: IpcHostService,
    private readonly registry: NodeRegistry,
    private readonly storage: StorageService,
    private readonly events: EventLogService,
  ) {
    this.handlers = this.ipcHost.buildHandlers();
    this.ipcHost.onNodeReady = (nodeId) => this.markReady(nodeId);
  }

  spawnAll(): void {
    for (const nodeId of coordinatorIds(this.scenario)) this.spawn(nodeId);
  }

  spawn(nodeId: NodeId): void {
    const entrypoint =
      this.env.MOZART_SLAVE_ENTRYPOINT ?? join(process.cwd(), 'apps/slave/dist/main.js');
    const child = fork(entrypoint, [], {
      serialization: 'json',
      execArgv: [],
      env: {
        ...process.env,
        MOZART_NODE_ID: nodeId,
        MOZART_PROTOCOL: this.scenario.protocol,
        MOZART_RUN_ID: this.runId,
        OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: this.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
        ...(this.env.MOZART_OTEL_PROCESSOR
          ? { MOZART_OTEL_PROCESSOR: this.env.MOZART_OTEL_PROCESSOR }
          : {}),
      },
    });

    const link = new NodeLink(nodeId, childFrameChannel(child), this.handlers, traceContextHooks());
    this.registry.register(link);
    this.slaves.set(nodeId, { child, injectedKill: false });
    link.onClose(() => void this.onExit(nodeId));
    this.events.record({ type: 'node.spawned', nodeId });
  }

  /** SIGKILL a node. `injected` distinguishes fault kills from shutdown kills. */
  kill(nodeId: NodeId, injected = true): void {
    const slave = this.slaves.get(nodeId);
    if (!slave) return;
    slave.injectedKill = injected;
    if (injected) this.events.record({ type: 'node.killed', nodeId });
    slave.child.kill('SIGKILL');
  }

  restart(nodeId: NodeId): void {
    this.events.record({ type: 'node.restarted', nodeId });
    this.spawn(nodeId); // fresh, stateless; link rebound under same nodeId
  }

  activateAll(): void {
    for (const nodeId of this.registry.liveNodeIds()) {
      this.registry.get(nodeId)?.push('protocol.activate', {});
    }
  }

  /** Graceful shutdown: ask protocols to deactivate, then SIGKILL survivors. */
  shutdown(): void {
    for (const nodeId of this.registry.liveNodeIds()) {
      this.registry.get(nodeId)?.push('protocol.deactivate', {});
    }
    for (const nodeId of [...this.slaves.keys()]) this.kill(nodeId, false);
  }

  awaitAllReady(timeoutMs: number): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`nodes not ready within ${timeoutMs}ms`)),
        timeoutMs,
      );
      this.readyCheck = () => {
        if (this.ready.size >= coordinatorIds(this.scenario).length) {
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
    this.readyCheck?.();
  }

  private async onExit(nodeId: NodeId): Promise<void> {
    const slave = this.slaves.get(nodeId);
    const injected = slave?.injectedKill ?? false;
    this.slaves.delete(nodeId);
    this.registry.unregister(nodeId);
    this.ready.delete(nodeId);
    this.events.record({ type: 'node.exited', nodeId, data: { injected } });
    // Free any locks the dead node held or was waiting on.
    await this.storage.releaseNode(nodeId).catch((err: unknown) => {
      this.logger.warn(`releaseNode(${nodeId}) failed: ${String(err)}`);
    });
  }
}
