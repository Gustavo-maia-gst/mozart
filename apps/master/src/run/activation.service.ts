import {
  type Json,
  ProtocolLogger,
  Scenario,
  StoragePort,
  type StorageQuery,
  type TaskId,
  type TaskMatch,
  type TaskState,
  TransportPort,
} from '@mozart/contracts';
import { type Protocol, resolveProtocol } from '@mozart/protocols';
import { annotateSpan, ATTR, Trace } from '@mozart/telemetry';
import { Inject, Injectable, Logger } from '@nestjs/common';
import { SCENARIO } from '../tokens';
import { StorageService } from '../storage/storage.service';

/** Storage ops from the master's persist-only protocol are attributed to `harness`. */
const PERSIST_ORIGIN = 'harness';

/** StoragePort over the master's StorageService — enough for graph persistence. */
class MasterStorage extends StoragePort {
  constructor(private readonly storage: StorageService) {
    super();
  }
  public read(taskId: TaskId): Promise<TaskState | null> {
    return this.storage.read(PERSIST_ORIGIN, taskId);
  }
  public save(taskId: TaskId, data: TaskState): Promise<void> {
    return this.storage.save(PERSIST_ORIGIN, taskId, data);
  }
  public find(query: StorageQuery): Promise<TaskMatch[]> {
    return this.storage.find(PERSIST_ORIGIN, query);
  }
  public readExclusive(): Promise<never> {
    throw new Error('readExclusive is not available to the master persist-only protocol');
  }
  public delete(): Promise<never> {
    throw new Error('delete is not available to the master persist-only protocol');
  }
}

/** The persist-only protocol instance never sends — persistGraph is setup, not coordination. */
class NoTransport extends TransportPort {
  private fail(): never {
    throw new Error('transport is not available to the master persist-only protocol');
  }
  public sendToCoordinators(_topic: string, _body: Json): Promise<void> {
    return this.fail();
  }
  public sendToWorkerPool(_taskId: TaskId): Promise<void> {
    return this.fail();
  }
  public completeGraph(): Promise<void> {
    return this.fail();
  }
}

class NestProtocolLogger extends ProtocolLogger {
  private readonly logger = new Logger('MasterProtocol');
  public debug(message: string): void {
    this.logger.debug(message);
  }
  public info(message: string): void {
    this.logger.log(message);
  }
  public warn(message: string): void {
    this.logger.warn(message);
  }
  public error(message: string): void {
    this.logger.error(message);
  }
}

/**
 * Persists every graph up-front, on the master, using a local instance of the
 * run's protocol (so the on-disk layout stays protocol-defined). Doing it here —
 * a single synchronous step before any `graph.start` — is what guarantees the
 * invariant: no graph is ever started before ALL graphs are persisted. No
 * per-slave activation handshake needed.
 */
@Injectable()
export class ActivationService {
  private readonly protocol: Protocol;

  constructor(
    @Inject(SCENARIO) private readonly scenario: Scenario,
    storage: StorageService,
  ) {
    const ProtocolClass = resolveProtocol(scenario.protocol);
    this.protocol = new ProtocolClass(new NoTransport(), new MasterStorage(storage), new NestProtocolLogger());
  }

  @Trace({ name: 'persistAllGraphs' })
  public async persistAllGraphs(): Promise<void> {
    annotateSpan({ [ATTR.graphId]: 'all', 'mozart.graph_count': this.scenario.graphs.length });
    for (const graph of this.scenario.graphs) await this.protocol.persistGraph(graph);
  }
}
