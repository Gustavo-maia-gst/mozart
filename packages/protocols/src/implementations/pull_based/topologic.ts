import { Graph, GraphId, WorkerSuccessEvent, WorkerFailEvent, Message } from '@mozart/contracts';
import { Protocol } from '../../protocol';

export class TopologicalBarrierProtocol extends Protocol {
  override name: string = 'topological-barrier';

  public override startGraph(_graphId: GraphId): Promise<void> {
    throw new Error('Method not implemented.');
  }

  public override onMessage(_event: Message): Promise<void> {
    throw new Error('Method not implemented.');
  }

  public override onWorkerSuccess(_event: WorkerSuccessEvent): Promise<void> {
    throw new Error('Method not implemented.');
  }

  public override onWorkerFail(_event: WorkerFailEvent): Promise<void> {
    throw new Error('Method not implemented.');
  }

  public override persistGraph(_graph: Graph): Promise<void> {
    throw new Error('Method not implemented.');
  }
}
