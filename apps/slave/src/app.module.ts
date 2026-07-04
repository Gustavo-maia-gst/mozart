import { Module, type DynamicModule } from '@nestjs/common';
import { ProtocolHostModule } from './protocol-host/protocol-host.module';
import { NODE_ID, PROTOCOL_NAME, RUN_ID } from './tokens';

export interface SlaveParams {
  nodeId: string;
  protocol: string;
  runId: string;
}

@Module({})
export class SlaveModule {
  static forNode(params: SlaveParams): DynamicModule {
    return {
      module: SlaveModule,
      global: true, // NODE_ID/PROTOCOL_NAME/RUN_ID visible to ProtocolHostModule
      imports: [ProtocolHostModule],
      providers: [
        { provide: NODE_ID, useValue: params.nodeId },
        { provide: PROTOCOL_NAME, useValue: params.protocol },
        { provide: RUN_ID, useValue: params.runId },
      ],
      exports: [NODE_ID, PROTOCOL_NAME, RUN_ID],
    };
  }
}
