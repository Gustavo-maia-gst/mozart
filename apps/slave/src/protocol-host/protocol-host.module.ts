import { Module } from '@nestjs/common';
import { HarnessClientModule } from '../harness-client/harness-client.module';
import { ProtocolHostService } from './protocol-host.service';

@Module({
  imports: [HarnessClientModule],
  providers: [ProtocolHostService],
  exports: [ProtocolHostService],
})
export class ProtocolHostModule {}
