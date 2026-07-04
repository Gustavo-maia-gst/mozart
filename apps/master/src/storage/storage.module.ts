import type { Scenario } from '@mozart/contracts';
import { Module } from '@nestjs/common';
import type { EnvConfig } from '../config/env';
import { EventLogModule } from '../event-log/event-log.module';
import { ENV_CONFIG, SCENARIO } from '../tokens';
import { InMemoryStorageAdapter } from './in-memory.adapter';
import { PostgresStorageAdapter } from './postgres.adapter';
import { StorageService } from './storage.service';
import { STORAGE_ADAPTER, type StorageAdapter } from './storage-adapter';
import { StorageGate } from './storage-gate';

@Module({
  imports: [EventLogModule],
  providers: [
    StorageService,
    StorageGate,
    {
      provide: STORAGE_ADAPTER,
      inject: [SCENARIO, ENV_CONFIG],
      useFactory: async (scenario: Scenario, env: EnvConfig): Promise<StorageAdapter> => {
        const adapter: StorageAdapter =
          scenario.storage.adapter === 'postgres'
            ? new PostgresStorageAdapter(scenario.storage.url ?? env.MOZART_PG_URL)
            : new InMemoryStorageAdapter();
        await adapter.init?.();
        return adapter;
      },
    },
  ],
  exports: [StorageService, StorageGate, STORAGE_ADAPTER],
})
export class StorageModule {}
