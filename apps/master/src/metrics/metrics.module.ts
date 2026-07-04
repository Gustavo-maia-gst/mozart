import { Global, Module } from '@nestjs/common';
import { MetricsService } from './metrics.service';

/**
 * Global so any feature service can inject {@link MetricsService} the same way
 * they inject {@link EventLogService}, without threading it through every module.
 */
@Global()
@Module({
  providers: [MetricsService],
  exports: [MetricsService],
})
export class MetricsModule {}
