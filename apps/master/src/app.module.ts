import { type DynamicModule, Module } from '@nestjs/common';
import { CoreModule, type CoreParams } from './core/core.module';
import { RunModule } from './run/run.module';

@Module({})
export class AppModule {
  static forRun(params: CoreParams): DynamicModule {
    return {
      module: AppModule,
      imports: [CoreModule.forRun(params), RunModule],
    };
  }
}
