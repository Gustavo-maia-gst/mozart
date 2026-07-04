import { initTelemetry } from '@mozart/telemetry';

async function bootstrap(): Promise<void> {
  const nodeId = required('MOZART_NODE_ID');
  const protocol = required('MOZART_PROTOCOL');
  const runId = required('MOZART_RUN_ID');
  // The coordinator's own name drives the OTel service, so each shows up as its
  // own service in Jaeger. Falls back to the node id when unnamed.
  const name = process.env.MOZART_NODE_NAME || nodeId;

  const telemetry = initTelemetry({
    serviceName: name,
    // Slaves are not metric-instrumented (master-only), but stamping protocol
    // keeps their *traces* filterable by protocol at no cost.
    attributes: { 'mozart.node_id': nodeId, 'mozart.run_id': runId, 'mozart.protocol': protocol },
    processor: process.env.MOZART_OTEL_PROCESSOR as 'batch' | 'simple' | undefined,
  });

  const { NestFactory } = await import('@nestjs/core');
  const { SlaveModule } = await import('./app.module.js');
  const { ProtocolHostService } = await import('./protocol-host/protocol-host.service.js');

  const app = await NestFactory.createApplicationContext(SlaveModule.forNode({ nodeId, protocol, runId }), {
    logger: ['warn', 'error'],
  });

  // Graceful deactivate exits the process; ensure telemetry is flushed on exit.
  process.on('exit', () => void telemetry.shutdown());

  await app.get(ProtocolHostService).start();
  // The IPC channel keeps the event loop alive; we return and wait for pushes.
}

function required(key: string): string {
  const value = process.env[key];
  if (!value) throw new Error(`missing env ${key}`);
  return value;
}

bootstrap().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
