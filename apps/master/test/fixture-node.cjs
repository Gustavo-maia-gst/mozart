// Minimal slave stub for the process-manager test. Announces readiness and,
// on activation, acquires an exclusive lock it never releases — so a SIGKILL
// exercises the crash force-release path.
const { processFrameChannel, IpcClient } = require('@mozart/ipc');

const client = new IpcClient(processFrameChannel());

client.onPush(async (type) => {
  if (type === 'protocol.activate') {
    await client.call('storage.readExclusive', { taskId: 't1' });
    // Intentionally hold the lease forever.
  }
});

client.call('node.ready', {}).catch(() => {});
