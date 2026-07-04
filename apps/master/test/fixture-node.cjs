// Minimal slave stub for the process-manager test. Announces readiness and then
// acquires an exclusive lock it never releases — so a SIGKILL exercises the
// crash force-release path.
const { processFrameChannel, IpcClient } = require('@mozart/ipc');

const client = new IpcClient(processFrameChannel());

client
  .call('node.ready', {})
  .then(() => client.call('storage.readExclusive', { taskId: 't1' }))
  // Intentionally hold the lease forever.
  .catch(() => {});
