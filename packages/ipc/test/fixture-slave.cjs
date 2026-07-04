// Real forked-slave fixture for the IPC integration test. Plain CJS so it can
// be forked without a TS loader — it requires the built dist of @mozart/ipc.
const { processFrameChannel, IpcClient } = require('../dist/index.js');

const client = new IpcClient(processFrameChannel());

client.onPush(async (type, payload) => {
  if (type === 'delivery') {
    // Parent leaves storage.read pending, so this hangs until the parent
    // SIGKILLs us — exercising kill-during-call from the client side.
    await client.call('storage.read', { taskId: 'x' });
    await client.call('transport.ack', { deliveryId: payload.deliveryId });
  }
});

(async () => {
  const { scenario } = await client.call('node.ready', {});
  // Echo our identity back through a distinct RPC so the parent can assert the
  // full round trip happened over a real fork.
  await client.call('transport.toWorkerPool', { taskId: `ready:${scenario.nodeId}` });
})().catch(() => {});
