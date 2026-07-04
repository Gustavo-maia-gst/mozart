/** Logical identity of a coordinator node. Stable across process restarts. */
export type NodeId = string;

export type TaskId = string;

export type RunId = string;

/** The Worker Pool entity `W` participates in transport channels under this node id. */
export const WORKER_NODE_ID: NodeId = 'W';

/** Ordered logical pipe between a pair of components: `${from}->${to}`. */
export type ChannelKey = `${string}->${string}`;

export function channelKey(from: NodeId, to: NodeId): ChannelKey {
  return `${from}->${to}`;
}
