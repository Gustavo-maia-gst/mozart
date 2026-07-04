import { AsyncLocalStorage } from 'node:async_hooks';

/**
 * Ambient attributes the `@Trace` decorator stamps onto every span it opens
 * within the scope. The slave harness opens a scope per dispatch (with the
 * node id) and enriches it with the task id when the delivery carries one — so
 * downstream storage/worker spans inherit `nodeId`/`taskId` without threading
 * them through every call.
 */
export interface TraceScope {
  nodeId?: string;
  taskId?: string;
}

const store = new AsyncLocalStorage<TraceScope>();

/** Runs `fn` inside a trace scope, merged over any enclosing scope. */
export function runInTraceScope<T>(scope: TraceScope, fn: () => T): T {
  return store.run({ ...store.getStore(), ...scope }, fn);
}

/** Sets the task id on the current scope; a no-op outside any scope. */
export function setTaskId(taskId: string): void {
  const current = store.getStore();
  if (current) current.taskId = taskId;
}

/** The current ambient scope, or an empty object when none is active. */
export function currentScope(): TraceScope {
  return store.getStore() ?? {};
}
