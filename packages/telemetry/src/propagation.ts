import { context, propagation } from '@opentelemetry/api';

/** W3C-inject the currently-active context into a carrier (for an outgoing envelope). */
export function injectActiveContext(carrier: Record<string, string>): void {
  propagation.inject(context.active(), carrier);
}

/** Run `fn` with the context extracted from an incoming envelope carrier as parent. */
export function runWithExtractedContext<T>(carrier: Record<string, string>, fn: () => T): T {
  const ctx = propagation.extract(context.active(), carrier);
  return context.with(ctx, fn);
}

/**
 * Trace-context hooks shaped for `@mozart/ipc`'s `IpcHooks`. Returned as a
 * plain object so this package need not depend on the ipc package.
 */
export function traceContextHooks(): {
  injectTraceCtx: (carrier: Record<string, string>) => void;
  runWithTraceCtx: <T>(carrier: Record<string, string>, fn: () => T) => T;
} {
  return {
    injectTraceCtx: injectActiveContext,
    runWithTraceCtx: runWithExtractedContext,
  };
}
