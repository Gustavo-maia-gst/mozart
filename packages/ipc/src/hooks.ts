/**
 * Telemetry seam kept out of the IPC layer proper. The apps wire concrete
 * implementations from `@mozart/telemetry`; by default these are no-ops so
 * the IPC layer has zero OTel dependency and stays unit-testable in isolation.
 */
export interface IpcHooks {
  /** Populate a carrier with the currently-active trace context before send. */
  injectTraceCtx?: (carrier: Record<string, string>) => void;
  /** Run `fn` with the trace context extracted from an incoming carrier as parent. */
  runWithTraceCtx?: <T>(carrier: Record<string, string>, fn: () => T) => T;
}

export const NOOP_HOOKS: Required<IpcHooks> = {
  injectTraceCtx: () => {},
  runWithTraceCtx: (_carrier, fn) => fn(),
};

export function resolveHooks(hooks?: IpcHooks): Required<IpcHooks> {
  return {
    injectTraceCtx: hooks?.injectTraceCtx ?? NOOP_HOOKS.injectTraceCtx,
    runWithTraceCtx: hooks?.runWithTraceCtx ?? NOOP_HOOKS.runWithTraceCtx,
  };
}
