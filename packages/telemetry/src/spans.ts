import { type Attributes, type Span, SpanKind, SpanStatusCode, trace } from '@opentelemetry/api';
import { currentScope } from './context-store';
import { TRACER_NAME } from './telemetry';

/** Mozart span/attribute keys (mixed with standard messaging.* where they exist). */
export const ATTR = {
  nodeId: 'mozart.node_id',
  runId: 'mozart.run_id',
  protocol: 'mozart.protocol',
  channel: 'mozart.channel',
  taskId: 'mozart.task_id',
  messageId: 'messaging.message_id',
  deliveryId: 'mozart.delivery_id',
  attempt: 'mozart.attempt',
  topic: 'messaging.destination.name',
} as const;

/**
 * Builds a span name from the decorated call's arguments, e.g.
 * `(taskId) => \`worker.start(${taskId})\``. Only valid on method decorators.
 */
// `any[]` (not `unknown[]`): the builder mirrors the decorated method's own
// arg types, which are erased at the decorator boundary — this keeps call sites
// like `(taskId) => ...` ergonomic without per-site casts.
export type SpanNameBuilder = (...args: any[]) => string;

export interface TraceOptions {
  /**
   * Explicit span name. On a method it is the full span name (or a builder that
   * derives it from the call's args); on a class it is the prefix used for its
   * methods (default: the class name), which then get `<prefix>.<method>`.
   */
  name?: string | SpanNameBuilder;
  kind?: SpanKind;
}

/** Adds attributes to the currently active span (e.g. per-call dynamic values). */
export function annotateSpan(attributes: Attributes): void {
  trace.getActiveSpan()?.setAttributes(attributes);
}

/** Trace/span id of the currently active span, for stamping event-log records. */
export function activeIds(): { traceId?: string; spanId?: string } {
  const ctx = trace.getActiveSpan()?.spanContext();
  return ctx ? { traceId: ctx.traceId, spanId: ctx.spanId } : {};
}

type AnyFn = (...args: unknown[]) => unknown;

function message(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/** Stamp the ambient scope (nodeId/taskId) onto a span, when present. */
function stampScope(span: Span): void {
  const { nodeId, taskId } = currentScope();
  if (nodeId) span.setAttribute(ATTR.nodeId, nodeId);
  if (taskId) span.setAttribute(ATTR.taskId, taskId);
}

/** Wraps a method so each call runs inside an active span. */
function wrap(original: AnyFn, name: string | SpanNameBuilder, kind?: SpanKind): AnyFn {
  return function (this: unknown, ...args: unknown[]): unknown {
    const spanName = typeof name === 'function' ? name(...args) : name;
    return trace.getTracer(TRACER_NAME).startActiveSpan(spanName, { kind: kind ?? SpanKind.INTERNAL }, (span) => {
      stampScope(span);
      try {
        const result = original.apply(this, args);
        if (result instanceof Promise) {
          return result.then(
            (value) => {
              span.setStatus({ code: SpanStatusCode.OK });
              span.end();
              return value;
            },
            (err: unknown) => {
              span.recordException(err as Error);
              span.setStatus({ code: SpanStatusCode.ERROR, message: message(err) });
              span.end();
              throw err;
            },
          );
        }
        span.setStatus({ code: SpanStatusCode.OK });
        span.end();
        return result;
      } catch (err) {
        span.recordException(err as Error);
        span.setStatus({ code: SpanStatusCode.ERROR, message: message(err) });
        span.end();
        throw err;
      }
    });
  };
}

function decorateMethod(
  target: object,
  key: string | symbol,
  descriptor: PropertyDescriptor,
  opts: TraceOptions,
): void {
  if (typeof descriptor.value !== 'function') return;
  const className = (target as { constructor: { name: string } }).constructor.name;
  const spanName = opts.name ?? `${className}.${String(key)}`;
  descriptor.value = wrap(descriptor.value as AnyFn, spanName, opts.kind);
}

function decorateClass(ctor: { name: string; prototype: object }, opts: TraceOptions): void {
  // A name builder only makes sense per method; at class level the name is a prefix.
  const prefix = typeof opts.name === 'string' ? opts.name : ctor.name;
  for (const key of Object.getOwnPropertyNames(ctor.prototype)) {
    if (key === 'constructor') continue;
    const descriptor = Object.getOwnPropertyDescriptor(ctor.prototype, key);
    if (!descriptor || typeof descriptor.value !== 'function') continue;
    descriptor.value = wrap(descriptor.value as AnyFn, `${prefix}.${key}`, opts.kind);
    Object.defineProperty(ctor.prototype, key, descriptor);
  }
}

/**
 * Opens an active span around a method (or every method of a class). The span
 * kind and name come from `opts`; `nodeId`/`taskId` are stamped from the
 * ambient trace scope (see `runInTraceScope`). Awaited work inherits the span
 * as parent, so blocking calls nest correctly.
 */
export function Trace(
  opts: TraceOptions = {},
): (target: object, propertyKey?: string | symbol, descriptor?: PropertyDescriptor) => void {
  return (target, propertyKey, descriptor) => {
    if (propertyKey !== undefined && descriptor) {
      decorateMethod(target, propertyKey, descriptor, opts);
      return;
    }
    decorateClass(target as { name: string; prototype: object }, opts);
  };
}

export { SpanKind } from '@opentelemetry/api';
