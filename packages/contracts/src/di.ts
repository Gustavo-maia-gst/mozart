import type { JsonObject } from './json';

/**
 * DI tokens the harness binds and protocols inject. The concrete ports live in
 * the slave app (backed by IPC to the master); protocols depend only on the
 * port interfaces via these tokens.
 */
export const TRANSPORT_PORT = Symbol('TRANSPORT_PORT');
export const STORAGE_PORT = Symbol('STORAGE_PORT');
export const WORKER_POOL_PORT = Symbol('WORKER_POOL_PORT');
export const PROTOCOL_LOGGER = Symbol('PROTOCOL_LOGGER');
/** The resolved protocol instance for this node. */
export const PROTOCOL = Symbol('PROTOCOL');

/** Structured logger surfaced to protocol code; entries attach to the active span. */
export interface ProtocolLogger {
  debug(message: string, attrs?: JsonObject): void;
  info(message: string, attrs?: JsonObject): void;
  warn(message: string, attrs?: JsonObject): void;
  error(message: string, attrs?: JsonObject): void;
}
