import type { JsonObject } from './json';

/**
 * Structured logger surfaced to protocol code; entries attach to the active
 * span. Abstract class so it doubles as a Nest DI token (constructor injection
 * without `@Inject`).
 */
export abstract class ProtocolLogger {
  abstract debug(message: string, attrs?: JsonObject): void;
  abstract info(message: string, attrs?: JsonObject): void;
  abstract warn(message: string, attrs?: JsonObject): void;
  abstract error(message: string, attrs?: JsonObject): void;
}
