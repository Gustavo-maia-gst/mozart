import type { JsonObject, ProtocolLogger } from '@mozart/contracts';
import { Inject, Injectable, Logger } from '@nestjs/common';
import { type Attributes, trace } from '@opentelemetry/api';
import { NODE_ID } from '../tokens';

/** Protocol logger: mirrors messages to the Nest logger and the active span. */
@Injectable()
export class SpanLogger implements ProtocolLogger {
  private readonly logger: Logger;

  constructor(@Inject(NODE_ID) private readonly nodeId: string) {
    this.logger = new Logger(`protocol:${nodeId}`);
  }

  public debug(message: string, attrs?: JsonObject): void {
    this.emit('debug', message, attrs);
  }
  public info(message: string, attrs?: JsonObject): void {
    this.emit('log', message, attrs);
  }
  public warn(message: string, attrs?: JsonObject): void {
    this.emit('warn', message, attrs);
  }
  public error(message: string, attrs?: JsonObject): void {
    this.emit('error', message, attrs);
  }

  private emit(level: 'debug' | 'log' | 'warn' | 'error', message: string, attrs?: JsonObject): void {
    trace.getActiveSpan()?.addEvent(message, attrs as Attributes | undefined);
    this.logger[level](message);
  }
}
