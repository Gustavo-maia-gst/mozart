import { createWriteStream, mkdirSync, type WriteStream } from 'node:fs';
import { join } from 'node:path';
import type { EventType, HarnessEvent } from '@mozart/contracts';
import { activeIds } from '@mozart/telemetry';
import { Inject, Injectable } from '@nestjs/common';
import type { Clock } from '../clock/clock';
import type { EnvConfig } from '../config/env';
import { CLOCK, ENV_CONFIG, RUN_ID } from '../tokens';

export type EventInput = Omit<HarnessEvent, 'ts' | 'seq' | 'runId' | 'traceId' | 'spanId'> &
  Partial<Pick<HarnessEvent, 'traceId' | 'spanId'>>;

/**
 * Append-only JSONL recorder — the master-side ground truth for later metric
 * and correctness analysis. `seq` is a monotonic per-run counter, so the log
 * is totally ordered. Each record is stamped with the active trace/span id.
 */
@Injectable()
export class EventLogService {
  private stream?: WriteStream;
  private seq = 0;
  private readonly counts = new Map<EventType, number>();
  private path = '';

  constructor(
    @Inject(CLOCK) private readonly clock: Clock,
    @Inject(RUN_ID) private readonly runId: string,
    @Inject(ENV_CONFIG) private readonly env: EnvConfig,
  ) {}

  public open(): void {
    const dir = join(this.env.MOZART_LOG_DIR, this.runId);
    mkdirSync(dir, { recursive: true });
    this.path = join(dir, 'events.jsonl');
    this.stream = createWriteStream(this.path, { flags: 'a' });
  }

  public get logPath(): string {
    return this.path;
  }

  public record(event: EventInput): HarnessEvent {
    const ids = activeIds();
    const full: HarnessEvent = {
      ts: this.clock.now(),
      seq: this.seq++,
      runId: this.runId,
      traceId: ids.traceId,
      spanId: ids.spanId,
      ...event,
    };
    this.counts.set(full.type, (this.counts.get(full.type) ?? 0) + 1);
    this.stream?.write(`${JSON.stringify(full)}\n`);
    return full;
  }

  public countsByType(): Record<string, number> {
    return Object.fromEntries(this.counts);
  }

  public async close(): Promise<void> {
    const stream = this.stream;
    if (!stream) return;
    await new Promise<void>((resolve) => stream.end(resolve));
    this.stream = undefined;
  }
}
