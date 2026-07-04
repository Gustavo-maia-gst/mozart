import { Module } from '@nestjs/common';
import { CLOCK, SCHEDULER } from '../tokens';

/** Wall-clock time source. Injected so tests can substitute a fake. */
export interface Clock {
  now(): number;
}

export type CancelHandle = symbol;

/**
 * Timer facade. Backed by global timers (so vitest fake timers work), but
 * injected as a token so a fully virtual scheduler can be swapped in later.
 */
export interface Scheduler {
  after(ms: number, fn: () => void): CancelHandle;
  cancel(handle: CancelHandle): void;
}

export class SystemClock implements Clock {
  public now(): number {
    return Date.now();
  }
}

export class TimerScheduler implements Scheduler {
  private readonly timers = new Map<CancelHandle, NodeJS.Timeout>();

  public after(ms: number, fn: () => void): CancelHandle {
    const handle: CancelHandle = Symbol('timer');
    const timer = setTimeout(() => {
      this.timers.delete(handle);
      fn();
    }, ms);
    this.timers.set(handle, timer);
    return handle;
  }

  public cancel(handle: CancelHandle): void {
    const timer = this.timers.get(handle);
    if (timer) {
      clearTimeout(timer);
      this.timers.delete(handle);
    }
  }
}

@Module({
  providers: [
    { provide: CLOCK, useClass: SystemClock },
    { provide: SCHEDULER, useClass: TimerScheduler },
  ],
  exports: [CLOCK, SCHEDULER],
})
export class ClockModule {}
