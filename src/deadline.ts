import { performance } from "node:perf_hooks";

export class Deadline {
  private timer: NodeJS.Timeout | undefined;
  private at: number | undefined;
  private suspended = false;
  private window = 0;

  constructor(private readonly expire: () => void) {}

  set(milliseconds: number): void {
    this.at = performance.now() + milliseconds;
    if (!this.suspended && this.timer && this.window === milliseconds) {
      this.timer.refresh();
      return;
    }
    this.schedule();
  }

  suspend(): void {
    this.suspended = true;
    clearTimeout(this.timer);
    this.timer = undefined;
  }

  resume(): void {
    this.suspended = false;
    this.schedule();
  }

  cancel(): void {
    clearTimeout(this.timer);
    this.timer = undefined;
    this.at = undefined;
  }

  private schedule(): void {
    clearTimeout(this.timer);
    if (this.at === undefined || this.suspended) return;
    const remaining = this.at - performance.now();
    this.window = Math.max(1, Math.min(Math.ceil(remaining), 2_147_483_647));
    this.timer = setTimeout(() => {
      this.timer = undefined;
      if (this.at === undefined) return;
      if (performance.now() < this.at) this.schedule();
      else {
        this.at = undefined;
        this.expire();
      }
    }, this.window);
    this.timer.unref();
  }
}
