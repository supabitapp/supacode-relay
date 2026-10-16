export class Gate {
  readonly counts = new Map<string, number>();
  total = 0;

  constructor(
    private readonly perIP: number,
    private readonly max = 50_000,
  ) {}

  acquire(key: string): { status: number; reason: string } | (() => void) {
    if (this.total >= this.max)
      return { status: 503, reason: "connection capacity reached" };
    if ((this.counts.get(key) ?? 0) >= this.perIP)
      return { status: 429, reason: "client connection capacity reached" };
    this.total++;
    this.counts.set(key, (this.counts.get(key) ?? 0) + 1);
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.total--;
      const n = this.counts.get(key)! - 1;
      if (n) this.counts.set(key, n);
      else this.counts.delete(key);
    };
  }
}

export class Limiter {
  readonly entries = new Map<
    string,
    { tokens: number; seen: number; updated: number }
  >();
  private readonly burst: number;

  constructor(private readonly rate: number) {
    this.burst = Math.max(1, Math.trunc(rate));
  }

  allow(key: string, now = performance.now()): boolean {
    for (let n = 0; n < 64; n++) {
      const first = this.entries.entries().next().value;
      if (!first || now - first[1].seen <= 60_000) break;
      this.entries.delete(first[0]);
    }
    let entry = this.entries.get(key);
    if (!entry) {
      if (this.entries.size >= 65_536) return false;
      entry = { tokens: this.burst, seen: now, updated: now };
    }
    entry.tokens = Math.min(
      this.burst,
      entry.tokens + (Math.max(0, now - entry.updated) * this.rate) / 1000,
    );
    entry.updated = now;
    entry.seen = now;
    this.entries.delete(key);
    this.entries.set(key, entry);
    if (entry.tokens < 1) return false;
    entry.tokens--;
    return true;
  }
}
