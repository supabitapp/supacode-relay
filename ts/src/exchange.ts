import { type IncomingMessage, type ServerResponse, STATUS_CODES } from "node:http";
import type { Socket } from "node:net";

import { handshakeError, handshakeResponse } from "./websocket.ts";

export type Headers = Record<string, string>;

export type Upgraded = { socket: Socket; head: Buffer };

export class Exchange {
  readonly req: IncomingMessage;
  readonly url: URL;
  readonly headers: Headers = {};
  status = 0;
  upgraded = false;
  keepAlive = true;
  readonly #res: ServerResponse | undefined;
  readonly #socket: Socket | undefined;
  readonly #head: Buffer | undefined;
  readonly #done: (() => void)[] = [];
  #finished = false;

  constructor(req: IncomingMessage, res?: ServerResponse, socket?: Socket, head?: Buffer) {
    this.req = req;
    this.url = new URL(req.url ?? "/", "http://relay");
    this.#res = res;
    this.#socket = socket;
    this.#head = head;
  }

  onDone(fn: () => void): void {
    this.#done.push(fn);
  }

  done(): void {
    if (this.#finished) {
      return;
    }
    this.#finished = true;
    for (const fn of this.#done.reverse()) {
      fn();
    }
  }

  respond(status: number, headers: Headers, body: string | Buffer): void {
    this.status ||= status;
    const all: Headers = { ...this.headers, ...headers };
    const payload = this.req.method === "HEAD" ? "" : body;
    if (this.#res) {
      const res = this.#res;
      res.on("close", () => this.done());
      if (!this.keepAlive) {
        res.shouldKeepAlive = false;
      }
      res.writeHead(status, { ...all, "Content-Length": String(Buffer.byteLength(body)) });
      res.end(payload);
      return;
    }
    const socket = this.#socket!;
    const lines = [
      `HTTP/1.1 ${status} ${STATUS_CODES[status] ?? ""}`,
      ...Object.entries({ ...all, "Content-Length": String(Buffer.byteLength(body)), Connection: "close" }).map(
        ([k, v]) => `${k}: ${v}`,
      ),
    ];
    socket.on("error", () => {});
    socket.end(Buffer.concat([Buffer.from(lines.join("\r\n") + "\r\n\r\n"), Buffer.from(payload)]));
    setTimeout(() => socket.destroy(), 5000).unref();
    this.done();
  }

  text(status: number, body: string, headers: Headers = {}): void {
    this.respond(status, { "Content-Type": "text/plain; charset=utf-8", "X-Content-Type-Options": "nosniff", ...headers }, body);
  }

  json(status: number, value: unknown): void {
    this.respond(
      status,
      { "Content-Type": "application/json", "Cache-Control": "no-store" },
      JSON.stringify(value, null, 2) + "\n",
    );
  }

  upgrade(): Upgraded | undefined {
    const status = this.#socket ? handshakeError(this.req) : 400;
    if (status !== undefined) {
      this.text(status, (STATUS_CODES[status] ?? "") + "\n", { "Sec-Websocket-Version": "13" });
      return undefined;
    }
    const socket = this.#socket!;
    socket.write(handshakeResponse(this.req, this.headers));
    this.status = 101;
    this.upgraded = true;
    return { socket, head: this.#head! };
  }
}
