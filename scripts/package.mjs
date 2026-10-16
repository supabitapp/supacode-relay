import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { cp, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";

const version = (await readFile(".node-version", "utf8")).trim();
const platform = process.argv[2] ?? "linux-x64";
if (!["linux-x64", "linux-arm64"].includes(platform))
  throw new Error("unsupported release platform");
const archive = `node-v${version}-${platform}.tar.xz`;
const origin = `https://nodejs.org/dist/v${version}/`;
const checksums = await (await checkedFetch(origin + "SHASUMS256.txt")).text();
const expected = checksums
  .split("\n")
  .find((line) => line.endsWith(`  ${archive}`))
  ?.split(" ")[0];
if (!expected) throw new Error("runtime checksum missing");
const runtime = Buffer.from(
  await (await checkedFetch(origin + archive)).arrayBuffer(),
);
if (createHash("sha256").update(runtime).digest("hex") !== expected)
  throw new Error("runtime checksum mismatch");
const stage = `bin/release-${platform}`;
await rm(stage, { recursive: true, force: true });
await mkdir(`${stage}/node`, { recursive: true });
const runtimeFile = `bin/${archive}`;
await writeFile(runtimeFile, runtime);
execFileSync("tar", [
  "-xJf",
  runtimeFile,
  "--strip-components=1",
  "-C",
  `${stage}/node`,
]);
await cp("dist", `${stage}/app`, {
  recursive: true,
  filter: (path) => !path.startsWith("dist/test"),
});
await writeFile(
  `${stage}/package.json`,
  JSON.stringify({ type: "module" }) + "\n",
);
await cp("deploy/supaterm-relay-ts.service", `${stage}/supaterm-relay.service`);
await writeFile(
  `${stage}/relay`,
  '#!/usr/local/lib/supaterm-relay/current/node/bin/node --disable-sigusr1\nimport "/usr/local/lib/supaterm-relay/current/app/main.js";\n',
  { mode: 0o755 },
);
const files = {};
async function hashFiles(directory, relative = "") {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const name = join(relative, entry.name);
    if (entry.isDirectory()) await hashFiles(join(directory, entry.name), name);
    else if (entry.isFile())
      files[name] = createHash("sha256")
        .update(await readFile(join(directory, entry.name)))
        .digest("hex");
  }
}
await hashFiles(stage);
await writeFile(
  `${stage}/manifest.json`,
  JSON.stringify(
    { node: version, platform, runtimeSHA256: expected, files },
    null,
    2,
  ) + "\n",
);
const output = `bin/relay-ts-${platform}.tar.gz`;
execFileSync("tar", ["-czf", output, "-C", stage, "."]);
const digest = createHash("sha256")
  .update(await readFile(output))
  .digest("hex");
await writeFile(output + ".sha256", `${digest}  ${output.split("/").at(-1)}\n`);
console.log(`${output} ${digest}`);

async function checkedFetch(url) {
  const response = await fetch(url);
  if (!response.ok)
    throw new Error(`runtime download failed (${response.status})`);
  return response;
}
