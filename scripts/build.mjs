import { chmod, cp, mkdir, writeFile } from "node:fs/promises";

await mkdir("bin", { recursive: true });
await cp("internal/dashboard/static", "dist/assets/static", {
  recursive: true,
});
await cp("internal/dashboard/templates", "dist/assets/templates", {
  recursive: true,
});
await writeFile(
  "bin/relay-ts",
  `#!${process.execPath} --disable-sigusr1\nimport "../dist/main.js";\n`,
);
await chmod("bin/relay-ts", 0o755);
