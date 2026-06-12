// build.mjs — run from the backend folder: node build.mjs

import { execSync } from "child_process";
import fs           from "fs";
import path         from "path";
import { fileURLToPath } from "url";

import { rmSync } from "fs";

// Clean sensitive data before packaging
rmSync("data", { recursive: true, force: true });
rmSync("dist/logs", { recursive: true, force: true });
rmSync("dist/data", { recursive: true, force: true });
console.log("✓ Cleaned sensitive data before build");


const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT      = path.resolve(__dirname, "..");
const FRONTEND  = path.join(ROOT, "frontend");
const DIST      = path.join(__dirname, "dist");
const FB_DEST   = path.join(DIST, "frontend-build");

// 1. Build React
console.log("\n[1/3] Building React frontend...");
execSync("npm run build", { cwd: FRONTEND, stdio: "inherit" });

// 2. Copy frontend build into dist/
console.log("\n[2/3] Copying frontend build to dist/frontend-build...");
fs.rmSync(FB_DEST, { recursive: true, force: true });
fs.mkdirSync(DIST, { recursive: true });
fs.cpSync(path.join(FRONTEND, "build"), FB_DEST, { recursive: true });

// 3. Bundle + package
console.log("\n[3/3] Packaging backend into EXE...");

const esbuildScript = `
import * as esbuild from 'esbuild';

await esbuild.build({
  entryPoints: ['server.js'],
  bundle: true,
  platform: 'node',
  target: 'node18',
  format: 'cjs',
  outfile: '_bundle.cjs',

banner: {
    js: "const __importMetaUrl = require('url').pathToFileURL(__filename).href;",
  },
  define: {
    'import.meta.url': '__importMetaUrl',
  },

  external: [
    'axios', 'form-data', 'proxy-from-env', 'follow-redirects',
    'xml2js', 'xmlbuilder', 'sax',
    'cors', 'object-assign',
    'agentkeepalive', 'humanize-ms',
    'express', 'dotenv', 'node-cron',
  ],
});
console.log('esbuild bundle complete');
`;

const esbuildScriptPath = path.join(__dirname, "_esbuild.mjs");
fs.writeFileSync(esbuildScriptPath, esbuildScript);
const bundlePath = path.join(__dirname, "_bundle.cjs");

try {
  execSync("node _esbuild.mjs", { cwd: __dirname, stdio: "inherit" });

  execSync(
    "npx pkg _bundle.cjs --targets node18-win-x64 --output dist/tally-middleware-backend.exe --compress GZip",
    { cwd: __dirname, stdio: "inherit" }
  );
} finally {
  fs.rmSync(esbuildScriptPath, { force: true });
  fs.rmSync(bundlePath, { force: true });
}

console.log("\nDone! EXE is at: dist/tally-middleware-backend.exe");
console.log("Ship the entire dist/ folder (EXE + frontend-build/ together).\n");
