import { readFileSync } from "node:fs";
import { registerHooks } from "node:module";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const wrapperDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(wrapperDir, "..", "..");
const orchestratorPath = path.resolve(repoRoot, "src", "insight-engine", "orchestrator.ts");

registerHooks({
  resolve(specifier, context, nextResolve) {
    if ((specifier.startsWith("./") || specifier.startsWith("../")) && path.extname(specifier) === "") {
      const parentPath = fileURLToPath(context.parentURL);
      const resolvedPath = path.resolve(path.dirname(parentPath), `${specifier}.ts`);
      return {
        url: pathToFileURL(resolvedPath).href,
        format: "module-typescript",
        shortCircuit: true,
      };
    }

    return nextResolve(specifier, context);
  },
});

function fail(message) {
  process.stderr.write(`${message}\n`);
  process.exit(1);
}

let payloadText = "";
try {
  payloadText = readFileSync(0, "utf8");
} catch (error) {
  fail(`failed to read stdin: ${error instanceof Error ? error.message : String(error)}`);
}

let payload;
try {
  const normalizedPayloadText = payloadText.replace(/^\uFEFF/, "");
  payload = normalizedPayloadText.trim() ? JSON.parse(normalizedPayloadText) : null;
} catch (error) {
  fail(`invalid JSON payload: ${error instanceof Error ? error.message : String(error)}`);
}

if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
  fail("payload must be a JSON object");
}

const triggers = Array.isArray(payload.triggers) ? payload.triggers : null;
const context = payload.context && typeof payload.context === "object" && !Array.isArray(payload.context)
  ? payload.context
  : null;

if (!triggers || !context) {
  fail("payload must include object keys: triggers, context");
}

try {
  const { runInsightEngine } = await import(pathToFileURL(orchestratorPath).href);
  const result = runInsightEngine(triggers, context);
  process.stdout.write(`${JSON.stringify(result)}\n`);
} catch (error) {
  fail(`insight engine execution failed: ${error instanceof Error ? error.stack || error.message : String(error)}`);
}
