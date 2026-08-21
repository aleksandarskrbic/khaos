#!/usr/bin/env node
// Generates public/llms-full.txt at build time by concatenating every doc
// page's raw MDX source. Runs as a build-time script rather than a request-time
// route handler because the deployed Cloudflare Worker bundle doesn't carry
// the raw content/docs source tree, only compiled build output, so any
// runtime fs.readFileSync against it fails on the deployed Worker even
// though it works fine locally with `next dev`/`next start`.
import fs from "node:fs";
import path from "node:path";

const ROOT = path.join(import.meta.dirname, "..");
const DOCS_DIR = path.join(ROOT, "content/docs");
const OUT_FILE = path.join(ROOT, "public/llms-full.txt");

const siteUrl = process.env.NEXT_PUBLIC_SITE_URL ?? "https://getkhaos.dev";
const siteName = "Khaos";
const siteDescription =
  "Khaos is an open-source Kafka traffic generator, load-testing tool, and chaos engineering CLI. Generate realistic Kafka traffic and reproduce failure scenarios like consumer lag, hot partitions, rebalances, and broker failures.";

function walk(dir) {
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) return walk(full);
    if (entry.name.endsWith(".mdx")) return [full];
    return [];
  });
}

function toUrl(file) {
  const rel = path
    .relative(DOCS_DIR, file)
    .replace(/\.mdx$/, "")
    .replace(/\/index$/, "")
    .replace(/^index$/, "");
  return `${siteUrl}/docs${rel ? `/${rel}` : ""}`;
}

const files = walk(DOCS_DIR).sort();

const sections = files.map((file) => {
  const raw = fs.readFileSync(file, "utf-8");
  return `<!-- source: ${toUrl(file)} -->\n\n${raw}`;
});

const body = `# ${siteName}\n\n> ${siteDescription}\n\n${sections.join("\n\n---\n\n")}\n`;

fs.mkdirSync(path.dirname(OUT_FILE), { recursive: true });
fs.writeFileSync(OUT_FILE, body);

console.log(`[generate-llms-full] wrote ${files.length} pages to public/llms-full.txt`);
