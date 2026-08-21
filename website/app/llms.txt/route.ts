import { source } from "@/lib/source";
import { llms } from "fumadocs-core/source";
import { siteConfig } from "@/lib/site-config";

// A directory of every doc page with its description, in the format
// https://llmstxt.org proposes, letting coding agents and LLM tools discover
// what's documented without crawling HTML.
export const revalidate = false;

export function GET() {
  const { index } = llms(source);
  // index() emits relative /docs/... links; make them absolute so the file
  // is useful fetched standalone, outside the site.
  const body = index().replace(/\]\(\//g, `](${siteConfig.url}/`);
  return new Response(body, {
    headers: { "Content-Type": "text/markdown; charset=utf-8" },
  });
}
