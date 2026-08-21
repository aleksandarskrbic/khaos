import { source } from "@/lib/source";
import { createFromSource } from "fumadocs-core/search/server";

// In-process Orama search over the static docs index: no external search
// service, no cost, fast enough for a docs set this size. Good default for
// a small OSS project; swap for orama-cloud only if the corpus grows large
// enough that build-time indexing becomes slow.
export const { GET } = createFromSource(source);
