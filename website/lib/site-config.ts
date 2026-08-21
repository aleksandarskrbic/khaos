// Central place for anything that changes when the site moves domains,
// so swapping in a custom domain later is a one-line edit.
// NEXT_PUBLIC_SITE_URL should be set explicitly in the hosting provider's
// env config (it must be present at build time, not just runtime, since
// Next.js inlines NEXT_PUBLIC_* into the client bundle). This falls back to
// the real production domain so a local build without the var still points
// somewhere real rather than a broken placeholder, but the var is still the
// source of truth, warn if it's missing in a production build so that
// doesn't happen silently.
if (!process.env.NEXT_PUBLIC_SITE_URL && process.env.NODE_ENV === "production") {
  console.warn(
    "[site-config] NEXT_PUBLIC_SITE_URL is unset, falling back to https://getkhaos.dev in SEO metadata. Set it explicitly in the deploy environment.",
  );
}
const url = process.env.NEXT_PUBLIC_SITE_URL ?? "https://getkhaos.dev";

export const siteConfig = {
  name: "Khaos",
  url,
  description:
    "Khaos is an open-source Kafka traffic generator, load-testing tool, and chaos engineering CLI. Generate realistic Kafka traffic and reproduce failure scenarios like consumer lag, hot partitions, rebalances, and broker failures.",
  github: "https://github.com/aleksandarskrbic/khaos",
  githubOrg: "aleksandarskrbic",
  repo: "khaos",
} as const;
