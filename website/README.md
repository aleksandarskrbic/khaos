# khaos.dev

The Khaos documentation and marketing site. Next.js (App Router) + Fumadocs + Tailwind v4.

## Develop

```bash
npm install
npm run dev
```

Open [http://localhost:3000](http://localhost:3000).

## Build

```bash
npm run build
npm run start   # serve the production build locally
```

## Deploy

Deployed to Cloudflare Workers via [OpenNext](https://opennext.js.org/cloudflare).

```bash
npx wrangler login   # once, interactive
npm run deploy
```

`NEXT_PUBLIC_SITE_URL` must be set at build time (it's inlined into the client bundle). Locally
this comes from `.dev.vars` (copy `.dev.vars.example`); see `lib/site-config.ts`.

## Structure

- `app/` — routes: the landing page, the docs catch-all route, and SEO endpoints
  (`sitemap.xml`, `robots.txt`, `llms.txt`, OG image generation).
- `content/docs/` — all documentation content (MDX), organized by
  Getting Started / Guides / Scenarios / Reference.
- `components/kafka/` — the reusable Kafka visualization components
  (`Terminal`, `KafkaPartitions`, `ConsumerLag`, `ScenarioFlow`) used on both
  the landing page and inline in docs via MDX.
- `components/landing/` — landing-page-only sections.
- `lib/` — shared config (`site-config.ts`) and the Fumadocs source loader.
