import Link from "next/link";
import { LinkButton } from "@/components/ui/button";
import { siteConfig } from "@/lib/site-config";

export function LandingFooter() {
  return (
    <footer className="mx-auto max-w-6xl px-6 py-16">
      <div className="rounded-lg border border-fd-border bg-fd-card p-8 text-center sm:p-12">
        <h2 className="text-2xl leading-tight font-bold tracking-tight text-fd-foreground">
          Try it against a scenario in a few minutes.
        </h2>
        <p className="mx-auto mt-3 max-w-xl leading-relaxed text-fd-muted-foreground">
          A bundled 3-broker Kafka cluster, ready-made scenarios, and a live
          terminal dashboard. No cluster of your own required to start.
        </p>
        <div className="mt-6 flex flex-wrap items-center justify-center gap-3">
          <LinkButton href="/docs/quickstart">Quick Start</LinkButton>
          <LinkButton href="/docs" variant="secondary">
            Browse Docs
          </LinkButton>
        </div>
      </div>

      <div className="mt-10 flex flex-col gap-4 border-t border-fd-border pt-6 text-sm text-fd-muted-foreground sm:flex-row sm:items-center sm:justify-between">
        <span>Apache 2.0 License</span>
        <div className="flex gap-5">
          <Link href="/docs" className="hover:text-fd-foreground">
            Documentation
          </Link>
          <a
            href={siteConfig.github}
            target="_blank"
            rel="noopener noreferrer"
            className="hover:text-fd-foreground"
          >
            GitHub
          </a>
          <a
            href={`${siteConfig.github}/blob/main/CONTRIBUTING.md`}
            target="_blank"
            rel="noopener noreferrer"
            className="hover:text-fd-foreground"
          >
            Contributing
          </a>
        </div>
      </div>
    </footer>
  );
}
