import { LinkButton } from "@/components/ui/button";
import { siteConfig } from "@/lib/site-config";

export function GoRewriteStrip() {
  return (
    <section className="border-y border-khaos-border-strong bg-fd-card">
      <div className="mx-auto flex max-w-6xl flex-col items-start justify-between gap-4 px-6 py-8 sm:flex-row sm:items-center">
        <p className="max-w-2xl text-sm leading-relaxed text-fd-muted-foreground">
          <span className="font-medium text-fd-foreground">
            Rewritten in Go.
          </span>{" "}
          A single static binary, no runtime dependencies, cross-compiled
          with a pure-Go Kafka client, for a smaller footprint, easier
          distribution, and a simpler single-binary CLI.
        </p>
        <LinkButton
          href={`${siteConfig.github}/releases`}
          variant="secondary"
          external
        >
          Releases
        </LinkButton>
      </div>
    </section>
  );
}
