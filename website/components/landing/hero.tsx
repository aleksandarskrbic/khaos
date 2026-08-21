import { LinkButton } from "@/components/ui/button";
import { KafkaPartitions } from "@/components/kafka/kafka-partitions";
import { siteConfig } from "@/lib/site-config";

export function Hero() {
  return (
    <section className="mx-auto grid max-w-6xl gap-12 px-6 pt-16 pb-20 lg:grid-cols-[1.1fr_1fr] lg:items-center lg:pt-24 lg:pb-28">
      <div className="min-w-0">
        <h1 className="text-4xl leading-[1.05] font-bold tracking-tighter text-fd-foreground sm:text-5xl">
          Break Kafka before production does.
        </h1>

        <p className="mt-5 max-w-xl text-base leading-relaxed text-fd-muted-foreground sm:text-lg">
          Khaos generates realistic Kafka traffic and reproduces the failure
          and performance conditions that only show up under real load:
          consumer lag, hot partitions, rebalances, and broker failures.
        </p>

        <div className="mt-8 flex flex-wrap items-center gap-3">
          <LinkButton href="/docs">Get Started</LinkButton>
          <LinkButton href={siteConfig.github} variant="secondary" external>
            View on GitHub
          </LinkButton>
        </div>

        <p className="mt-6 text-xs text-fd-muted-foreground">
          <span className="text-khaos-accent select-none">$ </span>
          go install github.com/{siteConfig.githubOrg}/{siteConfig.repo}
          /cmd/khaos@latest
        </p>
      </div>

      <KafkaPartitions
        topic="user-events"
        rate="khaos run traffic/hot-partition"
        partitions={[
          { name: "partition-0", value: 15200, hot: true },
          { name: "partition-1", value: 2100 },
          { name: "partition-2", value: 1800 },
          { name: "partition-3", value: 2400 },
        ]}
        lag="+42,381"
      />
    </section>
  );
}
