import { ConsumerLag } from "@/components/kafka/consumer-lag";
import { Terminal } from "@/components/kafka/terminal";

export function LagDemo() {
  return (
    <section className="mx-auto max-w-6xl px-6 py-16">
      <div className="grid gap-10 lg:grid-cols-2 lg:items-center">
        <div className="min-w-0">
          <h2 className="text-2xl leading-tight font-bold tracking-tight text-fd-foreground">
            Reproduce consumer lag on purpose
          </h2>
          <p className="mt-4 max-w-md leading-relaxed text-fd-muted-foreground">
            Set a producer rate your consumers can&apos;t keep up with, or add
            artificial per-message processing delay, and watch lag grow,
            self-reported from Khaos&apos;s own counters, or measured directly
            from broker offsets with{" "}
            <code className="rounded bg-fd-muted px-1 py-0.5 text-sm">
              --lag-poll
            </code>
            .
          </p>
          <div className="mt-6">
            <Terminal
              lines={[
                { type: "command", text: "khaos run traffic/consumer-lag --lag-poll 5s" },
              ]}
            />
          </div>
        </div>

        <div className="min-w-0">
          <ConsumerLag
            produceRate="2,000 msg/s"
            consumeRate="~1,100 msg/s"
            trend={["0", "4,200", "9,800", "16,300", "24,900"]}
          />
        </div>
      </div>
    </section>
  );
}
