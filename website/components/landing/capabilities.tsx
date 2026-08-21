import type { SVGProps } from "react";

function WaveformIcon(props: SVGProps<SVGSVGElement>) {
  return (
    <svg viewBox="0 0 20 20" fill="none" aria-hidden="true" {...props}>
      <polyline
        points="1.5,10 4.5,10 7,4 10,16 13,3 15.5,10 18.5,10"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="square"
        strokeLinejoin="miter"
      />
    </svg>
  );
}

function RampIcon(props: SVGProps<SVGSVGElement>) {
  return (
    <svg viewBox="0 0 20 20" fill="none" aria-hidden="true" {...props}>
      <rect x="2.5" y="12" width="3.5" height="5.5" fill="currentColor" />
      <rect x="8.25" y="8" width="3.5" height="9.5" fill="currentColor" />
      <rect x="14" y="2.5" width="3.5" height="15" fill="currentColor" />
    </svg>
  );
}

function FaultIcon(props: SVGProps<SVGSVGElement>) {
  return (
    <svg viewBox="0 0 20 20" fill="none" aria-hidden="true" {...props}>
      <g stroke="currentColor" strokeWidth="1.5" strokeLinecap="square">
        <line x1="1.5" y1="10" x2="6.5" y2="10" />
        <line x1="8" y1="3.5" x2="5" y2="11" />
        <line x1="12" y1="8.5" x2="9" y2="16.5" />
        <line x1="13.5" y1="10" x2="18.5" y2="10" />
      </g>
    </svg>
  );
}

function LayersIcon(props: SVGProps<SVGSVGElement>) {
  return (
    <svg viewBox="0 0 20 20" fill="none" aria-hidden="true" {...props}>
      <rect x="2.5" y="2.5" width="15" height="3.5" fill="currentColor" />
      <rect x="2.5" y="8.25" width="15" height="3.5" fill="currentColor" />
      <rect x="2.5" y="14" width="15" height="3.5" fill="currentColor" />
    </svg>
  );
}

const CAPABILITIES = [
  {
    n: "01",
    title: "Traffic generation",
    icon: WaveformIcon,
    body: "Generate realistic Kafka records and configurable producer traffic: structured fields, faker-backed data, JSON/Avro/Protobuf, and Schema Registry integration.",
  },
  {
    n: "02",
    title: "Load testing",
    icon: RampIcon,
    body: "Push Kafka clusters, producers, consumers, and downstream processing systems under sustained, configurable load with tunable batching, compression, and acknowledgements.",
  },
  {
    n: "03",
    title: "Failure simulation",
    icon: FaultIcon,
    body: "Reproduce consumer lag, hot partitions, rebalances, broker failures, and other operating conditions that surface real bugs before they reach production.",
  },
  {
    n: "04",
    title: "Production-like testing",
    icon: LayersIcon,
    body: "Compose scenarios in YAML (topics, correlated event flows, and scheduled incidents) into repeatable tests that resemble actual production workloads.",
  },
];

export function Capabilities() {
  return (
    <section className="mx-auto max-w-6xl px-6 py-16">
      <h2 className="text-sm font-medium tracking-widest text-fd-muted-foreground uppercase">
        What Khaos does
      </h2>
      <div className="mt-6 divide-y divide-fd-border border-t border-fd-border">
        {CAPABILITIES.map((c) => (
          <div
            key={c.n}
            className="grid gap-2 py-6 sm:grid-cols-[3rem_12rem_1fr] sm:items-baseline sm:gap-6"
          >
            <span className="text-sm text-khaos-accent">{c.n}</span>
            <h3 className="flex items-center gap-2 text-base font-medium text-fd-foreground">
              <c.icon className="size-4 shrink-0 text-khaos-accent" />
              {c.title}
            </h3>
            <p className="max-w-2xl text-sm leading-relaxed text-fd-muted-foreground">
              {c.body}
            </p>
          </div>
        ))}
      </div>
    </section>
  );
}
