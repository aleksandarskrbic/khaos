import { cn } from "@/lib/cn";
import { CopyButton } from "@/components/ui/copy-button";

export type PartitionLoad = { name: string; value: number; hot?: boolean };

/**
 * A topology + histogram diagram: khaos -> topic -> partitions -> per-partition
 * share. Used on the landing hero and in hot-partition / traffic guides.
 */
export function KafkaPartitions({
  topic,
  partitions,
  rate,
  lag,
}: {
  topic: string;
  partitions: PartitionLoad[];
  rate?: string;
  lag?: string;
}) {
  const max = Math.max(...partitions.map((p) => p.value));
  const total = partitions.reduce((sum, p) => sum + p.value, 0);
  const n = partitions.length;

  const chipW = 40;
  const chipGap = 8;
  const chipsW = n * chipW + (n - 1) * chipGap;
  const topicPad = 14;
  const topicW = Math.max(180, chipsW + topicPad * 2);
  const topicX = 130;
  const clientW = 92;
  const clientX = 4;
  const svgW = topicX + topicW + 6;
  const svgH = 108;
  const chipsStartX = topicX + (topicW - chipsW) / 2;

  return (
    <div className="min-w-0 border border-khaos-border-strong bg-fd-muted">
      <div className="flex min-w-0 items-center justify-between gap-2 border-b border-fd-border py-1.5 pr-1 pl-3.5 text-xs text-fd-muted-foreground">
        <span className="flex min-w-0 items-center gap-2">
          <span className="shrink-0 font-bold text-khaos-accent">$</span>
          <span className="truncate">
            khaos topic describe {topic} --hot-partitions
          </span>
        </span>
        <CopyButton text={`khaos topic describe ${topic} --hot-partitions`} />
      </div>

      {rate && (
        <div className="flex min-w-0 items-center gap-2 border-b border-fd-border px-3.5 py-2 text-xs text-fd-muted-foreground">
          <span className="shrink-0 text-khaos-accent">khaos</span>
          <span className="shrink-0">&rarr;</span>
          <span className="truncate">{rate}</span>
        </div>
      )}

      <div className="overflow-x-auto px-4 pt-5 pb-1">
        <svg
          viewBox={`0 0 ${svgW} ${svgH}`}
          className="mx-auto block h-auto w-full max-w-[420px]"
          role="img"
          aria-label={`Topology: khaos producing to topic ${topic} across ${n} partitions`}
          style={{
            fontFamily:
              'var(--font-mono), ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace',
          }}
        >
          <defs>
            <marker
              id="kp-arrow"
              markerWidth="8"
              markerHeight="8"
              refX="6"
              refY="4"
              orient="auto"
            >
              <path d="M0,0 L8,4 L0,8 Z" fill="var(--color-fd-muted-foreground)" />
            </marker>
          </defs>

          <rect
            x={clientX}
            y="24"
            width={clientW}
            height="58"
            fill="none"
            stroke="var(--color-khaos-border-strong)"
          />
          <text
            x={clientX + clientW / 2}
            y="49"
            textAnchor="middle"
            fontSize="10.5"
            fill="var(--color-fd-muted-foreground)"
            letterSpacing="0.07em"
          >
            CLIENT
          </text>
          <text
            x={clientX + clientW / 2}
            y="68"
            textAnchor="middle"
            fontSize="14.5"
            fontWeight="700"
            fill="var(--color-fd-foreground)"
          >
            khaos
          </text>

          <line
            x1={clientX + clientW}
            y1="53"
            x2={topicX - 6}
            y2="53"
            stroke="var(--color-fd-muted-foreground)"
            markerEnd="url(#kp-arrow)"
          />

          <rect
            x={topicX}
            y="4"
            width={topicW}
            height="100"
            fill="none"
            stroke="var(--color-khaos-border-strong)"
          />
          <text
            x={topicX + topicW / 2}
            y="26"
            textAnchor="middle"
            fontSize="10"
            fill="var(--color-fd-muted-foreground)"
            letterSpacing="0.07em"
          >
            TOPIC
          </text>
          <text
            x={topicX + topicW / 2}
            y="46"
            textAnchor="middle"
            fontSize="14.5"
            fontWeight="700"
            fill="var(--color-fd-foreground)"
          >
            {topic}
          </text>
          <line
            x1={topicX + 14}
            y1="58"
            x2={topicX + topicW - 14}
            y2="58"
            stroke="var(--color-fd-border)"
          />
          <text
            x={topicX + topicW / 2}
            y="76"
            textAnchor="middle"
            fontSize="10"
            fill="var(--color-khaos-faint)"
          >
            {n} partitions
          </text>

          {partitions.map((p, i) => {
            const x = chipsStartX + i * (chipW + chipGap);
            const isHot = !!p.hot;
            return (
              <g key={p.name} fontSize="10.5" fontWeight="700">
                <rect
                  x={x}
                  y="84"
                  width={chipW}
                  height="18"
                  fill="none"
                  stroke={
                    isHot
                      ? "var(--color-khaos-accent)"
                      : "var(--color-khaos-border-strong)"
                  }
                  strokeWidth={isHot ? 1.4 : 1}
                />
                <text
                  x={x + chipW / 2}
                  y="97"
                  textAnchor="middle"
                  fill={
                    isHot
                      ? "var(--color-khaos-accent)"
                      : "var(--color-fd-muted-foreground)"
                  }
                >
                  P{i}
                </text>
              </g>
            );
          })}
        </svg>
      </div>

      <div className="px-4" aria-hidden="true">
        <svg
          viewBox="0 0 100 20"
          preserveAspectRatio="none"
          className="block h-5 w-full"
        >
          <line
            x1="50"
            y1="0"
            x2="50"
            y2="13"
            stroke="var(--color-fd-muted-foreground)"
            strokeWidth="0.6"
            vectorEffect="non-scaling-stroke"
          />
          <path
            d="M46,11 L50,17 L54,11"
            fill="none"
            stroke="var(--color-fd-muted-foreground)"
            strokeWidth="0.6"
            vectorEffect="non-scaling-stroke"
          />
        </svg>
      </div>

      <div className="px-4 pb-1">
        <div className="grid grid-cols-[38px_1fr_64px_46px] gap-2 pb-2 text-[10px] tracking-wide text-khaos-faint sm:grid-cols-[46px_1fr_78px_56px] sm:gap-2.5">
          <span>PART</span>
          <span>DISTRIBUTION</span>
          <span className="text-right tabular-nums">MSGS</span>
          <span className="text-right tabular-nums">SHARE</span>
        </div>

        {partitions.map((p, i) => {
          const isHot = !!p.hot;
          const pct = max > 0 ? (p.value / max) * 100 : 0;
          const share = total > 0 ? (p.value / total) * 100 : 0;
          return (
            <div
              key={p.name}
              className="grid grid-cols-[38px_1fr_64px_46px] items-center gap-2 border-t border-fd-border py-2 sm:grid-cols-[46px_1fr_78px_56px] sm:gap-2.5"
            >
              <div className="flex flex-col gap-1">
                <span
                  className={cn(
                    "flex h-[22px] w-8 items-center justify-center border text-[13px] font-bold",
                    isHot
                      ? "border-khaos-accent text-khaos-accent"
                      : "border-khaos-border-strong text-fd-muted-foreground",
                  )}
                >
                  P{i}
                </span>
                {isHot && (
                  <span className="w-max border border-khaos-danger px-1 py-px text-[9px] font-bold tracking-wide text-khaos-danger">
                    HOT
                  </span>
                )}
              </div>

              <svg
                viewBox="0 0 300 16"
                preserveAspectRatio="none"
                className="block h-4 w-full"
                aria-hidden="true"
              >
                <rect
                  x="0.5"
                  y="0.5"
                  width="299"
                  height="15"
                  fill="none"
                  stroke="var(--color-fd-border)"
                  vectorEffect="non-scaling-stroke"
                />
                <line
                  x1="75"
                  y1="0"
                  x2="75"
                  y2="16"
                  stroke="var(--color-fd-border)"
                  vectorEffect="non-scaling-stroke"
                />
                <line
                  x1="150"
                  y1="0"
                  x2="150"
                  y2="16"
                  stroke="var(--color-fd-border)"
                  vectorEffect="non-scaling-stroke"
                />
                <line
                  x1="225"
                  y1="0"
                  x2="225"
                  y2="16"
                  stroke="var(--color-fd-border)"
                  vectorEffect="non-scaling-stroke"
                />
                <rect
                  x="0.5"
                  y="2.5"
                  width={Math.max(1, (pct / 100) * 298)}
                  height="11"
                  fill={
                    isHot
                      ? "var(--color-khaos-accent)"
                      : "var(--color-khaos-faint)"
                  }
                />
              </svg>

              <span
                className={cn(
                  "text-right text-[13px] tabular-nums sm:text-[13.5px]",
                  isHot ? "font-bold text-fd-foreground" : "text-fd-foreground",
                )}
              >
                {p.value.toLocaleString()}
              </span>
              <span
                className={cn(
                  "text-right text-xs tabular-nums",
                  isHot ? "font-bold text-khaos-accent" : "text-fd-muted-foreground",
                )}
              >
                {share.toFixed(1)}%
              </span>
            </div>
          );
        })}
      </div>

      {lag && (
        <div className="flex items-center justify-between border-t border-fd-border px-4 py-3 text-xs">
          <span className="text-fd-muted-foreground">consumer lag</span>
          <span className="font-bold text-khaos-accent">{lag}</span>
        </div>
      )}
    </div>
  );
}
