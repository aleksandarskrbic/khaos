import { cn } from "@/lib/cn";
import { CopyButton } from "@/components/ui/copy-button";

/**
 * Producer vs. consumer rate, plus a sparkline of the resulting lag trend.
 * Mirrors what `--lag-poll` reports as LAG(BROKER) in the TUI.
 */
export function ConsumerLag({
  produceRate,
  consumeRate,
  trend,
}: {
  produceRate: string;
  consumeRate: string;
  trend: string[];
}) {
  const values = trend.map((v) => Number(v.replace(/[^0-9.-]/g, "")) || 0);
  const max = Math.max(1, ...values);
  const w = 280;
  const h = 56;
  const pad = 4;
  const points = values.map((v, i) => {
    const x = values.length > 1 ? (i / (values.length - 1)) * (w - pad * 2) + pad : pad;
    const y = h - pad - (v / max) * (h - pad * 2);
    return [x, y] as const;
  });
  const path = points.map(([x, y], i) => `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`).join(" ");
  const areaPath = `${path} L${points[points.length - 1][0].toFixed(1)},${h - pad} L${pad},${h - pad} Z`;

  return (
    <div className="min-w-0 border border-khaos-border-strong bg-fd-muted">
      <div className="flex items-center justify-between gap-2 border-b border-fd-border py-1.5 pr-1 pl-3.5 text-xs text-fd-muted-foreground">
        <span className="flex items-center gap-2">
          <span className="font-bold text-khaos-accent">$</span>
          <span>khaos run --lag-poll 5s</span>
        </span>
        <CopyButton text="khaos run --lag-poll 5s" />
      </div>

      <div className="grid min-w-0 grid-cols-[1fr_auto_1fr] items-stretch gap-2 p-4 text-xs">
        <div className="border border-khaos-border-strong px-3 py-2.5 text-center">
          <div className="text-[10px] tracking-wide text-khaos-faint">PRODUCER</div>
          <div className="mt-1 font-bold tabular-nums text-fd-foreground">
            {produceRate}
          </div>
        </div>
        <div className="flex flex-col items-center justify-center gap-0.5 px-1 text-khaos-faint">
          <span className="text-[10px]">topic</span>
          <span aria-hidden="true">&rarr;</span>
        </div>
        <div className="border border-khaos-border-strong px-3 py-2.5 text-center">
          <div className="text-[10px] tracking-wide text-khaos-faint">CONSUMER</div>
          <div className="mt-1 font-bold tabular-nums text-fd-foreground">
            {consumeRate}
          </div>
        </div>
      </div>

      <div className="border-t border-fd-border px-4 pt-3 pb-4">
        <div className="mb-2 text-[10px] tracking-wide text-khaos-faint">
          LAG OVER TIME
        </div>
        <svg
          viewBox={`0 0 ${w} ${h}`}
          preserveAspectRatio="none"
          className="block h-14 w-full"
          role="img"
          aria-label={`Lag growing from ${trend[0]} to ${trend[trend.length - 1]}`}
        >
          <line x1="0" y1={h - pad} x2={w} y2={h - pad} stroke="var(--color-fd-border)" vectorEffect="non-scaling-stroke" />
          <path d={areaPath} fill="var(--color-khaos-accent)" opacity="0.12" />
          <path d={path} fill="none" stroke="var(--color-khaos-accent)" strokeWidth="1.5" vectorEffect="non-scaling-stroke" />
          {points.map(([x, y], i) => (
            <circle
              key={i}
              cx={x}
              cy={y}
              r={i === points.length - 1 ? 3 : 1.6}
              fill={i === points.length - 1 ? "var(--color-khaos-accent)" : "var(--color-fd-muted-foreground)"}
            />
          ))}
        </svg>

        <div className="mt-2 flex flex-wrap items-center gap-x-2 gap-y-1 text-sm">
          {trend.map((v, i) => (
            <span key={i} className="flex items-center gap-2">
              <span
                className={cn(
                  "tabular-nums",
                  i === trend.length - 1
                    ? "font-bold text-khaos-accent"
                    : "text-fd-muted-foreground",
                )}
              >
                {v}
              </span>
              {i < trend.length - 1 && (
                <span className="text-khaos-faint" aria-hidden="true">
                  &rarr;
                </span>
              )}
            </span>
          ))}
        </div>
      </div>
    </div>
  );
}
