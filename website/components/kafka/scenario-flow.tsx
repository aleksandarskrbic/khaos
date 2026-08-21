import { CopyButton } from "@/components/ui/copy-button";

export type FlowStep = {
  topic: string;
  eventType: string;
  delayMs?: number;
};

/**
 * Visualizes a correlated flow's steps in order: the shape of a `flows:`
 * entry in a scenario file (topic -> topic -> topic, each with a delay).
 */
export function ScenarioFlow({
  name,
  steps,
}: {
  name: string;
  steps: FlowStep[];
}) {
  return (
    <div className="min-w-0 border border-khaos-border-strong bg-fd-muted">
      <div className="flex items-center justify-between gap-2 border-b border-fd-border py-1.5 pr-1 pl-3.5 text-xs text-fd-muted-foreground">
        <span className="flex items-center gap-2">
          <span className="font-bold text-khaos-accent">$</span>
          <span>khaos flow describe {name}</span>
        </span>
        <CopyButton text={`khaos flow describe ${name}`} />
      </div>

      <div className="flex flex-wrap items-stretch gap-y-3 overflow-x-auto p-4">
        {steps.map((step, i) => (
          <div key={i} className="flex items-stretch">
            <div className="flex flex-col justify-center border border-khaos-border-strong bg-fd-background px-3 py-2">
              <span className="text-xs font-bold text-fd-foreground">
                {step.topic}
              </span>
              <span className="text-[11px] text-fd-muted-foreground">
                {step.eventType}
              </span>
            </div>
            {i < steps.length - 1 && (
              <div className="flex flex-col items-center justify-center px-2">
                <span className="text-[10px] tabular-nums text-khaos-faint">
                  {step.delayMs ? `+${step.delayMs}ms` : ""}
                </span>
                <span className="text-fd-muted-foreground" aria-hidden="true">
                  &rarr;
                </span>
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
