import { cn } from "@/lib/cn";
import { CopyButton } from "@/components/ui/copy-button";

type Line =
  | { type: "command"; text: string }
  | { type: "output"; text: string }
  | { type: "comment"; text: string };

export function Terminal({
  title = "khaos",
  lines,
  className,
}: {
  title?: string;
  lines: Line[];
  className?: string;
}) {
  const copyText = lines
    .filter((l) => l.type === "command")
    .map((l) => l.text)
    .join("\n");

  return (
    <div
      className={cn(
        "min-w-0 overflow-hidden border border-khaos-border-strong bg-fd-muted",
        className,
      )}
    >
      <div className="flex items-center justify-between gap-2 border-b border-fd-border pr-1 pl-4 py-1.5">
        <span className="flex items-center gap-2 text-xs text-fd-muted-foreground">
          <span className="size-1.5 bg-fd-muted-foreground/40" />
          {title}
        </span>
        {copyText && <CopyButton text={copyText} />}
      </div>
      <pre className="overflow-x-auto px-4 py-4 text-[13px] leading-6">
        <code>
          {lines.map((line, i) => (
            <div key={i}>
              {line.type === "command" && (
                <span>
                  <span className="font-bold text-khaos-accent select-none">
                    ${" "}
                  </span>
                  <span className="text-fd-foreground">{line.text}</span>
                </span>
              )}
              {line.type === "output" && (
                <span className="text-fd-muted-foreground">{line.text}</span>
              )}
              {line.type === "comment" && (
                <span className="text-khaos-faint"># {line.text}</span>
              )}
              {line.text === "" && " "}
            </div>
          ))}
        </code>
      </pre>
    </div>
  );
}
