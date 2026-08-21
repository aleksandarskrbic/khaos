"use client";

import { useState } from "react";
import { Copy, Check } from "lucide-react";
import { cn } from "@/lib/cn";

export function CopyButton({
  text,
  className,
}: {
  text: string;
  className?: string;
}) {
  const [copied, setCopied] = useState(false);

  return (
    <button
      type="button"
      aria-label={copied ? "Copied" : "Copy to clipboard"}
      onClick={() => {
        navigator.clipboard.writeText(text);
        setCopied(true);
        setTimeout(() => setCopied(false), 1500);
      }}
      className={cn(
        "inline-flex size-6 shrink-0 items-center justify-center border border-transparent text-fd-muted-foreground transition-colors hover:border-khaos-border-strong hover:text-fd-foreground",
        className,
      )}
    >
      {copied ? (
        <Check className="size-3.5 text-khaos-accent" />
      ) : (
        <Copy className="size-3.5" />
      )}
    </button>
  );
}
