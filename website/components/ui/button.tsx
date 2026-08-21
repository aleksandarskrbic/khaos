import Link from "next/link";
import type { ReactNode } from "react";
import { cn } from "@/lib/cn";

export function LinkButton({
  href,
  children,
  variant = "primary",
  external,
  className,
}: {
  href: string;
  children: ReactNode;
  variant?: "primary" | "secondary";
  external?: boolean;
  className?: string;
}) {
  return (
    <Link
      href={href}
      target={external ? "_blank" : undefined}
      rel={external ? "noopener noreferrer" : undefined}
      className={cn(
        "inline-flex h-10 items-center justify-center gap-2 px-4 text-sm font-semibold transition-colors",
        variant === "primary" &&
          "bg-khaos-accent text-khaos-accent-foreground hover:bg-khaos-accent/90",
        variant === "secondary" &&
          "border border-fd-border text-fd-foreground hover:bg-fd-accent",
        className,
      )}
    >
      {children}
    </Link>
  );
}
