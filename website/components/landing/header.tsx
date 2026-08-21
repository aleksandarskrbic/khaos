import Link from "next/link";
import { KhaosLogo } from "@/components/kafka/khaos-logo";
import { ThemeToggle } from "@/components/landing/theme-toggle";
import { siteConfig } from "@/lib/site-config";

export function LandingHeader() {
  return (
    <>
      <div className="border-b border-fd-border">
        <div className="mx-auto flex max-w-6xl items-center gap-2 px-6 py-1.5 text-[11px] tracking-wide text-fd-muted-foreground">
          <span className="size-1.5 shrink-0 bg-khaos-accent" aria-hidden="true" />
          <span className="truncate">khaos &middot; kafka chaos & load</span>
        </div>
      </div>
      <header className="mx-auto flex max-w-6xl items-center justify-between px-6 py-5">
        <Link
          href="/"
          className="flex items-center gap-2 font-semibold tracking-tight text-fd-foreground"
        >
          <KhaosLogo className="size-5 text-khaos-accent" />
          Khaos
        </Link>
        <nav className="flex items-center gap-1 text-sm">
          <Link
            href="/docs"
            className="rounded-md px-3 py-2 text-fd-muted-foreground hover:text-fd-foreground"
          >
            Docs
          </Link>
          <a
            href={siteConfig.github}
            target="_blank"
            rel="noopener noreferrer"
            className="rounded-md px-3 py-2 text-fd-muted-foreground hover:text-fd-foreground"
          >
            GitHub
          </a>
          <ThemeToggle />
        </nav>
      </header>
    </>
  );
}
