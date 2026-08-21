import type { BaseLayoutProps } from "fumadocs-ui/layouts/shared";
import { KhaosLogo } from "@/components/kafka/khaos-logo";
import { siteConfig } from "@/lib/site-config";

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: (
        <span className="flex items-center gap-2 font-semibold tracking-tight">
          <KhaosLogo className="size-5" />
          Khaos
        </span>
      ),
    },
    links: [
      {
        text: "Docs",
        url: "/docs",
      },
      {
        text: "Guides",
        url: "/docs/guides/kafka-load-testing",
      },
      {
        text: "GitHub",
        url: siteConfig.github,
        external: true,
      },
    ],
    githubUrl: siteConfig.github,
  };
}
