import { LandingHeader } from "@/components/landing/header";
import { Hero } from "@/components/landing/hero";
import { Capabilities } from "@/components/landing/capabilities";
import { LagDemo } from "@/components/landing/lag-demo";
import { GoRewriteStrip } from "@/components/landing/go-rewrite";
import { LandingFooter } from "@/components/landing/footer";
import { siteConfig } from "@/lib/site-config";

const jsonLd = {
  "@context": "https://schema.org",
  "@type": "SoftwareApplication",
  name: "Khaos",
  applicationCategory: "DeveloperApplication",
  operatingSystem: "Linux, macOS, Windows",
  description: siteConfig.description,
  url: siteConfig.url,
  softwareHelp: `${siteConfig.url}/docs`,
  codeRepository: siteConfig.github,
  license: "https://www.apache.org/licenses/LICENSE-2.0",
  offers: {
    "@type": "Offer",
    price: "0",
    priceCurrency: "USD",
  },
};

export default function Home() {
  return (
    <main className="flex flex-1 flex-col">
      <script
        type="application/ld+json"
        dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd) }}
      />
      <LandingHeader />
      <Hero />
      <Capabilities />
      <LagDemo />
      <GoRewriteStrip />
      <LandingFooter />
    </main>
  );
}
