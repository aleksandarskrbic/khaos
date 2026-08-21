import { source } from "@/lib/source";
import {
  DocsPage,
  DocsBody,
  DocsDescription,
  DocsTitle,
} from "fumadocs-ui/page";
import { notFound } from "next/navigation";
import { getMDXComponents } from "@/mdx-components";
import type { Metadata } from "next";
import { siteConfig } from "@/lib/site-config";

export default async function Page(props: {
  params: Promise<{ slug?: string[] }>;
}) {
  const params = await props.params;
  const page = source.getPage(params.slug);
  if (!page) notFound();

  const MDXContent = page.data.body;

  return (
    <DocsPage
      toc={page.data.toc}
      full={page.data.full}
      tableOfContent={{ style: "clerk" }}
    >
      <DocsTitle>{page.data.title}</DocsTitle>
      <DocsDescription>{page.data.description}</DocsDescription>
      <DocsBody>
        <MDXContent components={getMDXComponents()} />
      </DocsBody>
    </DocsPage>
  );
}

export async function generateStaticParams() {
  return source.generateParams();
}

export async function generateMetadata(props: {
  params: Promise<{ slug?: string[] }>;
}): Promise<Metadata> {
  const params = await props.params;
  const page = source.getPage(params.slug);
  if (!page) notFound();

  const url = page.slugs.length
    ? `${siteConfig.url}/docs/${page.slugs.join("/")}`
    : `${siteConfig.url}/docs`;
  const ogSlug = page.slugs.length ? page.slugs.join("/") : "index";
  const image = `${siteConfig.url}/docs-og/${ogSlug}`;

  return {
    title: page.data.title,
    description: page.data.description,
    alternates: {
      canonical: url,
    },
    openGraph: {
      title: page.data.title,
      description: page.data.description,
      url,
      type: "article",
      siteName: siteConfig.name,
      images: [image],
    },
    twitter: {
      card: "summary_large_image",
      title: page.data.title,
      description: page.data.description,
      images: [image],
    },
  };
}
