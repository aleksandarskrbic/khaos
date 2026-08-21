import { ImageResponse } from "next/og";
import { source } from "@/lib/source";
import { ACCENT, BG, FG, FG_DIM } from "@/lib/colors";

export const runtime = "nodejs";

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ slug: string[] }> },
) {
  const { slug } = await params;
  const isIndex = slug.length === 1 && slug[0] === "index";
  const page = source.getPage(isIndex ? [] : slug);
  const title = page?.data.title ?? "Documentation";
  const description = page?.data.description ?? "";

  return new ImageResponse(
    (
      <div
        style={{
          width: "100%",
          height: "100%",
          display: "flex",
          flexDirection: "column",
          justifyContent: "center",
          padding: "80px",
          background: BG,
          color: FG,
          fontFamily: "monospace",
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 12,
            marginBottom: 48,
            color: FG_DIM,
            fontSize: 26,
          }}
        >
          <svg width="28" height="28" viewBox="0 0 24 24" fill="none">
            <rect x="5" y="4" width="3" height="16" fill={ACCENT} />
            <g stroke={ACCENT} strokeWidth="2" strokeLinecap="square">
              <line x1="12.5" y1="12" x2="12.5" y2="7.5" />
              <line x1="12.5" y1="12" x2="12.5" y2="16.5" />
              <line x1="12.5" y1="12" x2="19.5" y2="5.5" />
              <line x1="12.5" y1="12" x2="20.5" y2="12" />
              <line x1="12.5" y1="12" x2="19.5" y2="18.5" />
            </g>
            <rect x="10.6" y="10.1" width="3.8" height="3.8" fill={ACCENT} />
          </svg>
          <span>Khaos Docs</span>
        </div>
        <div
          style={{
            display: "flex",
            fontSize: 54,
            fontWeight: 600,
            maxWidth: 980,
            lineHeight: 1.15,
          }}
        >
          {title}
        </div>
        {description && (
          <div
            style={{
              display: "flex",
              fontSize: 24,
              color: FG_DIM,
              marginTop: 24,
              maxWidth: 900,
              lineHeight: 1.4,
            }}
          >
            {description.slice(0, 140)}
          </div>
        )}
      </div>
    ),
    { width: 1200, height: 630 },
  );
}
