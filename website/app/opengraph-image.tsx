import { ImageResponse } from "next/og";
import { ACCENT, BG, FG, FG_DIM } from "@/lib/colors";

export const size = { width: 1200, height: 630 };
export const contentType = "image/png";

export default function Image() {
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
          style={{ display: "flex", alignItems: "center", gap: 16, marginBottom: 40 }}
        >
          <svg width="40" height="40" viewBox="0 0 24 24" fill="none">
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
          <span style={{ fontSize: 36, fontWeight: 600 }}>Khaos</span>
        </div>
        <div style={{ display: "flex", fontSize: 54, fontWeight: 600, maxWidth: 920, lineHeight: 1.15 }}>
          Break Kafka before production does.
        </div>
        <div style={{ display: "flex", fontSize: 25, color: FG_DIM, marginTop: 28, maxWidth: 820 }}>
          Kafka traffic generation, load testing, and chaos engineering.
        </div>
      </div>
    ),
    { ...size },
  );
}
