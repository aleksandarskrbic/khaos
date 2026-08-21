/**
 * Redrawn as clean vector strokes for use at any size/theme, in the same
 * spirit as assets/logo.png: a "K" leg fused with a Kafka-style hub-and-spoke
 * node. `currentColor` so it inherits text color everywhere it's placed.
 * Square hub and hard corners to match the Mono Terminal direction.
 */
export function KhaosLogo({ className }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      className={className}
      aria-hidden="true"
    >
      <rect x="5" y="4" width="3" height="16" fill="currentColor" />
      <g stroke="currentColor" strokeWidth="2" strokeLinecap="square">
        <line x1="12.5" y1="12" x2="12.5" y2="7.5" />
        <line x1="12.5" y1="12" x2="12.5" y2="16.5" />
        <line x1="12.5" y1="12" x2="19.5" y2="5.5" />
        <line x1="12.5" y1="12" x2="20.5" y2="12" />
        <line x1="12.5" y1="12" x2="19.5" y2="18.5" />
      </g>
      <rect x="10.6" y="10.1" width="3.8" height="3.8" fill="currentColor" />
    </svg>
  );
}
