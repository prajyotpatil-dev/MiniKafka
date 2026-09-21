interface BadgeProps {
  status: string;
  size?: "sm" | "md";
}

const dot: Record<string, string> = {
  active:      "#3fb950",
  connected:   "#3fb950",
  ONLINE:      "#3fb950",
  HEALTHY:     "#3fb950",
  Stable:      "#3fb950",
  INFO:        "#58a6ff",
  idle:        "#444",
  Empty:       "#444",
  disconnected:"#444",
  OFFLINE:     "#444",
  warning:     "#d29922",
  lagging:     "#d29922",
  Rebalancing: "#d29922",
  DEGRADED:    "#d29922",
  WARN:        "#d29922",
  error:       "#f85149",
  ERROR:       "#f85149",
  DEBUG:       "#8b949e",
};

const text: Record<string, string> = {
  active:      "#3fb950",
  connected:   "#3fb950",
  ONLINE:      "#3fb950",
  HEALTHY:     "#3fb950",
  Stable:      "#3fb950",
  INFO:        "#58a6ff",
  idle:        "#555",
  Empty:       "#555",
  disconnected:"#555",
  OFFLINE:     "#555",
  warning:     "#d29922",
  lagging:     "#d29922",
  Rebalancing: "#d29922",
  DEGRADED:    "#d29922",
  WARN:        "#d29922",
  error:       "#f85149",
  ERROR:       "#f85149",
  DEBUG:       "#8b949e",
};

export default function Badge({ status }: BadgeProps) {
  const d = dot[status] ?? "#444";
  const t = text[status] ?? "#555";
  return (
    <span className="inline-flex items-center gap-1.5" style={{ fontFamily: "inherit", fontSize: 11, color: t }}>
      <span
        style={{
          display: "inline-block",
          width: 5,
          height: 5,
          borderRadius: "50%",
          background: d,
          flexShrink: 0,
        }}
      />
      {status}
    </span>
  );
}
