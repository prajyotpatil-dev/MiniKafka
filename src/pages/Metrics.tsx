import { useState } from "react";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";
import { metricsData } from "../data/mock";

const ranges = ["5m", "15m", "1h"] as const;
type Range = typeof ranges[number];

const Tip = ({ active, payload, label, unit }: any) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background: "var(--panel)", border: "1px solid var(--border)", padding: "6px 10px", fontSize: 10, fontFamily: "'JetBrains Mono', monospace" }}>
      <div style={{ color: "var(--text-3)", marginBottom: 3 }}>{label}</div>
      {payload.map((p: any) => (
        <div key={p.name} style={{ color: "var(--text-1)" }}>{p.value}{unit ?? ""}</div>
      ))}
    </div>
  );
};

interface MiniChart {
  title: string;
  dataKey: string;
  unit?: string;
  color: string;
}

const charts: MiniChart[] = [
  { title: "Published/sec",       dataKey: "published",    unit: "/s",   color: "var(--text-1)" },
  { title: "Consumed/sec",        dataKey: "consumed",     unit: "/s",   color: "var(--text-2)" },
  { title: "Producer Throughput", dataKey: "producerTp",   unit: " KB/s",color: "var(--text-1)" },
  { title: "Consumer Throughput", dataKey: "consumerTp",   unit: " KB/s",color: "var(--text-2)" },
  { title: "Consumer Lag",        dataKey: "lag",          unit: " msgs",color: "var(--warn)" },
  { title: "Active Connections",  dataKey: "connections",  unit: "",     color: "var(--text-2)" },
  { title: "Memory %",            dataKey: "memory",       unit: "%",    color: "var(--text-1)" },
  { title: "CPU %",               dataKey: "cpu",          unit: "%",    color: "var(--err)" },
];

export default function Metrics() {
  const [range, setRange] = useState<Range>("15m");
  const data = metricsData[range] ?? metricsData["15m"];
  const latest = data[data.length - 1] ?? {};

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 28 }}>
      {/* Header row */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", paddingBottom: 16, borderBottom: "1px solid var(--border)" }}>
        <div>
          <div className="label">Metrics</div>
          <div style={{ fontSize: 11, color: "var(--text-3)", marginTop: 3, display: "flex", alignItems: "center", gap: 6 }}>
            <span style={{ width: 5, height: 5, borderRadius: "50%", background: "var(--ok)", display: "inline-block" }} />
            Live · auto-refresh 5s
          </div>
        </div>
        <div style={{ display: "flex", border: "1px solid var(--border)" }}>
          {ranges.map((r, i) => (
            <button
              key={r}
              onClick={() => setRange(r)}
              style={{
                background: range === r ? "var(--panel-raised)" : "transparent",
                border: "none",
                borderRight: i < 2 ? "1px solid var(--border)" : "none",
                color: range === r ? "var(--text-1)" : "var(--text-3)",
                fontFamily: "'JetBrains Mono', monospace",
                fontSize: 11,
                fontWeight: range === r ? 500 : 400,
                padding: "5px 14px",
                cursor: "pointer",
                transition: "color 0.1s",
              }}
            >
              {r}
            </button>
          ))}
        </div>
      </div>

      {/* KPI strip */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", borderTop: "1px solid var(--border)", borderBottom: "1px solid var(--border)" }}>
        {[
          { l: "Published/s",  v: `${latest.published ?? 0}`,  unit: "/s" },
          { l: "Consumed/s",   v: `${latest.consumed ?? 0}`,   unit: "/s" },
          { l: "Memory",       v: `${latest.memory ?? 0}`,     unit: "%" },
          { l: "CPU",          v: `${latest.cpu ?? 0}`,        unit: "%" },
        ].map(({ l, v, unit }, i) => (
          <div
            key={l}
            style={{
              padding: "16px 24px",
              borderRight: i < 3 ? "1px solid var(--border)" : "none",
            }}
          >
            <div className="label" style={{ marginBottom: 6 }}>{l}</div>
            <div style={{ fontSize: 24, fontWeight: 300, color: "var(--text-1)", lineHeight: 1, letterSpacing: "-0.02em" }}>
              {v}<span style={{ fontSize: 12, color: "var(--text-3)", marginLeft: 2 }}>{unit}</span>
            </div>
          </div>
        ))}
      </div>

      {/* Mini chart grid */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: "1px", background: "var(--border)", border: "1px solid var(--border)" }}>
        {charts.map(c => (
          <div key={c.dataKey} style={{ background: "var(--panel)", padding: "16px 16px 12px" }}>
            <div style={{ fontSize: 10, fontWeight: 500, letterSpacing: "0.06em", textTransform: "uppercase", color: "var(--text-3)", marginBottom: 10 }}>
              {c.title}
            </div>
            <ResponsiveContainer width="100%" height={72}>
              <LineChart data={data} margin={{ top: 2, right: 2, left: -40, bottom: 0 }}>
                <CartesianGrid stroke="var(--border-subtle)" strokeDasharray="none" vertical={false} />
                <XAxis dataKey="time" hide />
                <YAxis hide />
                <Tooltip content={<Tip unit={c.unit} />} />
                <Line type="linear" dataKey={c.dataKey} stroke={c.color} strokeWidth={1} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        ))}
      </div>

      {/* Published vs Consumed — full width */}
      <div>
        <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", marginBottom: 12 }}>
          <div className="label">Published vs Consumed</div>
          <div style={{ display: "flex", gap: 20, fontSize: 10, color: "var(--text-3)" }}>
            <span style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <span style={{ width: 16, height: 1, background: "var(--text-1)", display: "inline-block" }} />
              Published
            </span>
            <span style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <span style={{ width: 16, height: 1, background: "var(--text-3)", display: "inline-block", borderTop: "1px dashed var(--text-3)" }} />
              Consumed
            </span>
          </div>
        </div>
        <div style={{ borderTop: "1px solid var(--border)", paddingTop: 16 }}>
          <ResponsiveContainer width="100%" height={140}>
            <LineChart data={data} margin={{ top: 4, right: 4, left: -28, bottom: 0 }}>
              <CartesianGrid stroke="var(--border-subtle)" strokeDasharray="none" vertical={false} />
              <XAxis dataKey="time" tick={{ fill: "var(--text-3)", fontSize: 9, fontFamily: "'JetBrains Mono', monospace" }} axisLine={false} tickLine={false} interval={2} />
              <YAxis tick={{ fill: "var(--text-3)", fontSize: 9, fontFamily: "'JetBrains Mono', monospace" }} axisLine={false} tickLine={false} />
              <Tooltip content={<Tip unit="/s" />} />
              <Line type="linear" dataKey="published" name="Published" stroke="var(--text-1)" strokeWidth={1} dot={false} />
              <Line type="linear" dataKey="consumed" name="Consumed" stroke="var(--text-3)" strokeWidth={1} strokeDasharray="4 3" dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}
