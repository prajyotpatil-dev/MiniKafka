import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";
import Badge from "../components/Badge";
import { topics, messages, activityData } from "../data/mock";
import type { Page } from "../components/Sidebar";

const stats = [
  { label: "Topics",            value: "5",      sub: "total" },
  { label: "Msgs Published",    value: "24,847", sub: "+284 /min" },
  { label: "Msgs Consumed",     value: "23,191", sub: "+271 /min" },
  { label: "Active Producers",  value: "4",      sub: "3 connected" },
  { label: "Active Consumers",  value: "8",      sub: "1 disconnected" },
  { label: "Consumer Groups",   value: "8",      sub: "1 rebalancing" },
];

const Tip = ({ active, payload, label }: any) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background: "var(--panel)", border: "1px solid var(--border)", padding: "8px 12px", fontSize: 11, fontFamily: "'JetBrains Mono', monospace" }}>
      <div style={{ color: "var(--text-3)", marginBottom: 4 }}>{label}</div>
      {payload.map((p: any) => (
        <div key={p.name} style={{ color: p.color, marginBottom: 2 }}>
          {p.name}: {p.value}
        </div>
      ))}
    </div>
  );
};

interface OverviewProps {
  navigate: (page: Page, params?: Record<string, string>) => void;
}

export default function Overview({ navigate }: OverviewProps) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 32 }}>

      {/* Statistics row */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(6, 1fr)", borderTop: "1px solid var(--border)", borderBottom: "1px solid var(--border)" }}>
        {stats.map(({ label, value, sub }, i) => (
          <div
            key={label}
            style={{
              padding: "20px 24px",
              borderRight: i < 5 ? "1px solid var(--border)" : "none",
            }}
          >
            <div className="label" style={{ marginBottom: 8 }}>{label}</div>
            <div style={{ fontSize: 28, fontWeight: 300, color: "var(--text-1)", lineHeight: 1, letterSpacing: "-0.02em", marginBottom: 6 }}>
              {value}
            </div>
            <div style={{ fontSize: 11, color: "var(--text-3)" }}>{sub}</div>
          </div>
        ))}
      </div>

      {/* Activity chart */}
      <div>
        <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", marginBottom: 16 }}>
          <div>
            <div className="label">Message Activity</div>
            <div style={{ fontSize: 11, color: "var(--text-3)", marginTop: 4 }}>Last 2 hours · 5-minute intervals</div>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 20, fontSize: 11, color: "var(--text-3)" }}>
            <span style={{ display: "flex", alignItems: "center", gap: 6 }}><span style={{ width: 20, height: 1, background: "var(--text-1)", display: "inline-block" }} />Published</span>
            <span style={{ display: "flex", alignItems: "center", gap: 6 }}><span style={{ width: 20, height: 1, background: "var(--text-2)", display: "inline-block", borderTop: "1px dashed var(--text-2)" }} />Consumed</span>
          </div>
        </div>
        <div style={{ borderTop: "1px solid var(--border)", paddingTop: 20 }}>
          <ResponsiveContainer width="100%" height={180}>
            <LineChart data={activityData} margin={{ top: 4, right: 4, left: -28, bottom: 0 }}>
              <CartesianGrid stroke="var(--border-subtle)" strokeDasharray="none" vertical={false} />
              <XAxis
                dataKey="time"
                tick={{ fill: "var(--text-3)", fontSize: 10, fontFamily: "JetBrains Mono" }}
                axisLine={false}
                tickLine={false}
                interval={4}
              />
              <YAxis
                tick={{ fill: "var(--text-3)", fontSize: 10, fontFamily: "JetBrains Mono" }}
                axisLine={false}
                tickLine={false}
              />
              <Tooltip content={<Tip />} />
              <Line type="linear" dataKey="published" name="Published" stroke="var(--text-1)" strokeWidth={1} dot={false} />
              <Line type="linear" dataKey="consumed" name="Consumed" stroke="var(--text-2)" strokeWidth={1} strokeDasharray="4 3" dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Two-column section */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 360px", gap: 32, alignItems: "start" }}>

        {/* Topics table */}
        <div>
          <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", marginBottom: 12 }}>
            <div className="label">Topics Overview</div>
            <button
              onClick={() => navigate("topics")}
              style={{ background: "transparent", border: "none", fontSize: 11, color: "var(--text-3)", cursor: "pointer", fontFamily: "inherit", padding: 0 }}
              onMouseEnter={e => (e.currentTarget as HTMLElement).style.color = "var(--text-1)"}
              onMouseLeave={e => (e.currentTarget as HTMLElement).style.color = "var(--text-3)"}
            >
              View all →
            </button>
          </div>
          <table className="tbl">
            <thead>
              <tr>
                {["Topic", "Msgs", "In Rate", "Status", "Latest"].map(h => <th key={h}>{h}</th>)}
              </tr>
            </thead>
            <tbody>
              {topics.map(t => (
                <tr
                  key={t.id}
                  style={{ cursor: "pointer" }}
                  onClick={() => navigate("topic-detail", { topicId: t.id })}
                >
                  <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11 }}>{t.id}</td>
                  <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>{t.messages.toLocaleString()}</td>
                  <td style={{ textAlign: "right", color: "var(--text-2)", fontVariantNumeric: "tabular-nums" }}>
                    {t.incomingRate > 0 ? `${t.incomingRate}/s` : "—"}
                  </td>
                  <td><Badge status={t.status} /></td>
                  <td style={{ color: "var(--text-3)", fontFamily: "'JetBrains Mono', monospace", fontSize: 10 }}>
                    {t.latestMessage.split(" ")[1]}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Recent messages */}
        <div>
          <div className="label" style={{ marginBottom: 12 }}>Recent Messages</div>
          <div style={{ borderTop: "1px solid var(--border)" }}>
            {messages.slice(0, 7).map(msg => (
              <div
                key={msg.id}
                style={{
                  padding: "9px 0",
                  borderBottom: "1px solid var(--border-subtle)",
                  cursor: "pointer",
                }}
                onClick={() => navigate("messages")}
                onMouseEnter={e => (e.currentTarget as HTMLElement).style.background = "var(--panel-raised)"}
                onMouseLeave={e => (e.currentTarget as HTMLElement).style.background = "transparent"}
              >
                <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 3 }}>
                  <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: "var(--text-2)" }}>{msg.id}</span>
                  <span style={{ fontSize: 10, color: "var(--text-3)", fontFamily: "'JetBrains Mono', monospace" }}>{msg.timestamp.split(" ")[1]}</span>
                </div>
                <div style={{ fontSize: 11, color: "var(--text-3)" }}>{msg.topic} · off:{msg.offset}</div>
              </div>
            ))}
          </div>
        </div>

      </div>
    </div>
  );
}
