import { useState } from "react";
import Badge from "../components/Badge";
import { producers } from "../data/mock";

export default function Producers() {
  const [selected, setSelected] = useState<string | null>(null);
  const prod = producers.find(p => p.id === selected);

  return (
    <div>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", paddingBottom: 16, borderBottom: "1px solid var(--border)", marginBottom: 20 }}>
        <div className="label">Producers</div>
        <div style={{ fontSize: 11, color: "var(--text-3)" }}>
          {producers.filter(p => p.status === "connected").length} connected · {producers.filter(p => p.status === "idle").length} idle
        </div>
      </div>

      <div style={{ display: "flex", gap: 24, alignItems: "flex-start" }}>
        <div style={{ flex: 1, overflow: "auto" }}>
          <table className="tbl" style={{ minWidth: 700 }}>
            <thead>
              <tr>
                <th>Producer ID</th>
                <th>Service</th>
                <th>Topics</th>
                <th style={{ textAlign: "right" }}>Published</th>
                <th style={{ textAlign: "right" }}>Rate</th>
                <th>Last Activity</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {producers.map(p => (
                <tr
                  key={p.id}
                  style={{ cursor: "pointer", background: selected === p.id ? "var(--panel-raised)" : "transparent" }}
                  onClick={() => setSelected(selected === p.id ? null : p.id)}
                >
                  <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-2)" }}>{p.id}</td>
                  <td style={{ fontSize: 12 }}>{p.service}</td>
                  <td>
                    <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
                      {p.topics.map(t => (
                        <span key={t} style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: "var(--text-3)", border: "1px solid var(--border)", padding: "1px 5px" }}>{t}</span>
                      ))}
                    </div>
                  </td>
                  <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>{p.published.toLocaleString()}</td>
                  <td style={{ textAlign: "right", fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-2)" }}>
                    {p.rate > 0 ? `${p.rate}/s` : "—"}
                  </td>
                  <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-3)" }}>
                    {p.lastActivity.split(" ")[1]}
                  </td>
                  <td><Badge status={p.status} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Detail panel */}
        {prod && (
          <div style={{ width: 260, flexShrink: 0, border: "1px solid var(--border)", background: "var(--panel)" }}>
            <div style={{ padding: "12px 16px", borderBottom: "1px solid var(--border)" }}>
              <div className="label">{prod.id}</div>
              <div style={{ fontSize: 14, fontWeight: 400, color: "var(--text-1)", marginTop: 4 }}>{prod.service}</div>
            </div>
            <div style={{ padding: 16 }}>
              <div style={{ marginBottom: 12 }}><Badge status={prod.status} /></div>
              {[
                { l: "Connection",     v: prod.connection },
                { l: "Topics",         v: prod.topics.join(", ") },
                { l: "Total Published",v: prod.published.toLocaleString() },
                { l: "Current Rate",   v: prod.rate > 0 ? `${prod.rate} msg/s` : "idle" },
                { l: "Last Activity",  v: prod.lastActivity },
              ].map(({ l, v }) => (
                <div key={l} style={{ marginBottom: 12 }}>
                  <div className="label" style={{ marginBottom: 3 }}>{l}</div>
                  <div style={{ fontSize: 11, color: "var(--text-1)", fontFamily: "'JetBrains Mono', monospace" }}>{v}</div>
                </div>
              ))}

              {/* Sparkline */}
              <div style={{ marginTop: 16, paddingTop: 16, borderTop: "1px solid var(--border)" }}>
                <div className="label" style={{ marginBottom: 8 }}>Publishing Rate</div>
                <div style={{ display: "flex", alignItems: "flex-end", gap: 1, height: 36 }}>
                  {Array.from({ length: 28 }, (_, i) => {
                    const h = prod.status === "idle" ? 4 : 15 + Math.abs(Math.sin(i * 0.9 + parseInt(prod.id.slice(-1), 10) * 2)) * 60;
                    return (
                      <div
                        key={i}
                        style={{
                          flex: 1,
                          height: `${h}%`,
                          background: prod.status === "idle" ? "var(--border)" : "var(--text-3)",
                        }}
                      />
                    );
                  })}
                </div>
                <div style={{ fontSize: 10, color: "var(--text-3)", marginTop: 4 }}>last 2 hours</div>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
