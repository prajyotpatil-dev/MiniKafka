import { useState } from "react";
import Badge from "../components/Badge";
import { consumers } from "../data/mock";

function LagBar({ lag, latestOffset }: { lag: number; latestOffset: number }) {
  if (latestOffset === 0) return <span style={{ color: "var(--text-3)", fontFamily: "'JetBrains Mono', monospace", fontSize: 11 }}>—</span>;
  const pct = Math.min((lag / latestOffset) * 100, 100);
  const color = pct === 0 ? "var(--ok)" : pct < 10 ? "var(--ok)" : pct < 50 ? "var(--warn)" : "var(--err)";
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
      <div style={{ width: 60, height: 2, background: "var(--border)" }}>
        <div style={{ height: "100%", width: `${Math.max(pct > 0 ? 4 : 0, pct)}%`, background: color, maxWidth: "100%" }} />
      </div>
      <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color, minWidth: 28, textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
        {lag.toLocaleString()}
      </span>
    </div>
  );
}

export default function Consumers() {
  const [filter, setFilter] = useState("all");
  const lagging = consumers.filter(c => c.status === "lagging").length;
  const disconnected = consumers.filter(c => c.status === "disconnected").length;

  const filtered = consumers.filter(c => filter === "all" || c.status === filter);

  return (
    <div>
      {/* Alerts */}
      {(lagging > 0 || disconnected > 0) && (
        <div style={{ marginBottom: 20, display: "flex", flexDirection: "column", gap: 6 }}>
          {lagging > 0 && (
            <div style={{ padding: "8px 14px", borderLeft: "2px solid var(--warn)", background: "var(--panel)", fontSize: 11, color: "var(--warn)", display: "flex", alignItems: "center", gap: 8 }}>
              <span>Warning</span>
              <span style={{ color: "var(--text-3)" }}>—</span>
              <span style={{ color: "var(--text-2)" }}>{lagging} consumer(s) are falling behind. Consumer lag exceeds threshold.</span>
            </div>
          )}
          {disconnected > 0 && (
            <div style={{ padding: "8px 14px", borderLeft: "2px solid var(--err)", background: "var(--panel)", fontSize: 11, color: "var(--err)", display: "flex", alignItems: "center", gap: 8 }}>
              <span>Error</span>
              <span style={{ color: "var(--text-3)" }}>—</span>
              <span style={{ color: "var(--text-2)" }}>{disconnected} consumer(s) are disconnected and not consuming.</span>
            </div>
          )}
        </div>
      )}

      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", paddingBottom: 16, borderBottom: "1px solid var(--border)", marginBottom: 20 }}>
        <div className="label">Consumers</div>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <select
            value={filter}
            onChange={e => setFilter(e.target.value)}
            className="inp"
            style={{ cursor: "pointer", fontSize: 11 }}
          >
            <option value="all">All statuses</option>
            <option value="active">Active</option>
            <option value="lagging">Lagging</option>
            <option value="idle">Idle</option>
            <option value="disconnected">Disconnected</option>
          </select>
          <span style={{ fontSize: 11, color: "var(--text-3)" }}>{filtered.length} consumers</span>
        </div>
      </div>

      <div style={{ overflow: "auto" }}>
        <table className="tbl" style={{ minWidth: 920 }}>
          <thead>
            <tr>
              <th>Consumer ID</th>
              <th>Service</th>
              <th>Topic</th>
              <th>Group</th>
              <th style={{ textAlign: "right" }}>Cur. Offset</th>
              <th style={{ textAlign: "right" }}>Latest</th>
              <th>Lag</th>
              <th style={{ textAlign: "right" }}>Consumed</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map(c => (
              <tr key={c.id}>
                <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-2)" }}>{c.id}</td>
                <td style={{ fontSize: 12 }}>{c.service}</td>
                <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-2)" }}>{c.topic}</td>
                <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-3)" }}>{c.group}</td>
                <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums", fontFamily: "'JetBrains Mono', monospace", fontSize: 11 }}>{c.currentOffset.toLocaleString()}</td>
                <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums", fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-2)" }}>{c.latestOffset.toLocaleString()}</td>
                <td><LagBar lag={c.lag} latestOffset={c.latestOffset} /></td>
                <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>{c.consumed.toLocaleString()}</td>
                <td><Badge status={c.status} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div style={{ marginTop: 16, display: "flex", gap: 24, fontSize: 10, color: "var(--text-3)" }}>
        {[
          { dot: "var(--ok)",   label: "Low lag — healthy" },
          { dot: "var(--warn)", label: "Medium lag — warning" },
          { dot: "var(--err)",  label: "High lag — critical" },
        ].map(({ dot, label }) => (
          <span key={label} style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <span style={{ width: 5, height: 5, borderRadius: "50%", background: dot, display: "inline-block" }} />
            {label}
          </span>
        ))}
      </div>
    </div>
  );
}
