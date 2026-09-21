import { useState } from "react";
import { ArrowLeft } from "lucide-react";
import Badge from "../components/Badge";
import { consumerGroups, consumers } from "../data/mock";
import type { ConsumerGroup } from "../data/mock";

function LagValue({ lag }: { lag: number }) {
  const color = lag === 0 ? "var(--ok)" : lag < 20 ? "var(--ok)" : lag < 100 ? "var(--warn)" : "var(--err)";
  return (
    <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color, fontVariantNumeric: "tabular-nums" }}>
      {lag.toLocaleString()}
    </span>
  );
}

export default function ConsumerGroups() {
  const [selected, setSelected] = useState<ConsumerGroup | null>(null);

  if (selected) {
    const members = consumers.filter(c => c.group === selected.id);
    return (
      <div>
        <button
          onClick={() => setSelected(null)}
          style={{ background: "transparent", border: "none", fontSize: 11, color: "var(--text-3)", cursor: "pointer", fontFamily: "inherit", padding: 0, display: "flex", alignItems: "center", gap: 6, marginBottom: 20 }}
          onMouseEnter={e => (e.currentTarget as HTMLElement).style.color = "var(--text-1)"}
          onMouseLeave={e => (e.currentTarget as HTMLElement).style.color = "var(--text-3)"}
        >
          <ArrowLeft size={11} />
          Consumer Groups
        </button>

        <div style={{ paddingBottom: 20, borderBottom: "1px solid var(--border)", marginBottom: 24 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 14, marginBottom: 12 }}>
            <h1 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 16, fontWeight: 400, margin: 0, color: "var(--text-1)" }}>
              {selected.id}
            </h1>
            <Badge status={selected.state} />
          </div>
          <div style={{ display: "flex", gap: 32 }}>
            {[
              { l: "Members",    v: selected.members },
              { l: "Topics",     v: selected.topics.length },
              { l: "Partitions", v: selected.partitions },
              { l: "Offset",     v: selected.currentOffset.toLocaleString() },
              { l: "Coordinator",v: selected.coordinator },
            ].map(({ l, v }) => (
              <div key={l}>
                <div className="label" style={{ marginBottom: 4 }}>{l}</div>
                <div style={{ fontSize: 13 }}>{v}</div>
              </div>
            ))}
            <div>
              <div className="label" style={{ marginBottom: 4 }}>Total Lag</div>
              <div style={{ fontSize: 13 }}><LagValue lag={selected.lag} /></div>
            </div>
          </div>
        </div>

        <div className="label" style={{ marginBottom: 12 }}>Member Assignment</div>
        {members.length === 0 ? (
          <div style={{ padding: "48px 0", color: "var(--text-3)", fontSize: 12 }}>No active members in this group.</div>
        ) : (
          <table className="tbl">
            <thead>
              <tr>
                <th>Consumer</th>
                <th>Service</th>
                <th>Topic</th>
                <th style={{ textAlign: "right" }}>Cur. Offset</th>
                <th style={{ textAlign: "right" }}>Latest</th>
                <th style={{ textAlign: "right" }}>Lag</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {members.map(c => (
                <tr key={c.id}>
                  <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-2)" }}>{c.id}</td>
                  <td>{c.service}</td>
                  <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-2)" }}>{c.topic}</td>
                  <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums", fontFamily: "'JetBrains Mono', monospace", fontSize: 11 }}>{c.currentOffset.toLocaleString()}</td>
                  <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums", fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-2)" }}>{c.latestOffset.toLocaleString()}</td>
                  <td style={{ textAlign: "right" }}><LagValue lag={c.lag} /></td>
                  <td><Badge status={c.status} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    );
  }

  return (
    <div>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", paddingBottom: 16, borderBottom: "1px solid var(--border)", marginBottom: 20 }}>
        <div className="label">Consumer Groups</div>
        <div style={{ display: "flex", gap: 16, fontSize: 11 }}>
          <span style={{ color: "var(--ok)" }}>{consumerGroups.filter(g => g.state === "Stable").length} Stable</span>
          <span style={{ color: "var(--warn)" }}>{consumerGroups.filter(g => g.state === "Rebalancing").length} Rebalancing</span>
          <span style={{ color: "var(--text-3)" }}>{consumerGroups.filter(g => g.state === "Empty").length} Empty</span>
        </div>
      </div>

      <table className="tbl">
        <thead>
          <tr>
            <th>Group ID</th>
            <th style={{ textAlign: "right" }}>Members</th>
            <th>Topics</th>
            <th style={{ textAlign: "right" }}>Partitions</th>
            <th style={{ textAlign: "right" }}>Offset</th>
            <th style={{ textAlign: "right" }}>Lag</th>
            <th>State</th>
          </tr>
        </thead>
        <tbody>
          {consumerGroups.map(g => (
            <tr
              key={g.id}
              style={{ cursor: "pointer" }}
              onClick={() => setSelected(g)}
            >
              <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11 }}>{g.id}</td>
              <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>{g.members}</td>
              <td>
                <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
                  {g.topics.map(t => (
                    <span key={t} style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: "var(--text-3)", border: "1px solid var(--border)", padding: "1px 5px" }}>{t}</span>
                  ))}
                </div>
              </td>
              <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>{g.partitions}</td>
              <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums", fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-2)" }}>{g.currentOffset.toLocaleString()}</td>
              <td style={{ textAlign: "right" }}><LagValue lag={g.lag} /></td>
              <td><Badge status={g.state} /></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
