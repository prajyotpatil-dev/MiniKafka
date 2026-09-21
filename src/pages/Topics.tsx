import { useState } from "react";
import { Search, Plus, X } from "lucide-react";
import Badge from "../components/Badge";
import { topics as initialTopics } from "../data/mock";
import type { Page } from "../components/Sidebar";

interface TopicsProps {
  navigate: (page: Page, params?: Record<string, string>) => void;
}

export default function Topics({ navigate }: TopicsProps) {
  const [search, setSearch] = useState("");
  const [filter, setFilter] = useState("all");
  const [showModal, setShowModal] = useState(false);
  const [form, setForm] = useState({ name: "", partitions: "3", replication: "1", retention: "7d" });
  const [toast, setToast] = useState<string | null>(null);

  const filtered = initialTopics.filter(t => {
    const ms = search.toLowerCase();
    return t.id.includes(ms) && (filter === "all" || t.status === filter);
  });

  const create = (e: React.FormEvent) => {
    e.preventDefault();
    if (!form.name.trim()) return;
    setShowModal(false);
    setToast(`Topic "${form.name}" created`);
    setForm({ name: "", partitions: "3", replication: "1", retention: "7d" });
    setTimeout(() => setToast(null), 3000);
  };

  return (
    <div>
      {/* Controls */}
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 20, paddingBottom: 16, borderBottom: "1px solid var(--border)" }}>
        <div style={{ position: "relative", flex: 1, maxWidth: 280 }}>
          <Search size={11} style={{ position: "absolute", left: 9, top: "50%", transform: "translateY(-50%)", color: "var(--text-3)" }} />
          <input
            type="text"
            placeholder="Search topics…"
            value={search}
            onChange={e => setSearch(e.target.value)}
            className="inp"
            style={{ width: "100%", paddingLeft: 28 }}
          />
        </div>
        <select
          value={filter}
          onChange={e => setFilter(e.target.value)}
          className="inp"
          style={{ cursor: "pointer" }}
        >
          <option value="all">All statuses</option>
          <option value="active">Active</option>
          <option value="idle">Idle</option>
          <option value="warning">Warning</option>
          <option value="error">Error</option>
        </select>
        <button className="btn-primary" onClick={() => setShowModal(true)}>
          <Plus size={12} strokeWidth={2} />
          Create Topic
        </button>
        <span style={{ marginLeft: "auto", fontSize: 11, color: "var(--text-3)" }}>
          {filtered.length} topics
        </span>
      </div>

      {/* Table */}
      <table className="tbl">
        <thead>
          <tr>
            <th>Topic Name</th>
            <th style={{ textAlign: "right" }}>Partitions</th>
            <th style={{ textAlign: "right" }}>Messages</th>
            <th style={{ textAlign: "right" }}>In Rate</th>
            <th style={{ textAlign: "right" }}>Out Rate</th>
            <th style={{ textAlign: "right" }}>Consumers</th>
            <th>Retention</th>
            <th>Status</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {filtered.length === 0 ? (
            <tr><td colSpan={9} style={{ textAlign: "center", color: "var(--text-3)", padding: "48px 16px" }}>No topics match.</td></tr>
          ) : filtered.map(t => (
            <tr key={t.id}>
              <td>
                <button
                  onClick={() => navigate("topic-detail", { topicId: t.id })}
                  style={{ background: "transparent", border: "none", fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-1)", cursor: "pointer", padding: 0, textAlign: "left" }}
                  onMouseEnter={e => (e.currentTarget as HTMLElement).style.color = "var(--text-2)"}
                  onMouseLeave={e => (e.currentTarget as HTMLElement).style.color = "var(--text-1)"}
                >
                  {t.id}
                </button>
                <div style={{ fontSize: 10, color: "var(--text-3)", marginTop: 2 }}>{t.description.substring(0, 48)}…</div>
              </td>
              <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>{t.partitions}</td>
              <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>{t.messages.toLocaleString()}</td>
              <td style={{ textAlign: "right", color: "var(--text-2)", fontFamily: "'JetBrains Mono', monospace", fontSize: 11 }}>
                {t.incomingRate > 0 ? `${t.incomingRate}/s` : "—"}
              </td>
              <td style={{ textAlign: "right", color: "var(--text-2)", fontFamily: "'JetBrains Mono', monospace", fontSize: 11 }}>
                {t.outgoingRate > 0 ? `${t.outgoingRate}/s` : "—"}
              </td>
              <td style={{ textAlign: "right" }}>{t.consumers}</td>
              <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-2)" }}>{t.retention}</td>
              <td><Badge status={t.status} /></td>
              <td>
                <button
                  onClick={() => navigate("topic-detail", { topicId: t.id })}
                  style={{ background: "transparent", border: "none", fontSize: 11, color: "var(--text-3)", cursor: "pointer", fontFamily: "inherit", padding: 0 }}
                  onMouseEnter={e => (e.currentTarget as HTMLElement).style.color = "var(--text-1)"}
                  onMouseLeave={e => (e.currentTarget as HTMLElement).style.color = "var(--text-3)"}
                >
                  Inspect →
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      {/* Create Topic Modal */}
      {showModal && (
        <div style={{ position: "fixed", inset: 0, zIndex: 50, display: "flex", alignItems: "center", justifyContent: "center" }}>
          <div
            style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,0.72)" }}
            onClick={() => setShowModal(false)}
          />
          <div style={{
            position: "relative",
            background: "var(--panel)",
            border: "1px solid var(--border)",
            width: "100%",
            maxWidth: 420,
            margin: "0 16px",
          }}>
            {/* Modal header */}
            <div style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              padding: "16px 20px",
              borderBottom: "1px solid var(--border)",
            }}>
              <div>
                <div className="label">Create Topic</div>
                <div style={{ fontSize: 11, color: "var(--text-3)", marginTop: 3 }}>Configure a new message stream</div>
              </div>
              <button
                onClick={() => setShowModal(false)}
                style={{ background: "transparent", border: "none", color: "var(--text-3)", cursor: "pointer", padding: 2 }}
              >
                <X size={14} />
              </button>
            </div>

            {/* Modal body */}
            <form onSubmit={create} style={{ padding: "20px" }}>
              <div style={{ marginBottom: 16 }}>
                <div className="label" style={{ marginBottom: 6 }}>Topic Name</div>
                <input
                  type="text"
                  placeholder="e.g. order-events"
                  value={form.name}
                  onChange={e => setForm(f => ({ ...f, name: e.target.value }))}
                  className="inp inp-mono"
                  style={{ width: "100%" }}
                  required
                />
                <div style={{ fontSize: 10, color: "var(--text-3)", marginTop: 5 }}>Use kebab-case. No spaces.</div>
              </div>

              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12, marginBottom: 24 }}>
                <div>
                  <div className="label" style={{ marginBottom: 6 }}>Partitions</div>
                  <input type="number" min="1" max="64" value={form.partitions} onChange={e => setForm(f => ({ ...f, partitions: e.target.value }))} className="inp" style={{ width: "100%" }} />
                </div>
                <div>
                  <div className="label" style={{ marginBottom: 6 }}>Replication</div>
                  <input type="number" min="1" max="3" value={form.replication} onChange={e => setForm(f => ({ ...f, replication: e.target.value }))} className="inp" style={{ width: "100%" }} />
                </div>
                <div>
                  <div className="label" style={{ marginBottom: 6 }}>Retention</div>
                  <select value={form.retention} onChange={e => setForm(f => ({ ...f, retention: e.target.value }))} className="inp" style={{ width: "100%", cursor: "pointer" }}>
                    <option>1d</option>
                    <option>3d</option>
                    <option>7d</option>
                    <option>14d</option>
                    <option>30d</option>
                    <option>90d</option>
                  </select>
                </div>
              </div>

              <div style={{ display: "flex", gap: 10 }}>
                <button type="button" onClick={() => setShowModal(false)} className="btn-secondary" style={{ flex: 1, justifyContent: "center" }}>
                  Cancel
                </button>
                <button type="submit" className="btn-primary" style={{ flex: 1, justifyContent: "center" }}>
                  Create Topic
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Toast */}
      {toast && (
        <div style={{
          position: "fixed",
          bottom: 24,
          right: 24,
          zIndex: 60,
          background: "var(--panel)",
          border: "1px solid var(--border)",
          padding: "10px 16px",
          fontSize: 12,
          color: "var(--ok)",
          display: "flex",
          alignItems: "center",
          gap: 8,
        }}>
          <span style={{ width: 5, height: 5, borderRadius: "50%", background: "var(--ok)", display: "inline-block" }} />
          {toast}
        </div>
      )}
    </div>
  );
}
