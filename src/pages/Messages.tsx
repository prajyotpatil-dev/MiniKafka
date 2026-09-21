import { useState } from "react";
import { Search, ChevronRight, ChevronDown, Copy, X } from "lucide-react";
import { messages } from "../data/mock";
import type { Message } from "../data/mock";

const allTopics = Array.from(new Set(messages.map(m => m.topic)));
const allProducers = Array.from(new Set(messages.map(m => m.producer)));

export default function Messages() {
  const [filters, setFilters] = useState({ messageId: "", topic: "all", producer: "all", offsetFrom: "", offsetTo: "", payload: "" });
  const [results, setResults] = useState<Message[]>(messages);
  const [searched, setSearched] = useState(false);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [selected, setSelected] = useState<Message | null>(null);
  const [copied, setCopied] = useState(false);

  const search = (e: React.FormEvent) => {
    e.preventDefault();
    const r = messages.filter(m => {
      if (filters.messageId && !m.id.toLowerCase().includes(filters.messageId.toLowerCase())) return false;
      if (filters.topic !== "all" && m.topic !== filters.topic) return false;
      if (filters.producer !== "all" && m.producer !== filters.producer) return false;
      if (filters.offsetFrom && m.offset < parseInt(filters.offsetFrom)) return false;
      if (filters.offsetTo && m.offset > parseInt(filters.offsetTo)) return false;
      if (filters.payload && !m.payload.toLowerCase().includes(filters.payload.toLowerCase())) return false;
      return true;
    });
    setResults(r);
    setSearched(true);
    setExpanded(new Set());
    setSelected(null);
  };

  const toggleExpand = (id: string) => {
    setExpanded(prev => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });
  };

  const copy = () => {
    if (!selected) return;
    navigator.clipboard.writeText(JSON.stringify(selected, null, 2));
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const setF = (k: string) => (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) =>
    setFilters(f => ({ ...f, [k]: e.target.value }));

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
      {/* Search form */}
      <form onSubmit={search} style={{ borderTop: "1px solid var(--border)", borderBottom: "1px solid var(--border)", paddingBottom: 16, paddingTop: 16 }}>
        <div className="label" style={{ marginBottom: 14 }}>Message Explorer</div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(6, 1fr)", gap: 12, marginBottom: 14 }}>
          <div>
            <div className="label" style={{ marginBottom: 5 }}>Message ID</div>
            <input value={filters.messageId} onChange={setF("messageId")} placeholder="msg_01J8K…" className="inp inp-mono" style={{ width: "100%" }} />
          </div>
          <div>
            <div className="label" style={{ marginBottom: 5 }}>Topic</div>
            <select value={filters.topic} onChange={setF("topic")} className="inp" style={{ width: "100%", cursor: "pointer" }}>
              <option value="all">All topics</option>
              {allTopics.map(t => <option key={t} value={t}>{t}</option>)}
            </select>
          </div>
          <div>
            <div className="label" style={{ marginBottom: 5 }}>Producer</div>
            <select value={filters.producer} onChange={setF("producer")} className="inp" style={{ width: "100%", cursor: "pointer" }}>
              <option value="all">All producers</option>
              {allProducers.map(p => <option key={p} value={p}>{p}</option>)}
            </select>
          </div>
          <div>
            <div className="label" style={{ marginBottom: 5 }}>Offset From</div>
            <input type="number" value={filters.offsetFrom} onChange={setF("offsetFrom")} placeholder="0" className="inp inp-mono" style={{ width: "100%" }} />
          </div>
          <div>
            <div className="label" style={{ marginBottom: 5 }}>Offset To</div>
            <input type="number" value={filters.offsetTo} onChange={setF("offsetTo")} placeholder="∞" className="inp inp-mono" style={{ width: "100%" }} />
          </div>
          <div>
            <div className="label" style={{ marginBottom: 5 }}>Payload Text</div>
            <input value={filters.payload} onChange={setF("payload")} placeholder="Search payload…" className="inp" style={{ width: "100%" }} />
          </div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <button type="submit" className="btn-primary">
            <Search size={11} />
            Search
          </button>
          {searched && (
            <span style={{ fontSize: 11, color: "var(--text-3)" }}>{results.length} result{results.length !== 1 ? "s" : ""}</span>
          )}
        </div>
      </form>

      {/* Results */}
      {!searched ? (
        <div style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", padding: "64px 0", color: "var(--text-3)" }}>
          <Search size={28} strokeWidth={1} style={{ marginBottom: 12, opacity: 0.3 }} />
          <div style={{ fontSize: 12 }}>Configure filters above and search</div>
          <div style={{ fontSize: 11, marginTop: 4, color: "var(--text-3)" }}>Results appear here</div>
        </div>
      ) : (
        <div style={{ display: "flex", gap: 24, alignItems: "flex-start" }}>
          <div style={{ flex: 1, overflow: "auto" }}>
            {results.length === 0 ? (
              <div style={{ padding: "64px 0", textAlign: "center", color: "var(--text-3)", fontSize: 12 }}>
                No messages match your filters.
              </div>
            ) : (
              <table className="tbl">
                <thead>
                  <tr>
                    <th style={{ width: 28 }} />
                    <th style={{ textAlign: "right" }}>Offset</th>
                    <th>Message ID</th>
                    <th>Topic</th>
                    <th>Timestamp</th>
                    <th>Producer</th>
                    <th>Payload</th>
                  </tr>
                </thead>
                <tbody>
                  {results.map(msg => (
                    <>
                      <tr
                        key={msg.id}
                        style={{
                          cursor: "pointer",
                          background: selected?.id === msg.id ? "var(--panel-raised)" : "transparent",
                        }}
                      >
                        <td style={{ padding: "9px 8px 9px 16px" }}>
                          <button
                            onClick={() => toggleExpand(msg.id)}
                            style={{ background: "transparent", border: "none", color: "var(--text-3)", cursor: "pointer", padding: 0, display: "flex", alignItems: "center" }}
                          >
                            {expanded.has(msg.id) ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
                          </button>
                        </td>
                        <td style={{ textAlign: "right", fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-3)" }}>{msg.offset}</td>
                        <td
                          style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-2)", cursor: "pointer" }}
                          onClick={() => setSelected(selected?.id === msg.id ? null : msg)}
                        >
                          {msg.id}
                        </td>
                        <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-2)" }}>{msg.topic}</td>
                        <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-3)" }}>{msg.timestamp}</td>
                        <td style={{ fontSize: 12, color: "var(--text-2)" }}>{msg.producer}</td>
                        <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-3)", maxWidth: 180, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                          {msg.payload.replace(/\s+/g, " ").substring(0, 48)}…
                        </td>
                      </tr>
                      {expanded.has(msg.id) && (
                        <tr key={`${msg.id}-exp`}>
                          <td />
                          <td colSpan={6} style={{ paddingBottom: 12, paddingTop: 4 }}>
                            <pre style={{
                              fontFamily: "'JetBrains Mono', monospace",
                              fontSize: 10,
                              color: "var(--text-1)",
                              background: "var(--bg)",
                              border: "1px solid var(--border)",
                              padding: "12px",
                              overflowX: "auto",
                              lineHeight: 1.6,
                              margin: 0,
                            }}>
                              {msg.payload}
                            </pre>
                          </td>
                        </tr>
                      )}
                    </>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {/* Inspector panel */}
          {selected && (
            <div style={{ width: 300, flexShrink: 0, border: "1px solid var(--border)", background: "var(--panel)" }}>
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "10px 16px", borderBottom: "1px solid var(--border)" }}>
                <div className="label">Inspector</div>
                <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <button onClick={copy} className="btn-secondary" style={{ fontSize: 10, padding: "3px 8px", gap: 4 }}>
                    <Copy size={10} />
                    {copied ? "Copied" : "Copy JSON"}
                  </button>
                  <button onClick={() => setSelected(null)} style={{ background: "transparent", border: "none", color: "var(--text-3)", cursor: "pointer" }}>
                    <X size={13} />
                  </button>
                </div>
              </div>
              <div style={{ padding: 16 }}>
                {[
                  { l: "Message ID",  v: selected.id,          mono: true },
                  { l: "Topic",       v: selected.topic,        mono: false },
                  { l: "Partition",   v: String(selected.partition), mono: true },
                  { l: "Offset",      v: String(selected.offset), mono: true },
                  { l: "Timestamp",   v: selected.timestamp,   mono: true },
                  { l: "Producer",    v: selected.producer,    mono: false },
                  { l: "Key",         v: selected.key,         mono: true },
                ].map(({ l, v, mono }) => (
                  <div key={l} style={{ marginBottom: 12 }}>
                    <div className="label" style={{ marginBottom: 3 }}>{l}</div>
                    <div style={{ fontSize: 11, color: "var(--text-1)", fontFamily: mono ? "'JetBrains Mono', monospace" : "inherit", wordBreak: "break-all" }}>{v}</div>
                  </div>
                ))}
                <div style={{ marginBottom: 12 }}>
                  <div className="label" style={{ marginBottom: 6 }}>Payload</div>
                  <pre style={{
                    fontFamily: "'JetBrains Mono', monospace",
                    fontSize: 10,
                    color: "var(--text-1)",
                    background: "var(--bg)",
                    border: "1px solid var(--border)",
                    padding: "10px",
                    overflowX: "auto",
                    lineHeight: 1.6,
                    margin: 0,
                    maxHeight: 240,
                    overflowY: "auto",
                  }}>
                    {selected.payload}
                  </pre>
                </div>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
