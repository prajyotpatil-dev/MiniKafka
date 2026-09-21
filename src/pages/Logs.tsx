import { useState, useEffect, useRef } from "react";
import { Play, Pause, Trash2 } from "lucide-react";
import { logEntries } from "../data/mock";
import type { LogEntry, LogLevel } from "../data/mock";

const levelColor: Record<LogLevel, string> = {
  INFO:  "var(--info)",
  DEBUG: "#8b949e",
  WARN:  "var(--warn)",
  ERROR: "var(--err)",
};

export default function Logs() {
  const [search, setSearch] = useState("");
  const [level, setLevel] = useState<LogLevel | "ALL">("ALL");
  const [topic, setTopic] = useState("all");
  const [live, setLive] = useState(true);
  const [entries, setEntries] = useState<LogEntry[]>(logEntries);
  const logRef = useRef<HTMLDivElement>(null);

  const allTopics = Array.from(new Set(logEntries.map(e => e.topic)));

  const visible = entries.filter(e => {
    const ms = `${e.message} ${e.producer} ${e.consumer}`.toLowerCase();
    return ms.includes(search.toLowerCase())
      && (level === "ALL" || e.level === level)
      && (topic === "all" || e.topic === topic);
  });

  useEffect(() => {
    if (!live) return;
    const id = setInterval(() => {
      const base = logEntries[Math.floor(Math.random() * logEntries.length)];
      const now = new Date().toISOString().replace("T", " ").substring(0, 23);
      setEntries(prev => [{ ...base, id: `log-${Date.now()}`, timestamp: now }, ...prev].slice(0, 200));
    }, 3500);
    return () => clearInterval(id);
  }, [live]);

  const levels: (LogLevel | "ALL")[] = ["ALL", "INFO", "DEBUG", "WARN", "ERROR"];

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "calc(100vh - 120px)", gap: 12 }}>
      {/* Controls */}
      <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap", flexShrink: 0 }}>
        <input
          type="text"
          placeholder="Search logs…"
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="inp"
          style={{ width: 240 }}
        />

        {/* Level filter */}
        <div style={{ display: "flex", border: "1px solid var(--border)" }}>
          {levels.map(l => (
            <button
              key={l}
              onClick={() => setLevel(l)}
              style={{
                background: level === l ? "var(--panel-raised)" : "transparent",
                border: "none",
                borderRight: l !== "ERROR" ? "1px solid var(--border)" : "none",
                color: level === l ? (l === "ALL" ? "var(--text-1)" : levelColor[l as LogLevel]) : "var(--text-3)",
                fontSize: 10,
                fontWeight: level === l ? 600 : 400,
                fontFamily: "'JetBrains Mono', monospace",
                letterSpacing: "0.04em",
                cursor: "pointer",
                padding: "5px 10px",
                transition: "color 0.1s",
              }}
            >
              {l}
            </button>
          ))}
        </div>

        <select
          value={topic}
          onChange={e => setTopic(e.target.value)}
          className="inp"
          style={{ cursor: "pointer", fontSize: 11 }}
        >
          <option value="all">All topics</option>
          {allTopics.map(t => <option key={t} value={t}>{t}</option>)}
        </select>

        <select className="inp" style={{ cursor: "pointer", fontSize: 11 }}>
          <option>Last 15m</option>
          <option>Last 1h</option>
          <option>Last 6h</option>
          <option>Last 24h</option>
        </select>

        <div style={{ marginLeft: "auto", display: "flex", alignItems: "center", gap: 8 }}>
          <button
            onClick={() => setLive(!live)}
            className="btn-secondary"
            style={{
              borderColor: live ? "var(--ok)" : "var(--border)",
              color: live ? "var(--ok)" : "var(--text-3)",
            }}
          >
            {live ? <><Pause size={10} />Pause</> : <><Play size={10} />Resume</>}
          </button>
          <button onClick={() => setEntries([])} className="btn-secondary">
            <Trash2 size={10} />
            Clear
          </button>
        </div>
      </div>

      {/* Log viewer */}
      <div
        ref={logRef}
        style={{
          flex: 1,
          minHeight: 0,
          overflowY: "auto",
          background: "var(--bg)",
          border: "1px solid var(--border)",
          fontFamily: "'JetBrains Mono', monospace",
          fontSize: 11,
          lineHeight: 1.5,
        }}
      >
        {/* Column headers */}
        <div style={{
          position: "sticky",
          top: 0,
          display: "grid",
          gridTemplateColumns: "138px 44px 130px 130px 1fr",
          gap: 0,
          padding: "6px 16px",
          background: "var(--panel)",
          borderBottom: "1px solid var(--border)",
          color: "var(--text-3)",
          fontSize: 9,
          fontWeight: 500,
          letterSpacing: "0.08em",
          textTransform: "uppercase",
          zIndex: 1,
        }}>
          <span>Timestamp</span>
          <span>Level</span>
          <span>Topic</span>
          <span>Source</span>
          <span>Message</span>
        </div>

        {visible.length === 0 ? (
          <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: 200, color: "var(--text-3)" }}>
            No log entries match filters.
          </div>
        ) : visible.map(entry => (
          <div
            key={entry.id}
            style={{
              display: "grid",
              gridTemplateColumns: "138px 44px 130px 130px 1fr",
              gap: 0,
              padding: "4px 16px",
              borderBottom: "1px solid var(--border-subtle)",
            }}
            onMouseEnter={e => (e.currentTarget as HTMLElement).style.background = "#131313"}
            onMouseLeave={e => (e.currentTarget as HTMLElement).style.background = "transparent"}
          >
            <span style={{ color: "var(--text-3)", userSelect: "none", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {entry.timestamp.replace("2026-09-12 ", "")}
            </span>
            <span style={{ color: levelColor[entry.level], fontWeight: 500, letterSpacing: "0.02em" }}>
              {entry.level}
            </span>
            <span style={{ color: "var(--text-3)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", paddingRight: 8 }}>
              {entry.topic || "—"}
            </span>
            <span style={{ color: "var(--text-2)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", paddingRight: 8 }}>
              {entry.producer || entry.consumer || "—"}
            </span>
            <span style={{ color: "var(--text-1)", overflowWrap: "break-word" }}>
              {entry.message}
            </span>
          </div>
        ))}
      </div>

      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", fontSize: 10, color: "var(--text-3)", flexShrink: 0 }}>
        <span>{visible.length} entries</span>
        {live && (
          <span style={{ color: "var(--ok)", display: "flex", alignItems: "center", gap: 6 }}>
            <span style={{ width: 5, height: 5, borderRadius: "50%", background: "var(--ok)", display: "inline-block" }} />
            streaming
          </span>
        )}
      </div>
    </div>
  );
}
