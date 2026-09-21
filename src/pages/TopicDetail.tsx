import { useState } from "react";
import { ArrowLeft, Copy, X } from "lucide-react";
import Badge from "../components/Badge";
import { topics, messages } from "../data/mock";
import type { Message } from "../data/mock";
import type { Page } from "../components/Sidebar";

interface TopicDetailProps {
  topicId: string;
  navigate: (page: Page, params?: Record<string, string>) => void;
}

const tabs = ["Messages", "Consumers", "Partitions", "Configuration"] as const;
type Tab = typeof tabs[number];

export default function TopicDetail({ topicId, navigate }: TopicDetailProps) {
  const topic = topics.find(t => t.id === topicId) ?? topics[0];
  const topicMessages = messages.filter(m => m.topic === topic.id);
  const [tab, setTab] = useState<Tab>("Messages");
  const [selected, setSelected] = useState<Message | null>(null);
  const [copied, setCopied] = useState(false);

  const copy = () => {
    if (!selected) return;
    navigator.clipboard.writeText(JSON.stringify({
      id: selected.id, topic: selected.topic, partition: selected.partition,
      offset: selected.offset, timestamp: selected.timestamp, producer: selected.producer,
      headers: selected.headers, payload: JSON.parse(selected.payload),
    }, null, 2));
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const partitions = Array.from({ length: topic.partitions }, (_, i) => ({
    id: i,
    messages: Math.floor(topic.messages / topic.partitions),
    hw: Math.floor(topic.messages / topic.partitions) - 3,
    logSize: `${(20 + i * 8.4).toFixed(1)} MB`,
  }));

  const configRows = [
    { key: "retention.ms",       value: topic.retention === "7d" ? "604800000" : "2592000000", custom: true },
    { key: "retention.bytes",    value: "-1",           custom: false },
    { key: "cleanup.policy",     value: "delete",       custom: false },
    { key: "compression.type",   value: "producer",     custom: false },
    { key: "max.message.bytes",  value: "1048588",      custom: false },
    { key: "min.insync.replicas",value: "1",            custom: false },
  ];

  return (
    <div>
      {/* Back */}
      <button
        onClick={() => navigate("topics")}
        style={{ background: "transparent", border: "none", fontSize: 11, color: "var(--text-3)", cursor: "pointer", fontFamily: "inherit", padding: 0, display: "flex", alignItems: "center", gap: 6, marginBottom: 20 }}
        onMouseEnter={e => (e.currentTarget as HTMLElement).style.color = "var(--text-1)"}
        onMouseLeave={e => (e.currentTarget as HTMLElement).style.color = "var(--text-3)"}
      >
        <ArrowLeft size={11} />
        Topics
      </button>

      {/* Topic header */}
      <div style={{ paddingBottom: 20, borderBottom: "1px solid var(--border)", marginBottom: 20 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 14, marginBottom: 8 }}>
          <h1 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 18, fontWeight: 400, color: "var(--text-1)", margin: 0, letterSpacing: "-0.01em" }}>
            {topic.id}
          </h1>
          <Badge status={topic.status} />
        </div>
        <div style={{ fontSize: 11, color: "var(--text-3)", marginBottom: 16 }}>{topic.description}</div>
        <div style={{ display: "flex", gap: 32 }}>
          {[
            { l: "Partitions", v: topic.partitions },
            { l: "Messages",   v: topic.messages.toLocaleString() },
            { l: "Producers",  v: topic.producers },
            { l: "Consumers",  v: topic.consumers },
            { l: "In Rate",    v: topic.incomingRate > 0 ? `${topic.incomingRate}/s` : "—" },
            { l: "Out Rate",   v: topic.outgoingRate > 0 ? `${topic.outgoingRate}/s` : "—" },
            { l: "Retention",  v: topic.retention },
          ].map(({ l, v }) => (
            <div key={l}>
              <div className="label" style={{ marginBottom: 4 }}>{l}</div>
              <div style={{ fontSize: 13, fontVariantNumeric: "tabular-nums", color: "var(--text-1)" }}>{v}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Tabs */}
      <div style={{ display: "flex", borderBottom: "1px solid var(--border)", marginBottom: 20, gap: 0 }}>
        {tabs.map(t => (
          <button
            key={t}
            onClick={() => setTab(t)}
            style={{
              background: "transparent",
              border: "none",
              borderBottom: tab === t ? "1px solid var(--text-1)" : "1px solid transparent",
              color: tab === t ? "var(--text-1)" : "var(--text-3)",
              fontSize: 12,
              fontWeight: tab === t ? 500 : 400,
              fontFamily: "inherit",
              cursor: "pointer",
              padding: "8px 16px 9px",
              marginBottom: -1,
              transition: "color 0.1s",
            }}
          >
            {t}
          </button>
        ))}
      </div>

      {/* Messages tab */}
      {tab === "Messages" && (
        <div style={{ display: "flex", gap: 24, alignItems: "flex-start" }}>
          <div style={{ flex: 1, overflow: "auto" }}>
            <table className="tbl" style={{ minWidth: 640 }}>
              <thead>
                <tr>
                  <th style={{ textAlign: "right" }}>Offset</th>
                  <th>Message ID</th>
                  <th>Timestamp</th>
                  <th>Producer</th>
                  <th>Key</th>
                  <th>Payload</th>
                </tr>
              </thead>
              <tbody>
                {topicMessages.length === 0 ? (
                  <tr><td colSpan={6} style={{ textAlign: "center", color: "var(--text-3)", padding: "48px 0" }}>No messages.</td></tr>
                ) : topicMessages.map(msg => (
                  <tr
                    key={msg.id}
                    style={{ cursor: "pointer", background: selected?.id === msg.id ? "var(--panel-raised)" : "transparent" }}
                    onClick={() => setSelected(selected?.id === msg.id ? null : msg)}
                  >
                    <td style={{ textAlign: "right", fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-3)" }}>{msg.offset}</td>
                    <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-2)" }}>{msg.id}</td>
                    <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-3)" }}>{msg.timestamp}</td>
                    <td style={{ fontSize: 12, color: "var(--text-2)" }}>{msg.producer}</td>
                    <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-3)" }}>{msg.key}</td>
                    <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-3)", maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                      {msg.payload.replace(/\s+/g, " ").substring(0, 50)}…
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Message detail panel */}
          {selected && (
            <div style={{
              width: 320,
              flexShrink: 0,
              border: "1px solid var(--border)",
              background: "var(--panel)",
            }}>
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "10px 16px", borderBottom: "1px solid var(--border)" }}>
                <div className="label">Inspector</div>
                <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <button
                    onClick={copy}
                    className="btn-secondary"
                    style={{ fontSize: 10, padding: "3px 8px", gap: 4 }}
                  >
                    <Copy size={10} />
                    {copied ? "Copied" : "Copy JSON"}
                  </button>
                  <button onClick={() => setSelected(null)} style={{ background: "transparent", border: "none", color: "var(--text-3)", cursor: "pointer" }}>
                    <X size={13} />
                  </button>
                </div>
              </div>
              <div style={{ padding: "16px", overflowY: "auto", maxHeight: "calc(100vh - 320px)" }}>
                {[
                  { l: "Message ID",  v: selected.id,        mono: true },
                  { l: "Topic",       v: selected.topic,     mono: false },
                  { l: "Partition",   v: String(selected.partition), mono: true },
                  { l: "Offset",      v: String(selected.offset),    mono: true },
                  { l: "Timestamp",   v: selected.timestamp, mono: true },
                  { l: "Producer",    v: selected.producer,  mono: false },
                  { l: "Key",         v: selected.key,       mono: true },
                ].map(({ l, v, mono }) => (
                  <div key={l} style={{ marginBottom: 14 }}>
                    <div className="label" style={{ marginBottom: 3 }}>{l}</div>
                    <div style={{ fontSize: 11, color: "var(--text-1)", fontFamily: mono ? "'JetBrains Mono', monospace" : "inherit", wordBreak: "break-all" }}>{v}</div>
                  </div>
                ))}
                <div style={{ marginBottom: 14 }}>
                  <div className="label" style={{ marginBottom: 3 }}>Headers</div>
                  {Object.entries(selected.headers).map(([k, v]) => (
                    <div key={k} style={{ display: "flex", gap: 8, fontSize: 11, marginBottom: 2, fontFamily: "'JetBrains Mono', monospace" }}>
                      <span style={{ color: "var(--text-3)" }}>{k}:</span>
                      <span style={{ color: "var(--text-2)" }}>{v}</span>
                    </div>
                  ))}
                </div>
                <div>
                  <div className="label" style={{ marginBottom: 6 }}>Payload</div>
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
                    {selected.payload}
                  </pre>
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Partitions tab */}
      {tab === "Partitions" && (
        <table className="tbl">
          <thead>
            <tr>
              <th>Partition</th>
              <th>Leader</th>
              <th style={{ textAlign: "right" }}>Messages</th>
              <th style={{ textAlign: "right" }}>High Watermark</th>
              <th style={{ textAlign: "right" }}>Log Size</th>
              <th>Replicas</th>
              <th>ISR</th>
            </tr>
          </thead>
          <tbody>
            {partitions.map(p => (
              <tr key={p.id}>
                <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11 }}>{p.id}</td>
                <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-2)" }}>broker-1</td>
                <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums" }}>{p.messages.toLocaleString()}</td>
                <td style={{ textAlign: "right", fontVariantNumeric: "tabular-nums", color: "var(--text-2)" }}>{p.hw.toLocaleString()}</td>
                <td style={{ textAlign: "right", color: "var(--text-2)" }}>{p.logSize}</td>
                <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-3)" }}>[0]</td>
                <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--ok)" }}>[0]</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {/* Configuration tab */}
      {tab === "Configuration" && (
        <table className="tbl">
          <thead>
            <tr>
              <th>Key</th>
              <th>Value</th>
              <th>Source</th>
            </tr>
          </thead>
          <tbody>
            {configRows.map(r => (
              <tr key={r.key}>
                <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11 }}>{r.key}</td>
                <td style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-2)" }}>{r.value}</td>
                <td style={{ fontSize: 10, color: r.custom ? "var(--warn)" : "var(--text-3)" }}>
                  {r.custom ? "CUSTOM" : "DEFAULT"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      {tab === "Consumers" && (
        <div style={{ padding: "32px 0", color: "var(--text-3)", fontSize: 12 }}>
          Navigate to the Consumers page for full consumer details.
        </div>
      )}
    </div>
  );
}
