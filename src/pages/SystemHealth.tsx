import { Server, HardDrive, Wifi, Radio, Users, Database, Cpu } from "lucide-react";
import Badge from "../components/Badge";

type HS = "ONLINE" | "HEALTHY" | "DEGRADED" | "OFFLINE";

const statusColor: Record<HS, string> = {
  ONLINE:  "var(--ok)",
  HEALTHY: "var(--ok)",
  DEGRADED:"var(--warn)",
  OFFLINE: "var(--err)",
};

const healthItems = [
  { label: "Broker",            state: "ONLINE"   as HS, icon: Server,   detail: "broker-1 · 10.0.0.1",          metrics: [{ k: "Port", v: "9092" }, { k: "Protocol", v: "TCP" }] },
  { label: "Storage",           state: "HEALTHY"  as HS, icon: HardDrive,detail: "Local disk · 38% used",         metrics: [{ k: "Used", v: "18.4 GB" }, { k: "Free", v: "29.6 GB" }] },
  { label: "Network",           state: "HEALTHY"  as HS, icon: Wifi,     detail: "1.2ms avg latency",             metrics: [{ k: "In", v: "1.4 MB/s" }, { k: "Out", v: "1.3 MB/s" }] },
  { label: "Producer Conns",    state: "HEALTHY"  as HS, icon: Radio,    detail: "4 active connections",          metrics: [{ k: "Active", v: "3" }, { k: "Idle", v: "1" }] },
  { label: "Consumer Conns",    state: "DEGRADED" as HS, icon: Users,    detail: "1 consumer disconnected",       metrics: [{ k: "Active", v: "6" }, { k: "Lagging", v: "3" }] },
  { label: "Topic Health",      state: "HEALTHY"  as HS, icon: Database, detail: "5 topics · 1 with warning",    metrics: [{ k: "Total", v: "5" }, { k: "Active", v: "4" }] },
  { label: "Msg Processing",    state: "HEALTHY"  as HS, icon: Cpu,      detail: "284 msg/s throughput",          metrics: [{ k: "Published", v: "24,847" }, { k: "Consumed", v: "23,191" }] },
];

const sysInfo = [
  { l: "Broker Version",  v: "Mini Kafka v1.0.0" },
  { l: "Java Version",    v: "OpenJDK 21.0.2" },
  { l: "OS",              v: "Linux 5.15.0 x86_64" },
  { l: "Heap Used",       v: "512 MB / 1024 MB" },
  { l: "Log Directory",   v: "/var/kafka/logs" },
  { l: "Start Time",      v: "2026-08-29 04:02:18" },
  { l: "Uptime",          v: "14d 07h 22m 14s" },
  { l: "Topic Count",     v: "5" },
  { l: "Total Partitions",v: "13" },
  { l: "Total Messages",  v: "24,847" },
];

const resources = [
  { l: "Heap Memory", pct: 50, detail: "512 MB / 1024 MB",      color: "var(--text-1)" },
  { l: "CPU Usage",   pct: 28, detail: "28% · 4 cores",         color: "var(--text-2)" },
  { l: "Disk Usage",  pct: 38, detail: "18.4 GB / 48 GB",       color: "var(--text-2)" },
  { l: "Network I/O", pct: 14, detail: "1.4 MB/s in · 1.3 out", color: "var(--text-3)" },
];

export default function SystemHealth() {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 32 }}>

      {/* Health grid — flat, tabular */}
      <div>
        <div className="label" style={{ marginBottom: 12 }}>Component Status</div>
        <table className="tbl">
          <thead>
            <tr>
              <th>Component</th>
              <th>Status</th>
              <th>Detail</th>
              <th style={{ textAlign: "right" }}>Metric A</th>
              <th style={{ textAlign: "right" }}>Metric B</th>
            </tr>
          </thead>
          <tbody>
            {healthItems.map(({ label, state, icon: Icon, detail, metrics }) => (
              <tr key={label}>
                <td>
                  <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                    <Icon size={13} style={{ color: "var(--text-3)", flexShrink: 0 }} strokeWidth={1.5} />
                    <span style={{ fontWeight: 500 }}>{label}</span>
                  </div>
                </td>
                <td>
                  <span style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11, fontFamily: "'JetBrains Mono', monospace", color: statusColor[state] }}>
                    <span style={{ width: 5, height: 5, borderRadius: "50%", background: statusColor[state], display: "inline-block", flexShrink: 0 }} />
                    {state}
                  </span>
                </td>
                <td style={{ color: "var(--text-3)", fontSize: 11 }}>{detail}</td>
                <td style={{ textAlign: "right", fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-2)" }}>
                  {metrics[0].k}: {metrics[0].v}
                </td>
                <td style={{ textAlign: "right", fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-2)" }}>
                  {metrics[1].k}: {metrics[1].v}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Two column: sysinfo + resources */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 32, alignItems: "start" }}>
        {/* System info */}
        <div>
          <div className="label" style={{ marginBottom: 12 }}>System Information</div>
          <div style={{ borderTop: "1px solid var(--border)" }}>
            {sysInfo.map(({ l, v }) => (
              <div
                key={l}
                style={{
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "space-between",
                  padding: "8px 0",
                  borderBottom: "1px solid var(--border-subtle)",
                }}
              >
                <span style={{ fontSize: 11, color: "var(--text-3)" }}>{l}</span>
                <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: "var(--text-1)" }}>{v}</span>
              </div>
            ))}
          </div>
        </div>

        {/* Resource usage */}
        <div>
          <div className="label" style={{ marginBottom: 12 }}>Resource Usage</div>
          <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
            {resources.map(({ l, pct, detail, color }) => (
              <div key={l}>
                <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
                  <span style={{ fontSize: 11, color: "var(--text-2)" }}>{l}</span>
                  <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color }}>
                    {pct}%
                  </span>
                </div>
                <div style={{ height: 2, background: "var(--border)" }}>
                  <div style={{ height: "100%", width: `${pct}%`, background: color }} />
                </div>
                <div style={{ fontSize: 10, color: "var(--text-3)", marginTop: 4, fontFamily: "'JetBrains Mono', monospace" }}>
                  {detail}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
