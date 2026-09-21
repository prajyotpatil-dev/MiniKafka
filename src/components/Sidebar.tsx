import {
  LayoutDashboard,
  Database,
  MessageSquare,
  Users,
  Radio,
  UsersRound,
  ScrollText,
  BarChart2,
  Activity,
  Settings,
} from "lucide-react";

export type Page =
  | "overview"
  | "topics"
  | "topic-detail"
  | "messages"
  | "consumers"
  | "producers"
  | "consumer-groups"
  | "logs"
  | "metrics"
  | "system-health"
  | "settings";

const nav = [
  { id: "overview",        label: "Overview",         icon: LayoutDashboard },
  { id: "topics",          label: "Topics",           icon: Database },
  { id: "messages",        label: "Messages",         icon: MessageSquare },
  { id: "consumers",       label: "Consumers",        icon: Users },
  { id: "producers",       label: "Producers",        icon: Radio },
  { id: "consumer-groups", label: "Consumer Groups",  icon: UsersRound },
  { id: "logs",            label: "Logs",             icon: ScrollText },
  { id: "metrics",         label: "Metrics",          icon: BarChart2 },
  { id: "system-health",   label: "System Health",    icon: Activity },
  { id: "settings",        label: "Settings",         icon: Settings },
] as const;

interface SidebarProps {
  current: Page;
  navigate: (page: Page) => void;
}

export default function Sidebar({ current, navigate }: SidebarProps) {
  return (
    <aside
      style={{
        position: "fixed",
        left: 0,
        top: 0,
        height: "100%",
        width: 200,
        background: "var(--panel)",
        borderRight: "1px solid var(--border)",
        display: "flex",
        flexDirection: "column",
        zIndex: 40,
      }}
    >
      {/* Wordmark */}
      <div
        style={{
          padding: "16px 20px 14px",
          borderBottom: "1px solid var(--border)",
        }}
      >
        <div
          style={{
            fontSize: 11,
            fontWeight: 500,
            letterSpacing: "0.1em",
            textTransform: "uppercase",
            color: "var(--text-1)",
            lineHeight: 1,
          }}
        >
          Mini Kafka
        </div>
        <div
          style={{
            fontSize: 10,
            color: "var(--text-3)",
            marginTop: 4,
            letterSpacing: "0.04em",
          }}
        >
          Event Broker
        </div>
      </div>

      {/* Nav */}
      <nav style={{ flex: 1, padding: "8px 0", overflowY: "auto" }}>
        {nav.map(({ id, label, icon: Icon }) => {
          const active = current === id || (id === "topics" && current === "topic-detail");
          return (
            <button
              key={id}
              onClick={() => navigate(id as Page)}
              style={{
                width: "100%",
                display: "flex",
                alignItems: "center",
                gap: 10,
                padding: "7px 20px",
                background: "transparent",
                border: "none",
                borderLeft: active ? "2px solid var(--text-1)" : "2px solid transparent",
                color: active ? "var(--text-1)" : "var(--text-3)",
                fontSize: 12,
                fontWeight: active ? 500 : 400,
                fontFamily: "inherit",
                cursor: "pointer",
                textAlign: "left",
                transition: "color 0.1s, border-color 0.1s",
              }}
              onMouseEnter={e => { if (!active) (e.currentTarget as HTMLElement).style.color = "var(--text-2)"; }}
              onMouseLeave={e => { if (!active) (e.currentTarget as HTMLElement).style.color = "var(--text-3)"; }}
            >
              <Icon size={13} strokeWidth={1.75} style={{ flexShrink: 0 }} />
              {label}
            </button>
          );
        })}
      </nav>

      {/* Footer */}
      <div
        style={{
          padding: "12px 20px",
          borderTop: "1px solid var(--border)",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 7, marginBottom: 4 }}>
          <span style={{ width: 5, height: 5, borderRadius: "50%", background: "var(--ok)", display: "inline-block" }} />
          <span style={{ fontSize: 10, color: "var(--ok)", letterSpacing: "0.04em" }}>Online</span>
        </div>
        <div style={{ fontSize: 10, color: "var(--text-3)", fontFamily: "'JetBrains Mono', monospace", letterSpacing: "0.02em" }}>
          v1.0.0 · broker-1
        </div>
      </div>
    </aside>
  );
}
