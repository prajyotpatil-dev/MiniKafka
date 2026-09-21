import { Search, RefreshCw } from "lucide-react";
import { useState } from "react";

interface TopBarProps {
  title: string;
  breadcrumbs?: string[];
}

export default function TopBar({ title, breadcrumbs }: TopBarProps) {
  const [spinning, setSpinning] = useState(false);

  const refresh = () => {
    setSpinning(true);
    setTimeout(() => setSpinning(false), 800);
  };

  return (
    <header
      style={{
        position: "fixed",
        top: 0,
        right: 0,
        left: 200,
        height: 44,
        background: "var(--panel)",
        borderBottom: "1px solid var(--border)",
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        padding: "0 24px",
        zIndex: 30,
      }}
    >
      {/* Left: breadcrumb + title */}
      <div style={{ display: "flex", alignItems: "center", gap: 8, overflow: "hidden" }}>
        {breadcrumbs && breadcrumbs.length > 0 && (
          <>
            {breadcrumbs.map((b, i) => (
              <span key={i} style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <span style={{ fontSize: 11, color: "var(--text-3)" }}>{b}</span>
                <span style={{ fontSize: 11, color: "var(--text-3)" }}>/</span>
              </span>
            ))}
          </>
        )}
        <span style={{ fontSize: 12, fontWeight: 500, color: "var(--text-1)", whiteSpace: "nowrap" }}>
          {title}
        </span>
      </div>

      {/* Right: search + controls */}
      <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
        <div style={{ position: "relative" }}>
          <Search size={11} style={{ position: "absolute", left: 9, top: "50%", transform: "translateY(-50%)", color: "var(--text-3)" }} />
          <input
            type="text"
            placeholder="Search..."
            className="inp"
            style={{ paddingLeft: 28, width: 200, fontSize: 11 }}
          />
        </div>

        <button
          onClick={refresh}
          style={{
            background: "transparent",
            border: "none",
            color: "var(--text-3)",
            cursor: "pointer",
            padding: 4,
            display: "flex",
            alignItems: "center",
          }}
          onMouseEnter={e => (e.currentTarget as HTMLElement).style.color = "var(--text-2)"}
          onMouseLeave={e => (e.currentTarget as HTMLElement).style.color = "var(--text-3)"}
        >
          <RefreshCw size={12} style={spinning ? { animation: "spin 0.8s linear" } : {}} />
        </button>

        <div style={{ display: "flex", alignItems: "center", gap: 8, paddingLeft: 12, borderLeft: "1px solid var(--border)" }}>
          <span style={{ fontSize: 10, color: "var(--text-3)", letterSpacing: "0.04em" }}>admin</span>
          <div
            style={{
              width: 22,
              height: 22,
              border: "1px solid var(--border)",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              fontSize: 9,
              fontWeight: 600,
              color: "var(--text-2)",
              letterSpacing: "0.05em",
              borderRadius: 2,
            }}
          >
            MK
          </div>
        </div>
      </div>
    </header>
  );
}
