export default function Settings() {
  const brokerFields = [
    { l: "Broker Host",        v: "localhost",      mono: true },
    { l: "Broker Port",        v: "9092",           mono: true },
    { l: "Log Directory",      v: "/var/kafka/logs", mono: true },
    { l: "Default Retention",  v: "7d",             mono: true },
  ];

  const uiFields = [
    { l: "Auto-refresh interval",   v: "5s" },
    { l: "Max log entries",         v: "200" },
    { l: "Default time range",      v: "15m" },
  ];

  return (
    <div style={{ maxWidth: 560 }}>
      <div style={{ paddingBottom: 16, borderBottom: "1px solid var(--border)", marginBottom: 28 }}>
        <div className="label">Settings</div>
        <div style={{ fontSize: 11, color: "var(--text-3)", marginTop: 4 }}>Broker and UI configuration</div>
      </div>

      <div style={{ marginBottom: 32 }}>
        <div className="label" style={{ marginBottom: 16 }}>Broker Configuration</div>
        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          {brokerFields.map(({ l, v, mono }) => (
            <div key={l} style={{ display: "flex", alignItems: "center" }}>
              <label style={{ fontSize: 12, color: "var(--text-2)", width: 180, flexShrink: 0 }}>{l}</label>
              <input
                defaultValue={v}
                className={`inp${mono ? " inp-mono" : ""}`}
                style={{ flex: 1 }}
              />
            </div>
          ))}
        </div>
      </div>

      <div style={{ marginBottom: 32 }}>
        <div className="label" style={{ marginBottom: 16 }}>UI Preferences</div>
        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          {uiFields.map(({ l, v }) => (
            <div key={l} style={{ display: "flex", alignItems: "center" }}>
              <label style={{ fontSize: 12, color: "var(--text-2)", width: 180, flexShrink: 0 }}>{l}</label>
              <input defaultValue={v} className="inp" style={{ flex: 1 }} />
            </div>
          ))}
        </div>
      </div>

      <div style={{ paddingTop: 16, borderTop: "1px solid var(--border)" }}>
        <button className="btn-primary">Save Changes</button>
      </div>
    </div>
  );
}
