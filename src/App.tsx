import { useState } from "react";
import Sidebar, { type Page } from "./components/Sidebar";
import TopBar from "./components/TopBar";
import Overview from "./pages/Overview";
import Topics from "./pages/Topics";
import TopicDetail from "./pages/TopicDetail";
import Producers from "./pages/Producers";
import Consumers from "./pages/Consumers";
import ConsumerGroups from "./pages/ConsumerGroups";
import Logs from "./pages/Logs";
import Metrics from "./pages/Metrics";
import SystemHealth from "./pages/SystemHealth";
import Messages from "./pages/Messages";
import Settings from "./pages/Settings";

interface NavState {
  page: Page;
  params: Record<string, string>;
}

const pageMeta: Record<Page, { title: string; breadcrumbs: string[] }> = {
  overview:         { title: "Overview",         breadcrumbs: [] },
  topics:           { title: "Topics",           breadcrumbs: ["Mini Kafka"] },
  "topic-detail":   { title: "Topic Detail",     breadcrumbs: ["Mini Kafka", "Topics"] },
  messages:         { title: "Message Explorer", breadcrumbs: ["Mini Kafka"] },
  consumers:        { title: "Consumers",        breadcrumbs: ["Mini Kafka"] },
  producers:        { title: "Producers",        breadcrumbs: ["Mini Kafka"] },
  "consumer-groups":{ title: "Consumer Groups",  breadcrumbs: ["Mini Kafka"] },
  logs:             { title: "Logs",             breadcrumbs: ["Mini Kafka"] },
  metrics:          { title: "Metrics",          breadcrumbs: ["Mini Kafka"] },
  "system-health":  { title: "System Health",    breadcrumbs: ["Mini Kafka"] },
  settings:         { title: "Settings",         breadcrumbs: ["Mini Kafka"] },
};

export default function App() {
  const [nav, setNav] = useState<NavState>({ page: "overview", params: {} });

  const navigate = (page: Page, params: Record<string, string> = {}) => {
    setNav({ page, params });
    window.scrollTo({ top: 0 });
  };

  const meta = pageMeta[nav.page];
  const breadcrumbs = nav.page === "topic-detail" && nav.params.topicId
    ? [...meta.breadcrumbs, nav.params.topicId]
    : meta.breadcrumbs;
  const title = nav.page === "topic-detail" && nav.params.topicId
    ? nav.params.topicId
    : meta.title;

  return (
    <div style={{ minHeight: "100vh", background: "var(--bg)" }}>
      <Sidebar current={nav.page} navigate={navigate} />
      <TopBar title={title} breadcrumbs={breadcrumbs} />

      <main style={{ marginLeft: 200, paddingTop: 44, minHeight: "100vh" }}>
        <div style={{ maxWidth: 1400, padding: "28px 32px" }}>
          {nav.page === "overview"         && <Overview navigate={navigate} />}
          {nav.page === "topics"           && <Topics navigate={navigate} />}
          {nav.page === "topic-detail"     && <TopicDetail topicId={nav.params.topicId ?? "phone-events"} navigate={navigate} />}
          {nav.page === "producers"        && <Producers />}
          {nav.page === "consumers"        && <Consumers />}
          {nav.page === "consumer-groups"  && <ConsumerGroups />}
          {nav.page === "logs"             && <Logs />}
          {nav.page === "metrics"          && <Metrics />}
          {nav.page === "system-health"    && <SystemHealth />}
          {nav.page === "messages"         && <Messages />}
          {nav.page === "settings"         && <Settings />}
        </div>
      </main>
    </div>
  );
}
