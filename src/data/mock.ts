export type TopicStatus = "active" | "idle" | "warning" | "error";
export type ConsumerStatus = "active" | "idle" | "lagging" | "disconnected";
export type ProducerStatus = "connected" | "idle" | "disconnected";
export type GroupState = "Stable" | "Rebalancing" | "Empty";
export type LogLevel = "INFO" | "DEBUG" | "WARN" | "ERROR";
export type HealthState = "ONLINE" | "HEALTHY" | "DEGRADED" | "OFFLINE";

export interface Topic {
  id: string;
  partitions: number;
  messages: number;
  producers: number;
  consumers: number;
  latestMessage: string;
  status: TopicStatus;
  incomingRate: number;
  outgoingRate: number;
  retention: string;
  replication: number;
  description: string;
}

export interface Message {
  id: string;
  topic: string;
  partition: number;
  offset: number;
  timestamp: string;
  producer: string;
  key: string;
  payload: string;
  headers: Record<string, string>;
}

export interface Producer {
  id: string;
  service: string;
  topics: string[];
  published: number;
  rate: number;
  lastActivity: string;
  status: ProducerStatus;
  connection: string;
}

export interface Consumer {
  id: string;
  service: string;
  topic: string;
  group: string;
  currentOffset: number;
  latestOffset: number;
  lag: number;
  consumed: number;
  status: ConsumerStatus;
  lastActivity: string;
}

export interface ConsumerGroup {
  id: string;
  members: number;
  topics: string[];
  partitions: number;
  currentOffset: number;
  lag: number;
  state: GroupState;
  coordinator: string;
}

export interface LogEntry {
  id: string;
  timestamp: string;
  level: LogLevel;
  topic: string;
  producer: string;
  consumer: string;
  message: string;
  meta: Record<string, string | number>;
}

export const topics: Topic[] = [
  {
    id: "phone-events",
    partitions: 3,
    messages: 8421,
    producers: 2,
    consumers: 3,
    latestMessage: "2026-09-12 11:24:31",
    status: "active",
    incomingRate: 124,
    outgoingRate: 118,
    retention: "7d",
    replication: 1,
    description: "Mobile telephony events including call lifecycle and SMS events",
  },
  {
    id: "payment-events",
    partitions: 5,
    messages: 6834,
    producers: 1,
    consumers: 2,
    latestMessage: "2026-09-12 11:24:29",
    status: "active",
    incomingRate: 89,
    outgoingRate: 87,
    retention: "30d",
    replication: 1,
    description: "Payment transaction events: initiated, authorized, settled, failed",
  },
  {
    id: "authentication",
    partitions: 2,
    messages: 4211,
    producers: 1,
    consumers: 2,
    latestMessage: "2026-09-12 11:23:58",
    status: "active",
    incomingRate: 43,
    outgoingRate: 41,
    retention: "7d",
    replication: 1,
    description: "User authentication and session lifecycle events",
  },
  {
    id: "notifications",
    partitions: 2,
    messages: 3892,
    producers: 1,
    consumers: 1,
    latestMessage: "2026-09-12 11:20:14",
    status: "idle",
    incomingRate: 0,
    outgoingRate: 0,
    retention: "3d",
    replication: 1,
    description: "Push notification dispatch events",
  },
  {
    id: "system-logs",
    partitions: 1,
    messages: 1489,
    producers: 4,
    consumers: 1,
    latestMessage: "2026-09-12 11:24:30",
    status: "warning",
    incomingRate: 28,
    outgoingRate: 15,
    retention: "14d",
    replication: 1,
    description: "Aggregated service-level logs from all producers",
  },
];

export const messages: Message[] = [
  {
    id: "msg_01J8K9A72F",
    topic: "phone-events",
    partition: 0,
    offset: 1842,
    timestamp: "2026-09-12 11:24:31",
    producer: "phone-service",
    key: "device-1928",
    payload: JSON.stringify({ event: "CALL_STARTED", phoneNumber: "+91XXXXXXXXXX", deviceId: "device-1928", duration: 0 }, null, 2),
    headers: { "content-type": "application/json", "x-service-version": "2.4.1" },
  },
  {
    id: "msg_01J8K9A5QR",
    topic: "payment-events",
    partition: 2,
    offset: 934,
    timestamp: "2026-09-12 11:24:29",
    producer: "payment-service",
    key: "txn-882991",
    payload: JSON.stringify({ event: "PAYMENT_INITIATED", transactionId: "txn-882991", amount: 1499, currency: "INR", userId: "user-4821" }, null, 2),
    headers: { "content-type": "application/json", "x-trace-id": "tr-99214" },
  },
  {
    id: "msg_01J8K9A1MN",
    topic: "authentication",
    partition: 1,
    offset: 471,
    timestamp: "2026-09-12 11:23:58",
    producer: "auth-service",
    key: "session-aa4921",
    payload: JSON.stringify({ event: "LOGIN_SUCCESS", userId: "user-4821", ipAddress: "192.168.1.104", sessionId: "session-aa4921", mfa: true }, null, 2),
    headers: { "content-type": "application/json", "x-service-version": "1.8.0" },
  },
  {
    id: "msg_01J8K98XPQ",
    topic: "system-logs",
    partition: 0,
    offset: 288,
    timestamp: "2026-09-12 11:23:44",
    producer: "payment-service",
    key: "warn-timeout",
    payload: JSON.stringify({ level: "WARN", message: "Upstream payment gateway timeout after 3000ms", service: "payment-service", retries: 2 }, null, 2),
    headers: { "content-type": "application/json", "x-log-level": "WARN" },
  },
  {
    id: "msg_01J8K98TNK",
    topic: "phone-events",
    partition: 1,
    offset: 1841,
    timestamp: "2026-09-12 11:23:41",
    producer: "phone-service",
    key: "device-0441",
    payload: JSON.stringify({ event: "CALL_ENDED", phoneNumber: "+91XXXXXXXXXX", deviceId: "device-0441", duration: 142, quality: "HD" }, null, 2),
    headers: { "content-type": "application/json", "x-service-version": "2.4.1" },
  },
  {
    id: "msg_01J8K98NRM",
    topic: "payment-events",
    partition: 4,
    offset: 933,
    timestamp: "2026-09-12 11:23:29",
    producer: "payment-service",
    key: "txn-882990",
    payload: JSON.stringify({ event: "PAYMENT_SETTLED", transactionId: "txn-882990", amount: 2999, currency: "INR", gateway: "STRIPE", settlementId: "set-4419" }, null, 2),
    headers: { "content-type": "application/json" },
  },
  {
    id: "msg_01J8K98JQP",
    topic: "notifications",
    partition: 0,
    offset: 218,
    timestamp: "2026-09-12 11:20:14",
    producer: "notification-service",
    key: "notif-user4821",
    payload: JSON.stringify({ event: "PUSH_SENT", userId: "user-4821", channel: "FCM", title: "Payment Received", body: "Your payment of ₹1,499 was successful." }, null, 2),
    headers: { "content-type": "application/json" },
  },
  {
    id: "msg_01J8K98FMQ",
    topic: "authentication",
    partition: 0,
    offset: 470,
    timestamp: "2026-09-12 11:19:52",
    producer: "auth-service",
    key: "session-bb2811",
    payload: JSON.stringify({ event: "LOGOUT", userId: "user-3391", sessionId: "session-bb2811", reason: "user_initiated" }, null, 2),
    headers: { "content-type": "application/json", "x-service-version": "1.8.0" },
  },
];

export const producers: Producer[] = [
  {
    id: "prod-001",
    service: "phone-service",
    topics: ["phone-events", "system-logs"],
    published: 8241,
    rate: 124,
    lastActivity: "2026-09-12 11:24:31",
    status: "connected",
    connection: "10.0.0.12:9091",
  },
  {
    id: "prod-002",
    service: "payment-service",
    topics: ["payment-events", "system-logs"],
    published: 6834,
    rate: 89,
    lastActivity: "2026-09-12 11:24:29",
    status: "connected",
    connection: "10.0.0.14:9091",
  },
  {
    id: "prod-003",
    service: "auth-service",
    topics: ["authentication", "system-logs"],
    published: 4211,
    rate: 43,
    lastActivity: "2026-09-12 11:23:58",
    status: "connected",
    connection: "10.0.0.18:9091",
  },
  {
    id: "prod-004",
    service: "notification-service",
    topics: ["notifications", "system-logs"],
    published: 3892,
    rate: 0,
    lastActivity: "2026-09-12 11:20:14",
    status: "idle",
    connection: "10.0.0.22:9091",
  },
];

export const consumers: Consumer[] = [
  { id: "cons-001", service: "analytics-service", topic: "phone-events", group: "analytics-group", currentOffset: 1838, latestOffset: 1842, lag: 4, consumed: 8212, status: "active", lastActivity: "2026-09-12 11:24:30" },
  { id: "cons-002", service: "billing-service", topic: "payment-events", group: "billing-group", currentOffset: 928, latestOffset: 934, lag: 6, consumed: 6721, status: "active", lastActivity: "2026-09-12 11:24:28" },
  { id: "cons-003", service: "fraud-detector", topic: "payment-events", group: "fraud-group", currentOffset: 810, latestOffset: 934, lag: 124, consumed: 6710, status: "lagging", lastActivity: "2026-09-12 11:21:42" },
  { id: "cons-004", service: "audit-service", topic: "authentication", group: "audit-group", currentOffset: 469, latestOffset: 471, lag: 2, consumed: 4198, status: "active", lastActivity: "2026-09-12 11:23:57" },
  { id: "cons-005", service: "session-manager", topic: "authentication", group: "session-group", currentOffset: 390, latestOffset: 471, lag: 81, consumed: 4002, status: "lagging", lastActivity: "2026-09-12 11:18:31" },
  { id: "cons-006", service: "push-gateway", topic: "notifications", group: "push-group", currentOffset: 218, latestOffset: 218, lag: 0, consumed: 3892, status: "idle", lastActivity: "2026-09-12 11:20:14" },
  { id: "cons-007", service: "log-aggregator", topic: "system-logs", group: "log-group", currentOffset: 214, latestOffset: 288, lag: 74, consumed: 1401, status: "lagging", lastActivity: "2026-09-12 11:22:09" },
  { id: "cons-008", service: "crm-service", topic: "phone-events", group: "crm-group", currentOffset: 0, latestOffset: 1842, lag: 1842, consumed: 0, status: "disconnected", lastActivity: "2026-09-12 09:11:22" },
];

export const consumerGroups: ConsumerGroup[] = [
  { id: "analytics-group", members: 2, topics: ["phone-events"], partitions: 3, currentOffset: 5514, lag: 12, state: "Stable", coordinator: "broker-1" },
  { id: "billing-group", members: 1, topics: ["payment-events"], partitions: 5, currentOffset: 4640, lag: 6, state: "Stable", coordinator: "broker-1" },
  { id: "fraud-group", members: 1, topics: ["payment-events"], partitions: 5, currentOffset: 4050, lag: 620, state: "Stable", coordinator: "broker-1" },
  { id: "audit-group", members: 1, topics: ["authentication"], partitions: 2, currentOffset: 938, lag: 4, state: "Rebalancing", coordinator: "broker-1" },
  { id: "session-group", members: 2, topics: ["authentication"], partitions: 2, currentOffset: 780, lag: 162, state: "Stable", coordinator: "broker-1" },
  { id: "push-group", members: 1, topics: ["notifications"], partitions: 2, currentOffset: 436, lag: 0, state: "Stable", coordinator: "broker-1" },
  { id: "log-group", members: 1, topics: ["system-logs"], partitions: 1, currentOffset: 214, lag: 74, state: "Stable", coordinator: "broker-1" },
  { id: "crm-group", members: 0, topics: ["phone-events"], partitions: 0, currentOffset: 0, lag: 0, state: "Empty", coordinator: "broker-1" },
];

const seed = (n: number) => {
  let x = Math.sin(n + 1) * 10000;
  return x - Math.floor(x);
};

const baseTime = new Date("2026-09-12T11:24:00");
export const activityData = Array.from({ length: 24 }, (_, i) => {
  const t = new Date(baseTime.getTime() - (23 - i) * 5 * 60 * 1000);
  const h = String(t.getHours()).padStart(2, "0");
  const m = String(t.getMinutes()).padStart(2, "0");
  const base = 85 + Math.sin(i * 0.4) * 25;
  return {
    time: `${h}:${m}`,
    published: Math.round(base + seed(i) * 30),
    consumed: Math.round(base - 5 + seed(i + 7) * 28),
    rate: Math.round(base + seed(i + 3) * 20),
  };
});

export const metricsData = {
  "5m": Array.from({ length: 12 }, (_, i) => ({
    time: `${String(11).padStart(2,"0")}:${String(Math.max(0, 24 - (11 - i))).padStart(2,"0")}`,
    published: Math.round(100 + seed(i) * 40),
    consumed: Math.round(95 + seed(i + 5) * 35),
    producerTp: Math.round(140 + seed(i + 2) * 50),
    consumerTp: Math.round(130 + seed(i + 9) * 45),
    lag: Math.round(50 + seed(i + 1) * 80),
    connections: Math.round(8 + seed(i + 4) * 4),
    memory: Math.round(62 + seed(i + 6) * 15),
    cpu: Math.round(28 + seed(i + 8) * 20),
  })),
  "15m": Array.from({ length: 20 }, (_, i) => ({
    time: `${String(Math.floor(11 - (19 - i) * 0.25)).padStart(2,"0")}:${String(Math.round(((19 - i) * 0.25 % 1) * -60 + 24)).padStart(2,"0")}`,
    published: Math.round(100 + seed(i + 10) * 40),
    consumed: Math.round(95 + seed(i + 15) * 35),
    producerTp: Math.round(140 + seed(i + 12) * 50),
    consumerTp: Math.round(130 + seed(i + 19) * 45),
    lag: Math.round(50 + seed(i + 11) * 80),
    connections: Math.round(8 + seed(i + 14) * 4),
    memory: Math.round(62 + seed(i + 16) * 15),
    cpu: Math.round(28 + seed(i + 18) * 20),
  })),
  "1h": Array.from({ length: 24 }, (_, i) => ({
    time: `${String(Math.floor(10 + i / 4)).padStart(2,"0")}:${String((i % 4) * 15).padStart(2,"0")}`,
    published: Math.round(100 + seed(i + 20) * 40),
    consumed: Math.round(95 + seed(i + 25) * 35),
    producerTp: Math.round(140 + seed(i + 22) * 50),
    consumerTp: Math.round(130 + seed(i + 29) * 45),
    lag: Math.round(50 + seed(i + 21) * 80),
    connections: Math.round(8 + seed(i + 24) * 4),
    memory: Math.round(62 + seed(i + 26) * 15),
    cpu: Math.round(28 + seed(i + 28) * 20),
  })),
};

export const logEntries: LogEntry[] = [
  { id: "log-001", timestamp: "2026-09-12 11:24:31.882", level: "INFO", topic: "phone-events", producer: "phone-service", consumer: "", message: "Message published: msg_01J8K9A72F | offset=1842 partition=0", meta: { latency_ms: 2, size_bytes: 184 } },
  { id: "log-002", timestamp: "2026-09-12 11:24:30.441", level: "INFO", topic: "system-logs", producer: "auth-service", consumer: "", message: "Producer heartbeat acknowledged | producer=prod-003", meta: { interval_ms: 3000 } },
  { id: "log-003", timestamp: "2026-09-12 11:24:29.219", level: "INFO", topic: "payment-events", producer: "payment-service", consumer: "", message: "Message published: msg_01J8K9A5QR | offset=934 partition=2", meta: { latency_ms: 3, size_bytes: 201 } },
  { id: "log-004", timestamp: "2026-09-12 11:24:28.114", level: "DEBUG", topic: "payment-events", producer: "", consumer: "billing-service", message: "Consumer poll complete | fetched=6 lag=6", meta: { poll_ms: 12 } },
  { id: "log-005", timestamp: "2026-09-12 11:24:20.007", level: "WARN", topic: "system-logs", producer: "payment-service", consumer: "", message: "Upstream payment gateway timeout after 3000ms — retrying (attempt 2/3)", meta: { timeout_ms: 3000, retry: 2 } },
  { id: "log-006", timestamp: "2026-09-12 11:23:58.882", level: "INFO", topic: "authentication", producer: "auth-service", consumer: "", message: "Message published: msg_01J8K9A1MN | offset=471 partition=1", meta: { latency_ms: 1, size_bytes: 156 } },
  { id: "log-007", timestamp: "2026-09-12 11:23:44.332", level: "WARN", topic: "system-logs", producer: "payment-service", consumer: "", message: "Consumer lag for fraud-group on payment-events exceeds threshold: 124", meta: { lag: 124, threshold: 100 } },
  { id: "log-008", timestamp: "2026-09-12 11:22:09.991", level: "WARN", topic: "system-logs", producer: "", consumer: "log-aggregator", message: "Consumer lag for log-group on system-logs growing: 74 messages behind", meta: { lag: 74, trend: "increasing" } },
  { id: "log-009", timestamp: "2026-09-12 11:21:42.001", level: "ERROR", topic: "payment-events", producer: "", consumer: "fraud-detector", message: "Consumer fraud-detector failed to commit offset: connection reset by peer", meta: { partition: 3, offset: 810 } },
  { id: "log-010", timestamp: "2026-09-12 11:20:14.772", level: "INFO", topic: "notifications", producer: "notification-service", consumer: "", message: "Message published: msg_01J8K98JQP | offset=218 partition=0", meta: { latency_ms: 2, size_bytes: 212 } },
  { id: "log-011", timestamp: "2026-09-12 11:19:52.441", level: "INFO", topic: "authentication", producer: "auth-service", consumer: "", message: "Message published: msg_01J8K98FMQ | offset=470 partition=0", meta: { latency_ms: 1, size_bytes: 148 } },
  { id: "log-012", timestamp: "2026-09-12 11:18:31.009", level: "WARN", topic: "authentication", producer: "", consumer: "session-manager", message: "Consumer group session-group rebalance triggered: member left", meta: { members_before: 3, members_after: 2 } },
  { id: "log-013", timestamp: "2026-09-12 11:15:22.881", level: "DEBUG", topic: "phone-events", producer: "phone-service", consumer: "", message: "Batch published: 12 messages | partition=0 offsets=1829-1840", meta: { batch_size: 12 } },
  { id: "log-014", timestamp: "2026-09-12 11:11:04.221", level: "ERROR", topic: "system-logs", producer: "", consumer: "crm-service", message: "Consumer crm-service disconnected — missed 3 heartbeat intervals", meta: { group: "crm-group", heartbeat_timeout_ms: 30000 } },
  { id: "log-015", timestamp: "2026-09-12 11:08:48.114", level: "INFO", topic: "phone-events", producer: "", consumer: "analytics-service", message: "Consumer group analytics-group rebalance complete | partitions=[0,1,2]", meta: { duration_ms: 412 } },
];
