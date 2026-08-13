import type {
  EventLog,
  HealthSnapshot,
  Provider,
  RouteGroup,
  RouterSettings,
  RuntimeStats,
  RoutingPolicy,
  RoutingEvent,
  UsageQuery,
  UsageStatistics,
  UsageMetrics,
} from "./api";

export interface DemoData {
  providers: Provider[];
  health: HealthSnapshot[];
  settings: RouterSettings;
  routeGroup: RouteGroup;
  runtime: RuntimeStats;
  logs: EventLog[];
  routingPolicy: RoutingPolicy;
}

// 仅供普通浏览器中的视觉验收使用；Tauri 桌面运行时始终读取真实网关数据。
export function buildDemoData(): DemoData {
  const providers: Provider[] = [
    {
      id: "demo-primary",
      name: "主力中转",
      base_url: "https://api.primary.example/v1",
      models: ["gpt-*"],
      priority: 1,
      enabled: true,
      timeout_ms: 120000,
      has_api_key: true,
      api_key_error: null,
    },
    {
      id: "demo-backup",
      name: "备用中转",
      base_url: "https://api.backup.example/v1",
      models: [],
      priority: 2,
      enabled: true,
      timeout_ms: 120000,
      has_api_key: true,
      api_key_error: null,
    },
    {
      id: "demo-economy",
      name: "经济线路",
      base_url: "https://api.economy.example/v1",
      models: ["gpt-5*"],
      priority: 3,
      enabled: true,
      timeout_ms: 90000,
      has_api_key: true,
      api_key_error: null,
    },
  ];
  const makeProbeHistory = (
    state: HealthSnapshot["state"],
    latencyMs: number | null,
    error: string | null = null,
  ): HealthSnapshot["recent_probes"] =>
    Array.from({ length: 12 }, (_, index) => ({
      sequence: index + 1,
      timestamp: new Date(Date.now() - (11 - index) * 60_000).toISOString(),
      state,
      latency_ms: latencyMs,
      error,
    }));
  const health: HealthSnapshot[] = [
    {
      provider_id: "demo-primary",
      state: "healthy",
      latency_ms: 386,
      consecutive_failures: 0,
      last_error: null,
      cooldown_until: null,
      total_requests: 128,
      successful_requests: 127,
      payload_rejected_size: null,
      payload_rejected_until: null,
      recent_probes: makeProbeHistory("healthy", 386),
    },
    {
      provider_id: "demo-backup",
      state: "healthy",
      latency_ms: 612,
      consecutive_failures: 0,
      last_error: null,
      cooldown_until: null,
      total_requests: 42,
      successful_requests: 42,
      payload_rejected_size: null,
      payload_rejected_until: null,
      recent_probes: makeProbeHistory("healthy", 612),
    },
    {
      provider_id: "demo-economy",
      state: "open",
      latency_ms: 1450,
      consecutive_failures: 3,
      last_error: "上游返回 429 Too Many Requests，冷却中",
      cooldown_until: new Date(Date.now() + 45000).toISOString(),
      total_requests: 35,
      successful_requests: 29,
      payload_rejected_size: 482112,
      payload_rejected_until: new Date(Date.now() + 8 * 60_000).toISOString(),
      recent_probes: makeProbeHistory(
        "open",
        1450,
        "探测返回 429 Too Many Requests",
      ),
    },
  ];
  const demoProviderIds = providers.map((provider) => provider.id);
  const demoModels = ["gpt-5.2-codex", "gpt-5.1-codex-mini", "gpt-4.1"];
  const switchEvent: RoutingEvent = {
    id: "demo-switch-event",
    timestamp: new Date(Date.now() - 2 * 60_000).toISOString(),
    event_type: "auto_switch",
    model: "gpt-5.2-codex",
    request_path: "/v1/responses",
    request_body_bytes: 482_112,
    source_provider_id: "demo-primary",
    target_provider_id: "demo-backup",
    status_code: 200,
    retry_count: 0,
    attempts: [
      { provider_id: "demo-primary", provider_name: "主力中转", status_code: 413, error: "上游返回 413，拒绝 482112 字节请求体", latency_ms: 386, retry_count: 0, switched: true },
      { provider_id: "demo-backup", provider_name: "备用中转", status_code: 200, error: null, latency_ms: 612, retry_count: 0, switched: false },
    ],
    reason: "上游返回 413，拒绝 482112 字节请求体",
    final_result: "switched",
  };
  const logs: EventLog[] = Array.from({ length: 126 }, (_, index) => {
    const isFailure = index % 17 === 0;
    const isProbe = index % 11 === 0 && !isFailure;
    return {
      timestamp: new Date(Date.now() - index * 23000).toISOString(),
      level: isFailure ? "warning" : "info",
      message: isFailure
        ? "上游返回 429，已切换到下一个可用中转"
        : isProbe
          ? "健康探测成功，延迟 612 ms"
          : `已建立上游响应，响应头延迟 ${360 + (index % 9) * 47} ms`,
      provider_id: demoProviderIds[index % demoProviderIds.length],
      model: isProbe ? null : demoModels[index % demoModels.length],
      event_type: null,
      routing_event: null,
    };
  });
  logs.unshift({
    timestamp: switchEvent.timestamp,
    level: "warning",
    message: "主力中转不可用，已切换到备用中转",
    provider_id: "demo-backup",
    model: switchEvent.model,
    event_type: "auto_switch",
    routing_event: switchEvent,
  });
  return {
    providers,
    health,
    settings: {
      bind_address: "127.0.0.1",
      port: 8765,
      local_token: "demo-local-token",
      probe_interval_sec: 60,
      log_retention_days: 7,
      usage_retention_days: 365,
      gateway_running: true,
      gateway_listening: true,
      gateway_error: null,
    },
    routeGroup: {
      id: "default",
      name: "默认路由",
      provider_ids: demoProviderIds,
      locked_provider_id: null,
    },
    runtime: {
      total_requests: 205,
      successful_requests: 198,
      average_latency_ms: 816,
      active_provider_id: "demo-primary",
    },
    logs,
    routingPolicy: {
    retry_count: 1,
    retry_interval_sec: 1,
    max_retry_wait_sec: 10,
    capacity_retry_count: 3,
    capacity_retry_interval_sec: 5,
    capacity_max_retry_wait_sec: 30,
      circuit_failure_threshold: 3,
      circuit_cooldown_sec: 60,
      payload_too_large_skip_sec: 600,
      payload_too_large_action: "failover",
      notify_switches: true,
    },
  };
}

function localDateString(date: Date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function demoMetrics(input: number, output: number, records: number): UsageMetrics {
  return {
    input_tokens: { value: input, reported_count: records },
    output_tokens: { value: output, reported_count: records },
    cached_tokens: {
      value: Math.round(input * 0.42),
      reported_count: Math.max(0, records - 2),
    },
    reasoning_tokens: {
      value: Math.round(output * 0.31),
      reported_count: Math.max(0, records - 4),
    },
    total_tokens: { value: input + output, reported_count: records },
  };
}

// 演示统计跟随筛选生成，便于在普通浏览器验证今天、范围和月份三种布局。
export function buildDemoUsageStatistics(query: UsageQuery): UsageStatistics {
  const now = new Date();
  let start = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  let end = new Date(start);
  let label = "今天";
  let resolution: "hour" | "day" = "hour";
  if (query.mode === "range" && query.start_date && query.end_date) {
    start = new Date(`${query.start_date}T00:00:00`);
    end = new Date(`${query.end_date}T00:00:00`);
    label = `${query.start_date} 至 ${query.end_date}`;
    resolution = "day";
  } else if (query.mode === "month" && query.month) {
    const [year, month] = query.month.split("-").map(Number);
    start = new Date(year, month - 1, 1);
    end = new Date(year, month, 0);
    label = `${year}年${month}月`;
    resolution = "day";
  }
  const bucketCount = resolution === "hour"
    ? 24
    : Math.round((end.getTime() - start.getTime()) / 86_400_000) + 1;
  const series = Array.from({ length: bucketCount }, (_, index) => {
    const input = 8_400 + ((index * 2_713) % 18_000);
    const output = 2_100 + ((index * 1_177) % 6_800);
    const records = 4 + (index % 8);
    const date = new Date(start);
    date.setDate(start.getDate() + index);
    return {
      key: resolution === "hour" ? String(index).padStart(2, "0") : localDateString(date),
      label: resolution === "hour"
        ? `${String(index).padStart(2, "0")}:00`
        : `${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`,
      completed_requests: records + (index % 3 === 0 ? 1 : 0),
      reported_requests: records,
      metrics: demoMetrics(input, output, records),
    };
  });
  const totals = series.reduce(
    (sum, point) => ({
      input: sum.input + point.metrics.input_tokens.value,
      output: sum.output + point.metrics.output_tokens.value,
      completed: sum.completed + point.completed_requests,
      reported: sum.reported + point.reported_requests,
    }),
    { input: 0, output: 0, completed: 0, reported: 0 },
  );
  const providerShares = [0.62, 0.27, 0.11];
  const providers = ["主力中转", "备用中转", "经济线路"].map((name, index) => {
    const input = index === 2
      ? totals.input - Math.round(totals.input * providerShares[0]) - Math.round(totals.input * providerShares[1])
      : Math.round(totals.input * providerShares[index]);
    const output = index === 2
      ? totals.output - Math.round(totals.output * providerShares[0]) - Math.round(totals.output * providerShares[1])
      : Math.round(totals.output * providerShares[index]);
    const completed = Math.round(totals.completed * providerShares[index]);
    const reported = Math.min(completed, Math.round(totals.reported * providerShares[index]));
    return {
      provider_id: ["demo-primary", "demo-backup", "demo-economy"][index],
      provider_name: name,
      deleted: false,
      completed_requests: completed,
      reported_requests: reported,
      coverage_percent: completed ? Math.round((reported / completed) * 1000) / 10 : 0,
      metrics: demoMetrics(input, output, reported),
    };
  });
  return {
    period: {
      mode: query.mode,
      start_date: localDateString(start),
      end_date: localDateString(end),
      label,
      resolution,
    },
    totals: demoMetrics(totals.input, totals.output, totals.reported),
    providers,
    series,
    completed_requests: totals.completed,
    reported_requests: totals.reported,
    coverage_percent: totals.completed
      ? Math.round((totals.reported / totals.completed) * 1000) / 10
      : 0,
  };
}
