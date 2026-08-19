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
      last_issue_kind: "none",
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
      last_issue_kind: "none",
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
      last_error: "连接上游超时，线路冷却中",
      last_issue_kind: "transport",
      cooldown_until: new Date(Date.now() + 45000).toISOString(),
      total_requests: 35,
      successful_requests: 29,
      payload_rejected_size: 482112,
      payload_rejected_until: new Date(Date.now() + 8 * 60_000).toISOString(),
      recent_probes: makeProbeHistory(
        "open",
        1450,
        "连接上游超时",
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
      diagnostic_detail: null,
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
    diagnostic_detail: null,
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
    retry_count: 5,
    retry_interval_sec: 1,
    max_retry_wait_sec: 30,
    codex_stream_max_retries: 5,
    retry_unknown_transient_errors: true,
    allow_provider_failover: false,
    capacity_retry_count: 5,
    capacity_retry_interval_sec: 1,
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

function demoMetrics(
  input: number,
  output: number,
  records: number,
  estimatedRatio = 0.12,
): UsageMetrics {
  const estimatedInput = Math.round(input * estimatedRatio);
  const estimatedOutput = Math.round(output * estimatedRatio);
  return {
    input_tokens: { value: input, estimated_value: estimatedInput, reported_count: records },
    output_tokens: { value: output, estimated_value: estimatedOutput, reported_count: records },
    cache_hit_tokens: {
      value: Math.round(input * 0.42),
      estimated_value: 0,
      reported_count: Math.max(0, records - 2),
    },
    total_tokens: {
      value: input + output,
      estimated_value: estimatedInput + estimatedOutput,
      reported_count: records,
    },
  };
}

function sumDemoMetrics(items: UsageMetrics[]): UsageMetrics {
  const sumMetric = (select: (metrics: UsageMetrics) => UsageMetrics["total_tokens"]) =>
    items.reduce(
      (sum, metrics) => {
        const metric = select(metrics);
        return {
          value: sum.value + metric.value,
          estimated_value: sum.estimated_value + metric.estimated_value,
          reported_count: sum.reported_count + metric.reported_count,
        };
      },
      { value: 0, estimated_value: 0, reported_count: 0 },
    );
  return {
    total_tokens: sumMetric((metrics) => metrics.total_tokens),
    input_tokens: sumMetric((metrics) => metrics.input_tokens),
    output_tokens: sumMetric((metrics) => metrics.output_tokens),
    cache_hit_tokens: sumMetric((metrics) => metrics.cache_hit_tokens),
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
  // 演示数据按官方 18%、第三方 77%、未确认 5% 分配，保证线路合计与总览一致。
  const officialInput = Math.round(totals.input * 0.18);
  const officialOutput = Math.round(totals.output * 0.18);
  const providerShares = [0.47, 0.20, 0.10];
  const gatewayProviders = ["主力中转", "备用中转", "经济线路"].map((name, index) => {
    const input = Math.round(totals.input * providerShares[index]);
    const output = Math.round(totals.output * providerShares[index]);
    const completed = Math.round(totals.completed * providerShares[index]);
    const reported = Math.min(completed, Math.round(totals.reported * providerShares[index]));
    return {
      provider_id: ["demo-primary", "demo-backup", "demo-economy"][index],
      provider_name: name,
      deleted: false,
      completed_requests: completed,
      reported_requests: reported,
      coverage_percent: completed ? Math.round((reported / completed) * 1000) / 10 : 0,
      data_quality: index === 0 ? "mixed" as const : "exact" as const,
      metrics: demoMetrics(input, output, reported, index === 0 ? 0.12 : 0),
    };
  });
  const allocatedInput = officialInput + gatewayProviders.reduce(
    (sum, provider) => sum + provider.metrics.input_tokens.value,
    0,
  );
  const allocatedOutput = officialOutput + gatewayProviders.reduce(
    (sum, provider) => sum + provider.metrics.output_tokens.value,
    0,
  );
  const allProviders = [
    {
      provider_id: "_openai_official",
      provider_name: "OpenAI 官方",
      deleted: false,
      completed_requests: Math.round(totals.completed * 0.18),
      reported_requests: Math.round(totals.reported * 0.18),
      coverage_percent: 100,
      data_quality: "exact" as const,
      metrics: demoMetrics(officialInput, officialOutput, Math.round(totals.reported * 0.18), 0),
    },
    ...gatewayProviders,
    {
      provider_id: "_codex_unconfirmed",
      provider_name: "Codex 历史（来源未确认）",
      deleted: false,
      completed_requests: Math.max(1, totals.completed - Math.round(totals.completed * 0.95)),
      reported_requests: Math.max(1, totals.reported - Math.round(totals.reported * 0.95)),
      coverage_percent: 100,
      data_quality: "exact" as const,
      metrics: demoMetrics(
        totals.input - allocatedInput,
        totals.output - allocatedOutput,
        Math.max(1, totals.reported - Math.round(totals.reported * 0.95)),
        0,
      ),
    },
  ];
  const providers = allProviders.filter((provider) => {
    if (query.source === "all") return true;
    if (query.source === "official") return provider.provider_id === "_openai_official";
    if (query.source === "unconfirmed") return provider.provider_id === "_codex_unconfirmed";
    return !provider.provider_id.startsWith("_");
  });
  const selectedMetrics = sumDemoMetrics(providers.map((provider) => provider.metrics));
  const selectedCompleted = providers.reduce((sum, provider) => sum + provider.completed_requests, 0);
  const selectedReported = providers.reduce((sum, provider) => sum + provider.reported_requests, 0);
  const sourceRatio = query.source === "official"
    ? 0.18
    : query.source === "gateway"
      ? 0.77
      : query.source === "unconfirmed"
        ? 0.05
        : 1;
  const selectedSeries = sourceRatio === 1
    ? series
    : series.map((point) => ({
        ...point,
        completed_requests: Math.round(point.completed_requests * sourceRatio),
        reported_requests: Math.round(point.reported_requests * sourceRatio),
        metrics: demoMetrics(
          Math.round(point.metrics.input_tokens.value * sourceRatio),
          Math.round(point.metrics.output_tokens.value * sourceRatio),
          Math.round(point.reported_requests * sourceRatio),
          query.source === "gateway" ? 0.12 : 0,
        ),
      }));
  return {
    period: {
      mode: query.mode,
      start_date: localDateString(start),
      end_date: localDateString(end),
      label,
      resolution,
    },
    totals: selectedMetrics,
    providers,
    series: selectedSeries,
    completed_requests: selectedCompleted,
    reported_requests: selectedReported,
    coverage_percent: selectedCompleted
      ? Math.round((selectedReported / selectedCompleted) * 1000) / 10
      : 0,
    data_quality: providers.some((provider) => provider.data_quality === "mixed") ? "mixed" : "exact",
    codex_sync_status: {
      state: "success",
      last_synced_at: new Date().toISOString(),
      roots_scanned: 1,
      files_scanned: 48,
      token_events_found: 144,
      imported_count: 0,
      matched_count: 0,
      official_events: 0,
      gateway_matched_events: 142,
      gateway_completed_events: 8,
      gateway_unmatched_ignored: 2,
      duplicate_events: 12,
      removed_stale_events: 0,
      unknown_mode_events: 0,
      skipped_count: 0,
      deferred_count: 0,
      parser_version: 3,
      rebuild_performed: false,
      rebuild_status: "idle",
      warning_messages: [],
    },
  };
}
