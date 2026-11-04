import { useCallback, useEffect, useId, useLayoutEffect, useRef, useState } from "react";
import type { FormEvent, ReactNode } from "react";
import { createPortal } from "react-dom";
import { listen } from "@tauri-apps/api/event";
import { open, save } from "@tauri-apps/plugin-dialog";
import {
  Activity,
  BarChart3,
  Bot,
  ArrowDown,
  ArrowUp,
  Check,
  ChevronLeft,
  ChevronRight,
  CircleHelp,
  Copy,
  Download,
  FileKey,
  Gauge,
  Info,
  KeyRound,
  Minus,
  Pencil,
  Pin,
  PinOff,
  Plus,
  RefreshCw,
  Router,
  Save,
  ScrollText,
  Server,
  Settings,
  ShieldCheck,
  Trash2,
  Upload,
  X,
} from "lucide-react";
import {
  deleteProvider,
  EventLog,
  getHealthSnapshots,
  getRecentLogs,
  getRuntimeStats,
  listProviders,
  listRouteGroups,
  Provider,
  ProviderHealth,
  RouteGroup,
  RuntimeStats,
  reorderProviders,
  saveProvider,
  setProviderEnabled,
  testProvider,
  testLocalGateway,
  updateRouteGroup,
  HealthSnapshot,
  RouterSettings,
  GatewayTestResult,
  RoutingEvent,
  DiagnosticDetail,
  RoutingPolicy,
  ConfigurationInput,
  ConfigurationSnapshot,
  getConfiguration,
  saveConfiguration,
  HealthProbeSample,
  BackupExportResult,
  BackupImportMode,
  BackupImportResult,
  BackupPreview,
  exportConfigurationBackup,
  importConfigurationBackup,
  inspectConfigurationBackup,
  getUsageStatistics,
  UsageQuery,
  UsageStatistics,
  TokenMetric,
  CodexIntegrationState,
  CodexHistoryCompatibilityState,
  CodexHistoryMigrationPreview,
  CodexConnectionMode,
  getCodexIntegrationState,
  getCodexHistoryCompatibility,
  switchCodexConnectionMode,
  refreshCodexLoginStatus,
  repairCodexGatewayConfiguration,
  previewCodexHistoryMigration,
  applyCodexHistoryMigration,
  restoreCodexHistoryMigration,
  syncCodexUsage,
  rebuildCodexUsage,
} from "./api";
import { buildDemoData, buildDemoUsageStatistics } from "./demo";

const statusLabels: Record<ProviderHealth, string> = {
  healthy: "运行正常",
  degraded: "性能下降",
  open: "近期不稳定",
  half_open: "等待试探",
  unknown: "等待探测",
};

function monitorStatusLabel(
  snapshot: HealthSnapshot | undefined,
  failoverEnabled: boolean,
) {
  if (snapshot?.last_issue_kind === "model") return "模型暂不可用";
  if (snapshot?.state === "open") {
    return failoverEnabled ? "已熔断" : "近期不稳定";
  }
  if (snapshot?.state === "half_open") return "等待半开试探";
  return statusLabels[snapshot?.state ?? "unknown"];
}

// 与后端保留上限一致，空白占位和真实状态始终组成固定的 16 格。
const MAX_HEALTH_PROBES = 16;
const STATUS_REFRESH_MS = 15_000;

type AppNotice = {
  level: "info" | "warning" | "error";
  title: string;
  message: string;
  routingEvent?: RoutingEvent;
};

type ProviderDrawerState =
  | { mode: "closed" }
  | { mode: "create" }
  | { mode: "detail" | "edit"; providerId: string };
type BackupDialogState =
  | { mode: "closed" }
  | { mode: "export" }
  | { mode: "import"; path: string };
type AppView = "monitor" | "usage" | "providers" | "codex" | "logs" | "settings";
type ConfigurationSaveStatus = "idle" | "saving" | "saved" | "error";

const DEFAULT_ROUTING_POLICY: RoutingPolicy = {
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
};

function configurationInputFromSnapshot(
  snapshot: ConfigurationSnapshot,
): ConfigurationInput {
  return {
    port: snapshot.router_settings.port,
    probe_interval_sec: snapshot.router_settings.probe_interval_sec,
    log_retention_days: snapshot.router_settings.log_retention_days,
    usage_retention_days: snapshot.router_settings.usage_retention_days,
    routing_policy: { ...snapshot.routing_policy },
    autostart_enabled: snapshot.autostart_enabled,
  };
}

function configurationInputsEqual(
  left: ConfigurationInput | null,
  right: ConfigurationInput | null,
) {
  if (!left || !right) return left === right;
  return (
    left.port === right.port &&
    left.probe_interval_sec === right.probe_interval_sec &&
    left.log_retention_days === right.log_retention_days &&
    left.usage_retention_days === right.usage_retention_days &&
    left.autostart_enabled === right.autostart_enabled &&
    JSON.stringify(left.routing_policy) === JSON.stringify(right.routing_policy)
  );
}

function validateConfigurationDraft(draft: ConfigurationInput) {
  const checks: Array<[number, number, number | null, string]> = [
    [draft.port, 1024, 65535, "监听端口"],
    [draft.probe_interval_sec, 5, 3600, "健康探测间隔"],
    [draft.log_retention_days, 1, 365, "日志保留天数"],
    [draft.usage_retention_days, 30, 3650, "用量数据保留天数"],
    [draft.routing_policy.retry_count, 0, null, "重试次数"],
    [draft.routing_policy.retry_interval_sec, 1, 60, "重试间隔"],
    [draft.routing_policy.max_retry_wait_sec, 0, 600, "最大累计等待"],
    [draft.routing_policy.codex_stream_max_retries, 0, null, "Codex 流恢复次数"],
    [draft.routing_policy.circuit_failure_threshold, 1, 10, "连续失败阈值"],
    [draft.routing_policy.circuit_cooldown_sec, 10, 3600, "熔断冷却时间"],
    [draft.routing_policy.payload_too_large_skip_sec, 0, 86400, "413 跳过时间"],
  ];
  for (const [value, min, max, label] of checks) {
    if (!Number.isSafeInteger(value) || value < min || (max !== null && value > max)) {
      return max === null
        ? `${label}必须是大于等于 ${min} 的安全整数`
        : `${label}必须在 ${min} 到 ${max} 之间`;
    }
  }
  return null;
}

function localDateValue(date: Date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function shiftLocalDate(value: string, days: number) {
  const date = new Date(`${value}T00:00:00`);
  date.setDate(date.getDate() + days);
  return localDateValue(date);
}

const TODAY_VALUE = localDateValue(new Date());
const CURRENT_MONTH_VALUE = TODAY_VALUE.slice(0, 7);

function formatTime(value: string) {
  return new Date(value).toLocaleString("zh-CN", { hour12: false });
}
const DEFAULT_USAGE_QUERY: UsageQuery = { mode: "today", source: "all" };

function defaultUsageDraft(): UsageQuery {
  return {
    mode: "today",
    source: "all",
    start_date: shiftLocalDate(TODAY_VALUE, -6),
    end_date: TODAY_VALUE,
    month: CURRENT_MONTH_VALUE,
  };
}

function validateUsageQuery(query: UsageQuery) {
  if (query.mode === "range") {
    if (!query.start_date || !query.end_date) return "请选择开始和结束日期";
    if (query.start_date > query.end_date) return "开始日期不能晚于结束日期";
    if (query.end_date > TODAY_VALUE) return "不能查询未来日期";
    const days = Math.round(
      (new Date(`${query.end_date}T00:00:00`).getTime() -
        new Date(`${query.start_date}T00:00:00`).getTime()) /
        86_400_000,
    ) + 1;
    if (days > 30) return "自定义日期范围最多查询 30 个自然日";
  }
  if (query.mode === "month") {
    if (!query.month) return "请选择要查询的月份";
    if (query.month > CURRENT_MONTH_VALUE) return "不能查询未来月份";
  }
  return null;
}

function backupDefaultFileName() {
  const now = new Date();
  const pad = (value: number) => String(value).padStart(2, "0");
  return `AI-Gateway-backup-${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}-${pad(now.getHours())}${pad(now.getMinutes())}.aigateway`;
}

function backupFileName(path: string) {
  return path.split(/[\\/]/).pop() || "AI Gateway 配置备份";
}

function App() {
  const [providers, setProviders] = useState<Provider[]>([]);
  const [health, setHealth] = useState<HealthSnapshot[]>([]);
  const [settings, setSettings] = useState<RouterSettings | null>(null);
  const [routeGroup, setRouteGroup] = useState<RouteGroup | null>(null);
  const [runtime, setRuntime] = useState<RuntimeStats | null>(null);
  const [usageStatistics, setUsageStatistics] = useState<UsageStatistics | null>(null);
  const [usageDraft, setUsageDraft] = useState<UsageQuery>(() => defaultUsageDraft());
  const [usageQuery, setUsageQuery] = useState<UsageQuery>(DEFAULT_USAGE_QUERY);
  const [usageLoading, setUsageLoading] = useState(false);
  const [usageError, setUsageError] = useState<string | null>(null);
  const [routingPolicy, setRoutingPolicy] = useState<RoutingPolicy | null>(null);
  const [codexState, setCodexState] = useState<CodexIntegrationState | null>(null);
  const [codexHistoryCompatibility, setCodexHistoryCompatibility] =
    useState<CodexHistoryCompatibilityState | null>(null);
  const [codexBusy, setCodexBusy] = useState(false);
  const [codexError, setCodexError] = useState<string | null>(null);
  const [configurationSnapshot, setConfigurationSnapshot] =
    useState<ConfigurationSnapshot | null>(null);
  const [configurationDraft, setConfigurationDraft] =
    useState<ConfigurationInput | null>(null);
  const [configurationSaveStatus, setConfigurationSaveStatus] =
    useState<ConfigurationSaveStatus>("idle");
  const [configurationSaveError, setConfigurationSaveError] = useState<
    string | null
  >(null);
  const [configurationSavedAt, setConfigurationSavedAt] =
    useState<Date | null>(null);
  const [logs, setLogs] = useState<EventLog[]>([]);
  const [view, setView] = useState<AppView>("monitor");
  const [pendingView, setPendingView] = useState<AppView | null>(null);
  // 抽屉状态显式包含模式和目标 ID，后台轮询不能据列表选中项把新增表单误切为编辑。
  const [providerDrawer, setProviderDrawer] = useState<ProviderDrawerState>({
    mode: "closed",
  });
  const [providerFormDirty, setProviderFormDirty] = useState(false);
  const [providerFormSaving, setProviderFormSaving] = useState(false);
  const [providerClosePending, setProviderClosePending] = useState(false);
  const [providerDrawerClosing, setProviderDrawerClosing] = useState(false);
  const [backupDialog, setBackupDialog] = useState<BackupDialogState>({
    mode: "closed",
  });
  const [notice, setNotice] = useState<AppNotice | string | null>(null);
  const [routingDetail, setRoutingDetail] = useState<RoutingEvent | null>(null);
  const [diagnosticDetail, setDiagnosticDetail] = useState<DiagnosticDetail | null>(null);
  const [lastUpdatedAt, setLastUpdatedAt] = useState<Date | null>(null);
  const configurationDirty = !configurationInputsEqual(
    configurationDraft,
    configurationSnapshot
      ? configurationInputFromSnapshot(configurationSnapshot)
      : null,
  );

  const refresh = useCallback(async () => {
    try {
      const [nextProviders, nextHealth, nextConfiguration, groups, stats, events, nextCodexState] =
        await Promise.all([
          listProviders(),
          getHealthSnapshots(),
          getConfiguration(),
          listRouteGroups(),
          getRuntimeStats(),
          getRecentLogs(),
          getCodexIntegrationState(),
        ]);
      setProviders(nextProviders);
      setHealth(nextHealth);
      setSettings(nextConfiguration.router_settings);
      setRoutingPolicy(nextConfiguration.routing_policy);
      setConfigurationSnapshot(nextConfiguration);
      setRouteGroup(groups[0] ?? null);
      setRuntime(stats);
      setLogs(events);
      setCodexState(nextCodexState);
      setLastUpdatedAt(new Date());
      if (nextConfiguration.codex_restart_required) {
        setNotice({
          level: "warning",
          title: "Codex 流恢复配置已升级",
          message: "配置已修改，请完全退出并重新启动 Codex。",
        });
      }
    } catch {
      // demo 查询参数只用于普通浏览器视觉验收，打包后的桌面应用不会使用演示数据。
      if (import.meta.env.DEV && location.search.includes("demo=1")) {
        const demo = buildDemoData();
        setProviders(demo.providers);
        setHealth(demo.health);
        setSettings(demo.settings);
        setRoutingPolicy(demo.routingPolicy);
        setConfigurationSnapshot((current) => current ?? {
          router_settings: demo.settings,
          routing_policy: demo.routingPolicy,
          autostart_enabled: false,
          codex_restart_required: false,
        });
        setRouteGroup(demo.routeGroup);
        setRuntime(demo.runtime);
        setLogs(demo.logs);
        setCodexState((current) => current ?? {
          desktop_status: "installed_running",
          cli_status: "installed_stopped",
          login_status: "logged_in",
          current_mode: "gateway",
          current_provider_id: "ai_gateway",
          gateway_configured: true,
          history_native_supported: true,
          history_probe_error: null,
          history_migration_required: false,
          history_provider_counts: [],
        });
        setLastUpdatedAt(new Date());
      }
    }
  }, []);

  const refreshUsage = useCallback(async (query: UsageQuery) => {
    setUsageLoading(true);
    setUsageError(null);
    try {
      setUsageStatistics(await getUsageStatistics(query));
    } catch (error) {
      if (import.meta.env.DEV && location.search.includes("demo=1")) {
        setUsageStatistics(buildDemoUsageStatistics(query));
      } else {
        setUsageError(String(error));
      }
    } finally {
      setUsageLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => void refresh(), STATUS_REFRESH_MS);
    return () => window.clearInterval(timer);
  }, [refresh]);

  useEffect(() => {
    if (view !== "usage") return;
    void refreshUsage(usageQuery);
    const timer = window.setInterval(
      () => void refreshUsage(usageQuery),
      STATUS_REFRESH_MS,
    );
    return () => window.clearInterval(timer);
  }, [refreshUsage, usageQuery, view]);

  useEffect(() => {
    if (view !== "codex") return;
    void getCodexHistoryCompatibility()
      .then(setCodexHistoryCompatibility)
      .catch((error) => {
        setCodexHistoryCompatibility({
          native_supported: false,
          probe_error: String(error),
          migration_required: false,
          normal_session_count: 0,
          archived_session_count: 0,
          encrypted_content_session_count: 0,
          provider_counts: [],
          can_restore_latest: false,
        });
      });
  }, [view]);

  useEffect(() => {
    let unlisten: (() => void) | undefined;
    let refreshTimer: number | undefined;
    void listen("usage-updated", () => {
      if (view !== "usage") return;
      window.clearTimeout(refreshTimer);
      refreshTimer = window.setTimeout(() => void refreshUsage(usageQuery), 500);
    }).then((cleanup) => {
      unlisten = cleanup;
    }).catch(() => undefined);
    return () => {
      unlisten?.();
      window.clearTimeout(refreshTimer);
    };
  }, [refreshUsage, usageQuery, view]);

  useEffect(() => {
    // 轮询只在没有编辑草稿时同步配置；用户正在输入时不能被后台快照覆盖。
    if (configurationSnapshot && (!configurationDraft || !configurationDirty)) {
      const nextDraft = configurationInputFromSnapshot(configurationSnapshot);
      if (!configurationInputsEqual(configurationDraft, nextDraft)) {
        setConfigurationDraft(nextDraft);
      }
    }
  }, [configurationSnapshot, configurationDirty, configurationDraft]);

  useEffect(() => {
    let unlisten: (() => void) | undefined;
    // 路由事件由 Rust 网关统一产生；模型级重试只刷新日志，只有真实切换才显示 Toast。
    void listen<RoutingEvent>("routing-event", (event) => {
      const routingEvent = event.payload;
      const isAutoSwitch = routingEvent.event_type === "auto_switch";
      const modelRecovered = routingEvent.event_type === "model_availability_recovered"
        || routingEvent.event_type === "capacity_recovered";
      const modelUnavailable = routingEvent.event_type === "model_temporarily_unavailable"
        || routingEvent.event_type === "capacity_exhausted";
      const eventMessage = modelRecovered
        ? "模型或分组恢复，同一中转重试成功"
        : modelUnavailable
          ? "模型或分组暂不可用，重试已耗尽"
          : "主力中转不可用，已切换到备用中转";
      setLogs((current) => current.some((item) => item.routing_event?.id === routingEvent.id)
        ? current
        : [{
            timestamp: routingEvent.timestamp,
            level: modelRecovered
              ? "info" as const
              : modelUnavailable
                ? "error" as const
                : "warning" as const,
            message: eventMessage,
            provider_id: routingEvent.target_provider_id,
            model: routingEvent.model,
            event_type: routingEvent.event_type,
            routing_event: routingEvent,
            diagnostic_detail: null,
          }, ...current].slice(0, 2000));
      if (isAutoSwitch && routingPolicy?.notify_switches !== false) {
        const source = routingEvent.attempts.find(
          (attempt) => attempt.provider_id === routingEvent.source_provider_id,
        )?.provider_name ?? "主力中转";
        const target = routingEvent.attempts.find(
          (attempt) => attempt.provider_id === routingEvent.target_provider_id,
        )?.provider_name ?? "备用中转";
        setNotice({
          level: "warning",
          title: `${source} 不可用（${routingEvent.attempts.find((a) => a.status_code)?.status_code ?? "错误"}）`,
          message: `已切换到 ${target}`,
          routingEvent,
        });
        window.setTimeout(() => setNotice(null), 8000);
      }
      void refresh();
    }).then((cleanup) => {
      unlisten = cleanup;
    }).catch(() => undefined);
    return () => unlisten?.();
  }, [refresh, routingPolicy?.notify_switches]);

  useEffect(() => {
    let unlisten: (() => void) | undefined;
    void listen("codex-config-changed", () => void refresh()).then((cleanup) => {
      unlisten = cleanup;
    }).catch(() => undefined);
    return () => unlisten?.();
  }, [refresh]);

  const drawerProvider =
    providerDrawer.mode === "detail" || providerDrawer.mode === "edit"
      ? providers.find((provider) => provider.id === providerDrawer.providerId) ?? null
      : null;
  const drawerHealth = drawerProvider
    ? health.find((item) => item.provider_id === drawerProvider.id)
    : undefined;
  const healthyCount = health.filter((item) => item.state === "healthy").length;
  const avgLatency = health.length
    ? Math.round(
        health.reduce((sum, item) => sum + (item.latency_ms ?? 0), 0) /
          health.length,
      )
    : 0;

  useEffect(() => {
    if (
      (providerDrawer.mode === "detail" || providerDrawer.mode === "edit") &&
      !drawerProvider
    ) {
      // 中转可能在其他操作中被删除；不能让抽屉继续展示已经失效的配置。
      setProviderDrawer({ mode: "closed" });
      setProviderFormDirty(false);
      setProviderFormSaving(false);
      setProviderClosePending(false);
      setProviderDrawerClosing(false);
    }
  }, [providerDrawer, drawerProvider]);

  function openProviderDrawer(next: ProviderDrawerState) {
    setProviderFormDirty(false);
    setProviderFormSaving(false);
    setProviderClosePending(false);
    setProviderDrawerClosing(false);
    setProviderDrawer(next);
  }

  function closeProviderDrawer() {
    if (providerDrawerClosing) return;
    setProviderClosePending(false);
    setProviderDrawerClosing(true);
    // 等待退场动画完成后再卸载，避免抽屉在用户操作后瞬间消失。
    window.setTimeout(() => {
      setProviderDrawer({ mode: "closed" });
      setProviderFormDirty(false);
      setProviderFormSaving(false);
      setProviderDrawerClosing(false);
    }, 180);
  }

  function requestProviderDrawerClose() {
    if (providerFormSaving || providerDrawerClosing) return;
    if (
      (providerDrawer.mode === "create" || providerDrawer.mode === "edit") &&
      providerFormDirty
    ) {
      setProviderClosePending(true);
      return;
    }
    closeProviderDrawer();
  }

  async function handleToggle(provider: Provider) {
    await setProviderEnabled(provider.id, !provider.enabled);
    setNotice(`${provider.name} 已${provider.enabled ? "停用" : "启用"}`);
    await refresh();
  }

  async function handleTest(provider: Provider) {
    const result = await testProvider(provider.id);
    setNotice(
      result.ok
        ? `${provider.name} 基础连接正常，延迟 ${result.latency_ms ?? "-"} ms`
        : `${provider.name} 测试失败：${result.message}`,
    );
    await refresh();
  }

  async function handleDelete(provider: Provider) {
    if (!window.confirm(`确定删除「${provider.name}」？`)) return;
    await deleteProvider(provider.id);
    setNotice("中转已删除");
    await refresh();
  }

  async function handleLock(provider: Provider) {
    const lockedId =
      routeGroup?.locked_provider_id === provider.id ? null : provider.id;
    await updateRouteGroup(
      providers.map((item) => item.id),
      lockedId,
    );
    setNotice(
      lockedId ? `${provider.name} 已设为首选中转` : "已恢复按优先级自动选择",
    );
    await refresh();
  }

  async function handleMove(providerId: string, offset: -1 | 1) {
    const ids = providers.map((provider) => provider.id);
    const index = ids.indexOf(providerId);
    const target = index + offset;
    if (index < 0 || target < 0 || target >= ids.length) return;
    [ids[index], ids[target]] = [ids[target], ids[index]];
    await reorderProviders(ids);
    setNotice("中转优先级已更新");
    await refresh();
  }

  async function chooseBackupImportFile() {
    if (providerDrawer.mode !== "closed") return;
    try {
      const selected = await open({
        multiple: false,
        directory: false,
        filters: [{ name: "AI Gateway 配置备份", extensions: ["aigateway"] }],
      });
      if (typeof selected === "string") {
        setBackupDialog({ mode: "import", path: selected });
      }
    } catch (error) {
      if (import.meta.env.DEV && location.search.includes("demo=1")) {
        setBackupDialog({
          mode: "import",
          path: "C:\\Backups\\AI-Gateway-backup-demo.aigateway",
        });
        return;
      }
      setNotice({
        level: "error",
        title: "无法选择备份文件",
        message: String(error),
      });
    }
  }

  async function handleExportBackup(
    password: string,
  ): Promise<BackupExportResult | null> {
    if (import.meta.env.DEV && location.search.includes("demo=1")) {
      const result = {
        file_name: backupDefaultFileName(),
        provider_count: providers.length,
        warning_messages: [],
      };
      setBackupDialog({ mode: "closed" });
      setNotice(`已导出 ${result.provider_count} 个中转到 ${result.file_name}`);
      return result;
    }
    const path = await save({
      defaultPath: backupDefaultFileName(),
      filters: [{ name: "AI Gateway 配置备份", extensions: ["aigateway"] }],
    });
    if (!path) return null;
    const result = await exportConfigurationBackup(path, password);
    setBackupDialog({ mode: "closed" });
    setNotice({
      level: result.warning_messages.length ? "warning" : "info",
      title: "加密配置已导出",
      message: `已导出 ${result.provider_count} 个中转到 ${result.file_name}${result.warning_messages.length ? `。${result.warning_messages.join("；")}` : ""}`,
    });
    return result;
  }

  async function handleInspectBackup(path: string, password: string) {
    if (import.meta.env.DEV && location.search.includes("demo=1")) {
      return {
        format_version: 1,
        exported_at: new Date().toISOString(),
        app_version: "0.2.0",
        provider_count: providers.length,
        conflict_count: Math.min(1, providers.length),
        includes_local_token: true,
      } satisfies BackupPreview;
    }
    return inspectConfigurationBackup(path, password);
  }

  async function handleImportBackup(
    path: string,
    password: string,
    mode: BackupImportMode,
  ): Promise<BackupImportResult> {
    const result = import.meta.env.DEV && location.search.includes("demo=1")
      ? {
          created_count: 2,
          updated_count: 1,
          removed_count: mode === "replace" ? 1 : 0,
          warning_messages: [],
        }
      : await importConfigurationBackup(path, password, mode);
    await refresh();
    setBackupDialog({ mode: "closed" });
    setNotice({
      level: result.warning_messages.length ? "warning" : "info",
      title: "配置备份已导入",
      message: `新增 ${result.created_count} 个，更新 ${result.updated_count} 个，删除 ${result.removed_count} 个${mode === "replace" ? "。本地访问令牌已恢复，请确认 cc-switch 使用相同的 API Key。" : "。目标电脑的全局设置和本地令牌保持不变。"}${result.warning_messages.length ? ` ${result.warning_messages.join("；")}` : ""}`,
    });
    return result;
  }

  function updateConfigurationDraft(next: ConfigurationInput) {
    setConfigurationDraft(next);
    setConfigurationSaveStatus("idle");
    setConfigurationSaveError(null);
  }

  function submitUsageQuery() {
    const error = validateUsageQuery(usageDraft);
    if (error) {
      setUsageError(error);
      return;
    }
    const next: UsageQuery = usageDraft.mode === "today"
      ? { mode: "today", source: usageDraft.source }
      : usageDraft.mode === "range"
        ? {
            mode: "range",
            start_date: usageDraft.start_date,
            end_date: usageDraft.end_date,
            source: usageDraft.source,
          }
        : { mode: "month", month: usageDraft.month, source: usageDraft.source };
    setUsageError(null);
    setUsageQuery(next);
  }

  async function handleSaveConfiguration() {
    if (!configurationDraft) return false;
    const validationError = validateConfigurationDraft(configurationDraft);
    if (validationError) {
      setConfigurationSaveStatus("error");
      setConfigurationSaveError(validationError);
      return false;
    }

    setConfigurationSaveStatus("saving");
    setConfigurationSaveError(null);
    try {
      const previousPort = settings?.port ?? 8765;
      const next = await saveConfiguration(configurationDraft);
      setConfigurationSnapshot(next);
      setConfigurationDraft(configurationInputFromSnapshot(next));
      setSettings(next.router_settings);
      setRoutingPolicy(next.routing_policy);
      setConfigurationSaveStatus("saved");
      setConfigurationSavedAt(new Date());
      setNotice(next.codex_restart_required
        ? {
            level: "warning",
            title: "Codex 流恢复配置已修改",
            message: previousPort !== next.router_settings.port
              ? `配置已修改，请完全退出并重新启动 Codex；本地网关已切换至 127.0.0.1:${next.router_settings.port}，同时更新 cc-switch 的 Base URL。`
              : "配置已修改，请完全退出并重新启动 Codex。",
          }
        : previousPort !== next.router_settings.port
          ? {
            level: "warning",
            title: "本地网关端口已切换",
            message: `已切换至 127.0.0.1:${next.router_settings.port}，请将 cc-switch 的 Base URL 更新为 http://127.0.0.1:${next.router_settings.port}/v1。API Key 保持不变。`,
          }
          : "配置中心已保存，日志与用量保留期限已立即生效");
      return true;
    } catch (error) {
      if (import.meta.env.DEV && location.search.includes("demo=1") && settings) {
        const next: ConfigurationSnapshot = {
          router_settings: {
            ...settings,
            port: configurationDraft.port,
            probe_interval_sec: configurationDraft.probe_interval_sec,
            log_retention_days: configurationDraft.log_retention_days,
            usage_retention_days: configurationDraft.usage_retention_days,
          },
          routing_policy: { ...configurationDraft.routing_policy },
          autostart_enabled: configurationDraft.autostart_enabled,
          codex_restart_required: false,
        };
        setConfigurationSnapshot(next);
        setConfigurationDraft(configurationInputFromSnapshot(next));
        setSettings(next.router_settings);
        setRoutingPolicy(next.routing_policy);
        setConfigurationSaveStatus("saved");
        setConfigurationSavedAt(new Date());
        return true;
      }
      setConfigurationSaveStatus("error");
      setConfigurationSaveError(String(error));
      return false;
    }
  }

  async function handleSwitchCodexMode(mode: "official" | "gateway") {
    setCodexBusy(true);
    setCodexError(null);
    try {
      const result = await switchCodexConnectionMode(mode);
      await refresh();
      setNotice({
        level: "info",
        title: result.changed ? "Codex 接入模式已修改" : "Codex 接入配置无需修改",
        message: result.restart_required
          ? "配置已修改，请完全退出并重新启动 Codex"
          : "当前配置已经是目标模式。",
      });
    } catch (error) {
      setCodexError(String(error));
    } finally {
      setCodexBusy(false);
    }
  }

  async function handleRefreshCodexLogin() {
    setCodexBusy(true);
    setCodexError(null);
    try {
      setCodexState(await refreshCodexLoginStatus());
    } catch (error) {
      setCodexError(String(error));
    } finally {
      setCodexBusy(false);
    }
  }

  async function handleRepairCodexGateway() {
    setCodexBusy(true);
    setCodexError(null);
    try {
      const result = await repairCodexGatewayConfiguration();
      await refresh();
      setNotice({
        level: result.warning_messages.length ? "warning" : "info",
        title: "Codex 接入配置已修复",
        message: `${result.restart_required
          ? "请完全退出并重新启动 Codex。"
          : "AI Gateway Provider 配置已经是最新状态。"}${result.warning_messages.length ? ` ${result.warning_messages.join("；")}` : ""}`,
      });
    } catch (error) {
      setCodexError(String(error));
    } finally {
      setCodexBusy(false);
    }
  }

  function requestView(nextView: AppView) {
    if (nextView === view) return;
    if (view === "settings" && configurationDirty) {
      setPendingView(nextView);
      return;
    }
    setView(nextView);
  }

  async function saveAndLeaveConfiguration() {
    const nextView = pendingView;
    if (!nextView || !(await handleSaveConfiguration())) return;
    setPendingView(null);
    setView(nextView);
  }

  function discardAndLeaveConfiguration() {
    const nextView = pendingView;
    if (!nextView) return;
    if (configurationSnapshot) {
      setConfigurationDraft(configurationInputFromSnapshot(configurationSnapshot));
    }
    setConfigurationSaveStatus("idle");
    setConfigurationSaveError(null);
    setPendingView(null);
    setView(nextView);
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <span className="brand-mark">
            <Router size={18} />
          </span>
          <span>AI Gateway</span>
        </div>
        <nav className="nav-list">
          <button
            className={`nav-item ${view === "monitor" ? "active" : ""}`}
            onClick={() => requestView("monitor")}
          >
            <Activity size={17} />
            运行监控
          </button>
          <button
            className={`nav-item ${view === "usage" ? "active" : ""}`}
            onClick={() => requestView("usage")}
          >
            <BarChart3 size={17} />
            用量统计
          </button>
          <button
            className={`nav-item ${view === "providers" ? "active" : ""}`}
            onClick={() => requestView("providers")}
          >
            <Server size={17} />
            中转管理
          </button>
          <button
            className={`nav-item ${view === "codex" ? "active" : ""}`}
            onClick={() => requestView("codex")}
          >
            <Bot size={17} />
            Codex 接入
          </button>
          <button
            className={`nav-item ${view === "logs" ? "active" : ""}`}
            onClick={() => requestView("logs")}
          >
            <ScrollText size={17} />
            运行日志
          </button>
          <button
            className={`nav-item ${view === "settings" ? "active" : ""}`}
            onClick={() => requestView("settings")}
          >
            <Settings size={17} />
            配置中心
          </button>
        </nav>
        <div className="sidebar-foot">
          <div className="secure-note">
            <ShieldCheck size={16} />
            <span>仅本机访问</span>
          </div>
          <span className="version">v0.3.10</span>
        </div>
      </aside>
      <main className="main-content">
        <header className="topbar">
          <div>
            <p className="eyebrow">LOCAL ROUTER</p>
            <h1>
              {view === "monitor"
                ? "运行监控"
                : view === "usage"
                  ? "用量统计"
                : view === "providers"
                  ? "中转管理"
                : view === "codex"
                  ? "Codex 接入"
                : view === "logs"
                  ? "运行日志"
                  : "配置中心"}
            </h1>
          </div>
          <div className="top-actions">
            <span
              className={`gateway-dot ${settings?.gateway_running && settings?.gateway_listening ? "on" : ""}`}
            />
            {settings?.gateway_running && settings?.gateway_listening
              ? "网关运行中"
              : settings?.gateway_error
                ? "端口监听失败"
                : "网关已停止"}
            <button
              className="icon-button"
              title="刷新状态"
              onClick={() => {
                void refresh();
                if (view === "usage") void refreshUsage(usageQuery);
              }}
            >
              <RefreshCw size={16} />
            </button>
          </div>
        </header>
        {notice && <NoticeBanner notice={notice} onClose={() => setNotice(null)} onDetails={(event) => setRoutingDetail(event)} />}
        {view === "monitor" ? (
          <MonitorPanel
            providers={providers}
            health={health}
            settings={settings}
            routeGroup={routeGroup}
            runtime={runtime}
            logs={logs}
            routingPolicy={routingPolicy}
            lastUpdatedAt={lastUpdatedAt}
            onOpenRoutingEvent={setRoutingDetail}
            onTest={(provider) => void handleTest(provider)}
            onLock={(provider) => void handleLock(provider)}
          />
        ) : view === "usage" ? (
          <UsagePanel
            draft={usageDraft}
            statistics={usageStatistics}
            loading={usageLoading}
            error={usageError}
            onDraftChange={setUsageDraft}
            onSubmit={submitUsageQuery}
            onRefresh={() => void refreshUsage(usageQuery)}
          />
        ) : view === "providers" ? (
          <>
            <section className="stats-row">
              <Stat
                icon={<Activity size={17} />}
                label="可用中转"
                value={`${healthyCount}/${providers.length}`}
                tone="green"
              />
              <Stat
                icon={<Gauge size={17} />}
                label="平均延迟"
                value={
                  runtime?.average_latency_ms
                    ? `${runtime.average_latency_ms} ms`
                    : avgLatency
                      ? `${avgLatency} ms`
                      : "--"
                }
                tone="blue"
              />
              <Stat
                icon={<ShieldCheck size={17} />}
                label="路由策略"
                value={
                  routeGroup?.locked_provider_id
                    ? "首选中转 + 故障转移"
                    : "优先级故障转移"
                }
                tone="slate"
              />
            </section>
            <section className="panel provider-panel">
                <div className="panel-heading">
                  <div>
                    <h2>中转列表</h2>
                    <p>按优先级尝试，异常时自动进入下一个可用中转；第三方中转间切换无需重启 Codex</p>
                  </div>
                  <div className="panel-heading-actions">
                    <button
                      className="secondary-button"
                      disabled={providerDrawer.mode !== "closed"}
                      onClick={() => void chooseBackupImportFile()}
                    >
                      <Upload size={15} />
                      导入配置
                    </button>
                    <button
                      className="secondary-button"
                      disabled={providerDrawer.mode !== "closed"}
                      onClick={() => setBackupDialog({ mode: "export" })}
                    >
                      <Download size={15} />
                      导出配置
                    </button>
                    <button
                      className="primary-button"
                      onClick={() => openProviderDrawer({ mode: "create" })}
                    >
                      <Plus size={16} />
                      添加中转
                    </button>
                  </div>
                </div>
                {providers.length === 0 ? (
                  <EmptyState onAdd={() => openProviderDrawer({ mode: "create" })} />
                ) : (
                  <div className="provider-list">
                    {providers.map((provider, index) => (
                      <ProviderRow
                        key={provider.id}
                        provider={provider}
                        health={health.find(
                          (item) => item.provider_id === provider.id,
                        )}
                        index={index}
                        last={index === providers.length - 1}
                        selected={
                          providerDrawer.mode !== "closed" &&
                          providerDrawer.mode !== "create" &&
                          providerDrawer.providerId === provider.id
                        }
                        onSelect={() =>
                          openProviderDrawer({
                            mode: "detail",
                            providerId: provider.id,
                          })
                        }
                        onEdit={() =>
                          openProviderDrawer({
                            mode: "edit",
                            providerId: provider.id,
                          })
                        }
                        onToggle={() => void handleToggle(provider)}
                        onMoveUp={() => void handleMove(provider.id, -1)}
                        onMoveDown={() => void handleMove(provider.id, 1)}
                        onTest={() => void handleTest(provider)}
                        onDelete={() => void handleDelete(provider)}
                      />
                    ))}
                  </div>
                )}
            </section>
          </>
        ) : view === "codex" ? (
          <CodexIntegrationPanel
            state={codexState}
            historyCompatibility={codexHistoryCompatibility}
            onHistoryCompatibilityChanged={setCodexHistoryCompatibility}
            busy={codexBusy}
            error={codexError}
            onSwitch={handleSwitchCodexMode}
            onRefreshLogin={() => void handleRefreshCodexLogin()}
            onRepair={() => void handleRepairCodexGateway()}
          />
        ) : view === "logs" ? (
          <LogsPanel logs={logs} providers={providers} runtime={runtime} onOpenRoutingEvent={setRoutingDetail} onOpenDiagnostic={setDiagnosticDetail} />
        ) : (
          <ConfigurationCenter
            settings={settings}
            draft={configurationDraft}
            dirty={configurationDirty}
            saveStatus={configurationSaveStatus}
            saveError={configurationSaveError}
            savedAt={configurationSavedAt}
            onChange={updateConfigurationDraft}
            onSave={() => void handleSaveConfiguration()}
          />
        )}
      </main>
      {providerDrawer.mode !== "closed" && (
        <ProviderDrawer
          mode={providerDrawer.mode}
          provider={drawerProvider}
          health={drawerHealth}
          locked={
            Boolean(drawerProvider) &&
            routeGroup?.locked_provider_id === drawerProvider?.id
          }
          saving={providerFormSaving}
          closing={providerDrawerClosing}
          gatewayUrl={`http://127.0.0.1:${settings?.port ?? 8765}/v1`}
          onClose={requestProviderDrawerClose}
          onDirtyChange={setProviderFormDirty}
          onSavingChange={setProviderFormSaving}
          onEdit={() => {
            if (drawerProvider) {
              openProviderDrawer({
                mode: "edit",
                providerId: drawerProvider.id,
              });
            }
          }}
          onTest={() => drawerProvider && void handleTest(drawerProvider)}
          onLock={() => drawerProvider && void handleLock(drawerProvider)}
          onSaved={async () => {
            setNotice("中转配置已保存");
            await refresh();
            closeProviderDrawer();
          }}
        />
      )}
      {providerClosePending && (
        <ProviderUnsavedModal
          onContinue={() => setProviderClosePending(false)}
          onDiscard={closeProviderDrawer}
        />
      )}
      {backupDialog.mode === "export" && (
        <ExportBackupModal
          onClose={() => setBackupDialog({ mode: "closed" })}
          onExport={handleExportBackup}
        />
      )}
      {backupDialog.mode === "import" && (
        <ImportBackupModal
          path={backupDialog.path}
          onClose={() => setBackupDialog({ mode: "closed" })}
          onInspect={handleInspectBackup}
          onImport={handleImportBackup}
        />
      )}
      {routingDetail && (
        <RoutingDetailModal event={routingDetail} onClose={() => setRoutingDetail(null)} />
      )}
      {diagnosticDetail && (
        <DiagnosticDetailModal detail={diagnosticDetail} onClose={() => setDiagnosticDetail(null)} />
      )}
      {pendingView && (
        <UnsavedChangesModal
          saving={configurationSaveStatus === "saving"}
          error={configurationSaveError}
          onSaveAndLeave={() => void saveAndLeaveConfiguration()}
          onDiscard={discardAndLeaveConfiguration}
          onCancel={() => setPendingView(null)}
        />
      )}
    </div>
  );
}

function NoticeBanner({
  notice,
  onClose,
  onDetails,
}: {
  notice: AppNotice | string;
  onClose: () => void;
  onDetails: (event: RoutingEvent) => void;
}) {
  const structured = typeof notice === "object" ? notice : null;
  return (
    <div className={`notice ${structured?.level ?? "info"}`}>
      {structured?.level === "warning" ? <ShieldCheck size={16} /> : <Check size={16} />}
      <div className="notice-copy">
        <strong>{structured?.title ?? "操作完成"}</strong>
        <span>{structured?.message ?? (typeof notice === "string" ? notice : "")}</span>
      </div>
      {structured?.routingEvent && (
        <button className="notice-detail" onClick={() => onDetails(structured.routingEvent!)}>
          查看详情
        </button>
      )}
      <button className="notice-close" title="关闭提示" onClick={onClose}>
        <X size={15} />
      </button>
    </div>
  );
}

function Stat({
  icon,
  label,
  value,
  tone,
}: {
  icon: ReactNode;
  label: string;
  value: string;
  tone: string;
}) {
  return (
    <div className="stat">
      <span className={`stat-icon ${tone}`}>{icon}</span>
      <div>
        <span>{label}</span>
        <strong>{value}</strong>
      </div>
    </div>
  );
}

const compactTokenFormatter = new Intl.NumberFormat("zh-CN", {
  notation: "compact",
  maximumFractionDigits: 1,
});
const fullTokenFormatter = new Intl.NumberFormat("zh-CN");

function formatTokenCount(value: number) {
  return value >= 10_000
    ? compactTokenFormatter.format(value)
    : fullTokenFormatter.format(value);
}

function tokenMetricText(metric: TokenMetric, completedRequests: number) {
  if (!completedRequests) return "—";
  if (!metric.reported_count) return "未上报";
  return formatTokenCount(metric.value);
}

function usageQualityLabel(quality: "exact" | "mixed" | "estimated" | "unknown") {
  return {
    exact: "精确",
    mixed: "含估算",
    estimated: "估算",
    unknown: "未统计",
  }[quality];
}

function CodexIntegrationPanel({
  state,
  historyCompatibility,
  onHistoryCompatibilityChanged,
  busy,
  error,
  onSwitch,
  onRefreshLogin,
  onRepair,
}: {
  state: CodexIntegrationState | null;
  historyCompatibility: CodexHistoryCompatibilityState | null;
  onHistoryCompatibilityChanged: (value: CodexHistoryCompatibilityState) => void;
  busy: boolean;
  error: string | null;
  onSwitch: (mode: "official" | "gateway") => Promise<void>;
  onRefreshLogin: () => void;
  onRepair: () => void;
}) {
  const [historyPreview, setHistoryPreview] =
    useState<CodexHistoryMigrationPreview | null>(null);
  const [historyBusy, setHistoryBusy] = useState(false);
  const [historyError, setHistoryError] = useState<string | null>(null);
  const loginLabel = state?.login_status === "logged_in"
    ? "已登录"
    : state?.login_status === "logged_out"
      ? "未登录"
      : "无法判断";
  const applicationLabel = (status: CodexIntegrationState["desktop_status"] | undefined) => {
    if (status === "installed_running") return "已安装，运行中";
    if (status === "installed_stopped") return "已安装，未运行";
    if (status === "not_installed") return "未安装";
    return "检测中";
  };
  const currentMode = state?.current_mode;

  return (
    <div className="codex-page">
      {error && <div className="codex-error-banner"><strong>操作失败</strong><span>{error}</span></div>}

      <section className="panel codex-status-panel">
        <div className="codex-status-list">
          <div><span>Codex 桌面端</span><strong>{applicationLabel(state?.desktop_status)}</strong></div>
          <div><span>Codex CLI</span><strong>{applicationLabel(state?.cli_status)}</strong></div>
            <div><span>ChatGPT 登录</span><strong>{loginLabel}</strong></div>
            <div><span>当前 Provider</span><strong>{state?.current_provider_id ?? "未识别"}</strong></div>
        </div>
        <button className="secondary-button" disabled={busy} onClick={onRefreshLogin}>
          <RefreshCw size={15} />重新检测
        </button>
      </section>

      <section className="codex-mode-grid">
        <article className={`codex-mode-card ${currentMode === "official" ? "active" : ""}`}>
          <div className="codex-mode-heading">
            <span className="codex-mode-icon official"><ShieldCheck size={19} /></span>
            <div>
              <h2>OpenAI 官方</h2>
              <p>使用 Codex 原生 ChatGPT 登录，完全绕过 AI Gateway</p>
            </div>
            <span className="codex-mode-status">
              {currentMode === "official" ? "当前模式" : "未启用"}
            </span>
          </div>
          <div className="codex-mode-actions">
            <button
              className={`primary-button${busy ? " is-loading" : ""}`}
              aria-busy={busy}
              disabled={busy || currentMode === "official"}
              onClick={() => void onSwitch("official")}
            >
              切换到官方
            </button>
          </div>
        </article>

        <article className={`codex-mode-card ${currentMode === "gateway" ? "active" : ""}`}>
          <div className="codex-mode-heading">
            <span className="codex-mode-icon gateway"><Router size={19} /></span>
            <div>
              <h2>第三方中转</h2>
              <p>Codex 固定连接本地网关，中转选择由 AI Gateway 统一管理</p>
            </div>
            <span className="codex-mode-status">
              {currentMode === "gateway" ? "当前模式" : "未启用"}
            </span>
          </div>
          {!state?.gateway_configured && currentMode === "gateway" && (
            <p className="codex-repair-hint">当前接入配置需要修复，修复后请重新启动 Codex。</p>
          )}
          <div className="codex-mode-actions">
            <button
              className="secondary-button"
              disabled={busy || currentMode !== "gateway"}
              onClick={onRepair}
            >
              <RefreshCw size={15} />修复接入配置
            </button>
            <button
              className={`primary-button${busy ? " is-loading" : ""}`}
              aria-busy={busy}
              disabled={busy || (currentMode === "gateway" && state?.gateway_configured)}
              onClick={() => void onSwitch("gateway")}
            >
              切换到第三方
            </button>
          </div>
        </article>
      </section>

      {historyCompatibility?.native_supported && (
        <section className="panel codex-history-panel">
          <div>
            <p className="section-kicker">SESSION HISTORY</p>
            <h2>Codex 原生管理跨 Provider 会话</h2>
            <p>当前 Codex 支持按 Provider 之外的统一线程列表查看历史，AI Gateway 不会改写会话文件。</p>
          </div>
          {historyCompatibility.provider_counts.length > 0 && (
            <span className="codex-history-counts">
              {historyCompatibility.provider_counts.map((item) => `${item.provider} ${item.count}`).join(" · ")}
            </span>
          )}
        </section>
      )}

      {historyCompatibility && !historyCompatibility.native_supported && historyCompatibility.migration_required && (
        <section className="panel codex-history-panel legacy">
          <div>
            <p className="section-kicker">LEGACY COMPATIBILITY</p>
            <h2>旧版会话兼容迁移</h2>
            <p>当前 Codex 未确认支持跨 Provider 会话列表。此操作只修改会话首行 Provider 元数据，不修改聊天正文，并会先创建备份。</p>
            <span className="codex-history-counts">
              普通 {historyCompatibility.normal_session_count} · 归档 {historyCompatibility.archived_session_count} · 含加密内容 {historyCompatibility.encrypted_content_session_count}
            </span>
            {historyCompatibility.probe_error && <small>{historyCompatibility.probe_error}</small>}
            {historyError && <small className="form-error">{historyError}</small>}
          </div>
          <div className="codex-mode-actions">
            <button
              className="secondary-button"
              disabled={historyBusy}
              onClick={async () => {
                setHistoryBusy(true);
                setHistoryError(null);
                try {
                  setHistoryPreview(await previewCodexHistoryMigration());
                } catch (error) {
                  setHistoryError(String(error));
                } finally {
                  setHistoryBusy(false);
                }
              }}
            >
              {historyBusy ? "扫描中…" : "扫描并预览"}
            </button>
            {historyPreview && (
              <button
                className="primary-button"
                disabled={historyBusy || historyPreview.migration_required_count === 0}
                onClick={async () => {
                  if (!window.confirm(`将迁移 ${historyPreview.migration_required_count} 个会话的 Provider 元数据，是否继续？`)) return;
                  setHistoryBusy(true);
                  setHistoryError(null);
                  try {
                    await applyCodexHistoryMigration(historyPreview.preview_id);
                    setHistoryPreview(null);
                    onHistoryCompatibilityChanged(await getCodexHistoryCompatibility());
                  } catch (error) {
                    setHistoryError(String(error));
                  } finally {
                    setHistoryBusy(false);
                  }
                }}
              >
                执行旧版迁移
              </button>
            )}
            {historyCompatibility.can_restore_latest && (
              <button
                className="secondary-button"
                disabled={historyBusy}
                onClick={async () => {
                  setHistoryBusy(true);
                  setHistoryError(null);
                  try {
                    await restoreCodexHistoryMigration();
                    onHistoryCompatibilityChanged(await getCodexHistoryCompatibility());
                  } catch (error) {
                    setHistoryError(String(error));
                  } finally {
                    setHistoryBusy(false);
                  }
                }}
              >
                恢复最近一次备份
              </button>
            )}
          </div>
        </section>
      )}

    </div>
  );
}

function UsagePanel({
  draft,
  statistics,
  loading,
  error,
  onDraftChange,
  onSubmit,
  onRefresh,
}: {
  draft: UsageQuery;
  statistics: UsageStatistics | null;
  loading: boolean;
  error: string | null;
  onDraftChange: (query: UsageQuery) => void;
  onSubmit: () => void;
  onRefresh: () => void;
}) {
  const [syncBusy, setSyncBusy] = useState(false);
  const [rebuildConfirming, setRebuildConfirming] = useState(false);
  const [syncError, setSyncError] = useState<string | null>(null);
  const rangeEndMax = draft.start_date
    ? [TODAY_VALUE, shiftLocalDate(draft.start_date, 29)].sort()[0]
    : TODAY_VALUE;
  const peakTokens = Math.max(
    1,
    ...(statistics?.series.map((point) => point.metrics.total_tokens.value) ?? []),
  );
  const labelStep = Math.max(1, Math.ceil((statistics?.series.length ?? 1) / 8));
  const summaryItems = statistics
    ? [
        ["总 Token", statistics.totals.total_tokens, "精确值与本地估算合计"],
        ["输入 Token", statistics.totals.input_tokens, "Prompt 与输入上下文"],
        ["输出 Token", statistics.totals.output_tokens, "文本、工具与推理输出"],
        ["缓存命中 Token", statistics.totals.cache_hit_tokens, "仅统计明确上报的缓存命中"],
      ] as const
    : [];
  const syncStatus = statistics?.codex_sync_status;
  const syncRebuilding = Boolean(
    syncStatus?.state === "syncing" || syncStatus?.rebuild_status === "pending",
  );
  const syncNeedsAttention = Boolean(
    syncError
    || syncStatus?.warning_messages.length,
  );

  return (
    <div className="usage-page">
      <section className="panel usage-filter-panel">
        <div className="usage-filter-heading">
          <div>
            <p className="section-kicker">TOKEN USAGE</p>
            <h2>用量查询</h2>
            <span>官方使用 Codex 本地记录；第三方优先使用 usage，缺失时进行本地估算。</span>
          </div>
          {statistics && (
            <div className="usage-period-label">
              <span>当前统计周期</span>
              <strong>{statistics.period.label}</strong>
            </div>
          )}
        </div>
        <div className="usage-filter-controls">
          <div className="usage-mode-switch" role="group" aria-label="用量查询方式">
            {([
              ["today", "今天"],
              ["range", "日期范围"],
              ["month", "月份"],
            ] as const).map(([mode, label]) => (
              <button
                type="button"
                aria-pressed={draft.mode === mode}
                className={draft.mode === mode ? "active" : ""}
                key={mode}
                onClick={() => onDraftChange({ ...draft, mode })}
              >
                {label}
              </button>
            ))}
          </div>
          <div className="usage-date-fields">
            {draft.mode === "today" ? (
              <div className="usage-today-value">
                <span>本机日期</span>
                <strong>{TODAY_VALUE}</strong>
              </div>
            ) : draft.mode === "range" ? (
              <>
                <label>
                  <span>开始日期</span>
                  <input
                    type="date"
                    max={TODAY_VALUE}
                    value={draft.start_date ?? ""}
                    onChange={(event) => {
                      const start = event.target.value;
                      if (!start) {
                        onDraftChange({ ...draft, start_date: "", end_date: "" });
                        return;
                      }
                      const endMax = [TODAY_VALUE, shiftLocalDate(start, 29)].sort()[0];
                      onDraftChange({
                        ...draft,
                        start_date: start,
                        end_date: draft.end_date && draft.end_date <= endMax
                          ? draft.end_date
                          : endMax,
                      });
                    }}
                  />
                </label>
                <span className="usage-date-separator">至</span>
                <label>
                  <span>结束日期</span>
                  <input
                    type="date"
                    min={draft.start_date}
                    max={rangeEndMax}
                    value={draft.end_date ?? ""}
                    onChange={(event) => onDraftChange({
                      ...draft,
                      end_date: event.target.value,
                    })}
                  />
                </label>
              </>
            ) : (
              <label>
                <span>查询月份</span>
                <input
                  type="month"
                  max={CURRENT_MONTH_VALUE}
                  value={draft.month ?? ""}
                  onChange={(event) => onDraftChange({
                    ...draft,
                    month: event.target.value,
                  })}
                />
              </label>
            )}
          </div>
          <button
            type="button"
            className="primary-button usage-query-button"
            disabled={loading}
            onClick={onSubmit}
          >
            <BarChart3 size={16} />
            {loading ? "查询中…" : "查询"}
          </button>
          <button
            type="button"
            className="icon-button usage-refresh-button"
            title="同步 Codex 用量并刷新"
            aria-label="同步 Codex 用量并刷新"
            disabled={loading || syncBusy}
            onClick={async () => {
              setSyncBusy(true);
              setSyncError(null);
              try {
                await syncCodexUsage();
                onRefresh();
              } catch (syncFailure) {
                setSyncError(String(syncFailure));
              } finally {
                setSyncBusy(false);
              }
            }}
          >
            <RefreshCw size={16} className={syncBusy ? "spin" : ""} />
          </button>
        </div>
        <div className="usage-source-row">
          <span>数据来源</span>
          <div className="usage-source-switch" role="group" aria-label="用量数据来源">
            {([[
              "all", "全部"], ["official", "OpenAI 官方"], ["gateway", "第三方中转"], ["unconfirmed", "来源未确认"],
            ] as const).map(([source, label]) => (
              <button
                type="button"
                key={source}
                aria-pressed={draft.source === source}
                className={draft.source === source ? "active" : ""}
                onClick={() => onDraftChange({ ...draft, source })}
              >
                {label}
              </button>
            ))}
          </div>
        </div>
        {draft.mode === "range" && (
          <div className="usage-range-hint">自定义日期范围最多 30 个自然日；月份查询可覆盖完整 31 天月份。</div>
        )}
        {error && <div className="usage-query-error">{error}</div>}
      </section>

      {statistics ? (
        <>
          {syncRebuilding && (
            <section className="usage-sync-warning neutral">
              <span>正在重新扫描 Codex 本地用量，旧解析器产生的记录暂不计入当前结果。</span>
            </section>
          )}
          {syncNeedsAttention && (
            <section className="usage-sync-warning">
              <span>{syncError ?? statistics.codex_sync_status.warning_messages[0] ?? "Codex 本地用量没有成功导入"}</span>
              <button type="button" onClick={() => setRebuildConfirming(true)}>重建 Codex 用量</button>
            </section>
          )}

          <div className="usage-sync-summary">
            <span>
              Codex 本地同步：扫描 {statistics.codex_sync_status.files_scanned} 个文件，
              本轮发现 {statistics.codex_sync_status.token_events_found} 条用量，
              官方 {statistics.codex_sync_status.official_events} 条，
              匹配网关 {statistics.codex_sync_status.gateway_matched_events} 条
              {statistics.codex_sync_status.gateway_completed_events > 0
                ? `（补全 ${statistics.codex_sync_status.gateway_completed_events} 条）`
                : ""}
              {statistics.codex_sync_status.gateway_unmatched_ignored > 0
                ? `，未唯一匹配 ${statistics.codex_sync_status.gateway_unmatched_ignored} 条未计量`
                : ""}
              {statistics.codex_sync_status.duplicate_events > 0
                ? `，去重 ${statistics.codex_sync_status.duplicate_events} 条`
                : ""}
              {statistics.codex_sync_status.removed_stale_events > 0
                ? `，清理失效 ${statistics.codex_sync_status.removed_stale_events} 条`
                : ""}
            </span>
            {statistics.codex_sync_status.rebuild_performed && <strong>已完成自动重建</strong>}
          </div>

          <section className="usage-summary-grid">
            {summaryItems.map(([label, metric, detail]) => (
              <div className="usage-summary-card" key={label}>
                <span>{label}</span>
                <strong title={fullTokenFormatter.format(metric.value)}>
                  {tokenMetricText(metric, statistics.completed_requests)}
                </strong>
                <small>
                  {metric.estimated_value > 0
                    ? `其中估算 ${fullTokenFormatter.format(metric.estimated_value)}`
                    : detail}
                </small>
              </div>
            ))}
          </section>

          <section className="panel usage-provider-panel">
            <div className="usage-section-heading compact">
              <div>
                <p className="section-kicker">PROVIDERS</p>
                <h2>线路用量明细</h2>
              </div>
              <span className="privacy-note">官方数据来自 Codex 本地 token_count，不等同于 OpenAI 账单</span>
            </div>
            <div className="usage-provider-table">
              <div className="usage-provider-header">
                <span>线路 / 来源</span><span>总 Token</span><span>输入 Token</span><span>输出 Token</span><span>缓存命中 Token</span>
              </div>
              {statistics.providers.map((provider) => (
                <div className="usage-provider-row" key={provider.provider_id}>
                  <div>
                    <strong>{provider.provider_name}</strong>
                    <span className={`usage-quality ${provider.data_quality}`}>{usageQualityLabel(provider.data_quality)}</span>
                    {provider.deleted && <small>已删除中转</small>}
                  </div>
                  <strong>{tokenMetricText(provider.metrics.total_tokens, provider.completed_requests)}</strong>
                  <span>{tokenMetricText(provider.metrics.input_tokens, provider.completed_requests)}</span>
                  <span>{tokenMetricText(provider.metrics.output_tokens, provider.completed_requests)}</span>
                  <span>{tokenMetricText(provider.metrics.cache_hit_tokens, provider.completed_requests)}</span>
                </div>
              ))}
            </div>
          </section>

          <section className="panel usage-chart-panel">
            <div className="usage-section-heading">
              <div>
                <p className="section-kicker">TREND</p>
                <h2>用量趋势</h2>
                <span>{statistics.period.resolution === "hour" ? "按小时" : "按天"}汇总输入与输出 Token</span>
              </div>
              <span className={`usage-quality ${statistics.data_quality}`}>{usageQualityLabel(statistics.data_quality)}</span>
            </div>
            {statistics.series.some((point) => point.metrics.total_tokens.value > 0) ? (
              <div className="usage-chart" style={{ gridTemplateColumns: `repeat(${statistics.series.length}, minmax(8px, 1fr))` }}>
                {statistics.series.map((point, index) => {
                  const inputHeight = (point.metrics.input_tokens.value / peakTokens) * 100;
                  const outputHeight = (point.metrics.output_tokens.value / peakTokens) * 100;
                  const tooltip = `${point.label}\n总计 ${fullTokenFormatter.format(point.metrics.total_tokens.value)}\n输入 ${fullTokenFormatter.format(point.metrics.input_tokens.value)}\n输出 ${fullTokenFormatter.format(point.metrics.output_tokens.value)}\n缓存命中 ${fullTokenFormatter.format(point.metrics.cache_hit_tokens.value)}`;
                  return (
                    <div className="usage-chart-column" key={point.key} title={tooltip}>
                      <div className="usage-bar-track">
                        <span className="usage-bar-output" style={{ height: `${outputHeight}%` }} />
                        <span className="usage-bar-input" style={{ height: `${inputHeight}%` }} />
                      </div>
                      <small>{index % labelStep === 0 || index === statistics.series.length - 1 ? point.label : ""}</small>
                    </div>
                  );
                })}
              </div>
            ) : (
              <div className="usage-empty-chart">当前周期没有 Token 用量</div>
            )}
            <div className="usage-chart-legend">
              <span><i className="input" />输入 Token</span>
              <span><i className="output" />输出 Token</span>
            </div>
          </section>

          {rebuildConfirming && (
            <div className="modal-backdrop" role="presentation">
              <section className="confirm-modal" role="dialog" aria-modal="true" aria-labelledby="rebuild-usage-title">
                <h3 id="rebuild-usage-title">重建 Codex 用量？</h3>
                <p>将删除已导入的 Codex 会话记录并重新扫描本地 JSONL，不会删除网关响应记录或原始会话。</p>
                <div className="confirm-actions">
                  <button type="button" className="secondary-button" onClick={() => setRebuildConfirming(false)}>取消</button>
                  <button
                    type="button"
                    className="danger-button"
                    disabled={syncBusy}
                    onClick={async () => {
                      setSyncBusy(true);
                      setSyncError(null);
                      try {
                        await rebuildCodexUsage();
                        setRebuildConfirming(false);
                        onRefresh();
                      } catch (error) {
                        setSyncError(String(error));
                        setRebuildConfirming(false);
                      } finally {
                        setSyncBusy(false);
                      }
                    }}
                  >确认重建</button>
                </div>
              </section>
            </div>
          )}
        </>
      ) : (
        <section className="panel usage-loading-state">
          {loading ? "正在读取用量数据…" : "选择统计周期后查询用量"}
        </section>
      )}
    </div>
  );
}

function MonitorPanel({
  providers,
  health,
  settings,
  routeGroup,
  runtime,
  logs,
  routingPolicy,
  lastUpdatedAt,
  onOpenRoutingEvent,
  onTest,
  onLock,
}: {
  providers: Provider[];
  health: HealthSnapshot[];
  settings: RouterSettings | null;
  routeGroup: RouteGroup | null;
  runtime: RuntimeStats | null;
  logs: EventLog[];
  routingPolicy: RoutingPolicy | null;
  lastUpdatedAt: Date | null;
  onOpenRoutingEvent: (event: RoutingEvent) => void;
  onTest: (provider: Provider) => void;
  onLock: (provider: Provider) => void;
}) {
  const failoverEnabled = routingPolicy?.allow_provider_failover === true;
  const enabledProviders = providers.filter((provider) => provider.enabled);
  const snapshots = new Map(
    health.map((snapshot) => [snapshot.provider_id, snapshot]),
  );
  const healthyProviders = enabledProviders.filter(
    (provider) => snapshots.get(provider.id)?.state === "healthy",
  );
  const usableProviders = enabledProviders.filter(
    (provider) =>
      !failoverEnabled || snapshots.get(provider.id)?.state !== "open",
  );
  const lockedProvider = providers.find(
    (provider) => provider.id === routeGroup?.locked_provider_id,
  );
  // 监控页与后端候选规则保持一致：关闭自动切换时，内部熔断不能改变用户当前选择。
  const activeProvider =
    (lockedProvider?.enabled &&
    (!failoverEnabled || snapshots.get(lockedProvider.id)?.state !== "open")
      ? lockedProvider
      : undefined) ?? usableProviders[0];
  const successRate = runtime?.total_requests
    ? Math.round((runtime.successful_requests / runtime.total_requests) * 100)
    : null;
  const latestSwitch = logs.find(
    (entry) => entry.routing_event?.event_type === "auto_switch",
  )?.routing_event;

  let overallTitle = "正在检测中转";
  let overallDescription = "健康探测完成后会自动更新路由状态";
  let overallTone = "checking";
  if (settings?.gateway_error) {
    overallTitle = "本地端口监听失败";
    overallDescription = settings.gateway_error;
    overallTone = "down";
  } else if (!settings?.gateway_running || !settings.gateway_listening) {
    overallTitle = "本地网关已停止";
    overallDescription = "Codex 当前无法通过本地地址访问任何中转";
    overallTone = "down";
  } else if (!enabledProviders.length) {
    overallTitle = "还没有启用的中转";
    overallDescription = "请先在中转管理中添加并启用至少一个中转";
    overallTone = "checking";
  } else if (healthyProviders.length === enabledProviders.length) {
    overallTitle = "全部中转运行正常";
    overallDescription = failoverEnabled
      ? `${healthyProviders.length}/${enabledProviders.length} 个中转健康，自动故障转移已就绪`
      : `${healthyProviders.length}/${enabledProviders.length} 个中转健康，自动切换当前关闭`;
    overallTone = "healthy";
  } else if (healthyProviders.length || usableProviders.length) {
    overallTitle = "路由可用，部分中转状态异常";
    overallDescription = failoverEnabled
      ? `${usableProviders.length}/${enabledProviders.length} 个中转仍可参与路由，线路级异常中转将被自动跳过`
      : "自动切换已关闭，新请求仍会继续尝试当前中转";
    overallTone = "degraded";
  } else {
    overallTitle = "所有中转当前不可用";
    overallDescription = "网关仍在运行，但暂时没有能够接收请求的上游";
    overallTone = "down";
  }

  return (
    <div className="monitor-page">
      <section className={`overall-status ${overallTone}`}>
        <div className="overall-main">
          <span className="overall-indicator" />
          <div>
            <strong>{overallTitle}</strong>
            <p>{overallDescription}</p>
          </div>
        </div>
        <div className="updated-at">
          <span>最近更新</span>
          <strong>
            {lastUpdatedAt
              ? lastUpdatedAt.toLocaleTimeString("zh-CN", { hour12: false })
              : "--:--:--"}
          </strong>
        </div>
      </section>
      {latestSwitch && (
        <button className="switch-banner" onClick={() => onOpenRoutingEvent(latestSwitch)}>
          <span><ShieldCheck size={16} /><strong>最近一次自动切换</strong></span>
          <span>{latestSwitch.reason} · 点击查看处理详情 <ChevronRight size={15} /></span>
        </button>
      )}

      <section className="monitor-summary">
        <MonitorMetric
          label="当前路由"
          value={activeProvider?.name ?? "暂无可用中转"}
          detail={
            routeGroup?.locked_provider_id ? "已手动设为首选" : "按优先级自动选择"
          }
          tone={activeProvider ? "green" : "red"}
        />
        <MonitorMetric
          label="可用中转"
          value={`${usableProviders.length} / ${enabledProviders.length}`}
          detail={`${healthyProviders.length} 个状态正常`}
          tone="green"
        />
        <MonitorMetric
          label="累计成功率"
          value={successRate === null ? "暂无请求" : `${successRate}%`}
          detail={`共记录 ${runtime?.total_requests ?? 0} 次请求与探测`}
          tone="blue"
        />
        <MonitorMetric
          label="平均响应"
          value={
            runtime?.average_latency_ms
              ? `${runtime.average_latency_ms} ms`
              : "等待数据"
          }
          detail="最近一次各中转延迟均值"
          tone="slate"
        />
      </section>

      <section className="panel monitor-panel">
        <div className="monitor-section-head">
          <div>
            <p className="section-kicker">PROVIDER STATUS</p>
            <h2>中转实时状态</h2>
            <span>
              页面每 15 秒刷新 · 健康探测每 {settings?.probe_interval_sec ?? 30} 秒执行
            </span>
          </div>
          <div className="live-badge">
            <span />
            实时
          </div>
        </div>
        {providers.length ? (
          <div className="monitor-table-wrap">
            <div className="monitor-table-header">
              <span>中转</span>
              <span>路由角色</span>
              <span>状态</span>
              <span>响应</span>
              <span>成功率</span>
              <span>最近状态</span>
              <span />
            </div>
            {providers.map((provider, index) => {
              const snapshot = snapshots.get(provider.id);
              const state = provider.enabled
                ? (snapshot?.state ?? "unknown")
                : "unknown";
              const providerSuccessRate = snapshot?.total_requests
                ? Math.round(
                    (snapshot.successful_requests / snapshot.total_requests) *
                      100,
                  )
                : null;
              const isActive = activeProvider?.id === provider.id;
              const isLocked = routeGroup?.locked_provider_id === provider.id;
              return (
                <div
                  className={`monitor-provider-row ${!provider.enabled ? "disabled" : ""}`}
                  key={provider.id}
                >
                  <div className="monitor-provider-name">
                    <strong>{provider.name}</strong>
                    <span>{provider.base_url}</span>
                  </div>
                  <div>
                    <span className={`route-role ${isActive ? "active" : ""}`}>
                      {isActive
                        ? "当前路由"
                        : provider.enabled
                          ? `备用 ${index + 1}`
                          : "已停用"}
                    </span>
                  </div>
                  <div
                    className={`monitor-state ${state === "open" && !failoverEnabled ? "degraded" : state}`}
                  >
                    <span />
                    {provider.enabled
                      ? monitorStatusLabel(snapshot, failoverEnabled)
                      : "已停用"}
                  </div>
                  <strong className="monitor-value">
                    {snapshot?.latency_ms ? `${snapshot.latency_ms} ms` : "--"}
                  </strong>
                  <strong className="monitor-value">
                    {providerSuccessRate === null
                      ? "--"
                      : `${providerSuccessRate}%`}
                  </strong>
                  <HealthStrip samples={snapshot?.recent_probes ?? []} />
                  <div className="monitor-row-actions">
                    <button
                      className="text-button"
                      disabled={!provider.enabled}
                      onClick={() => onTest(provider)}
                    >
                      测试
                    </button>
                    <button
                      className={`text-button ${isLocked ? "selected" : ""}`}
                      disabled={!provider.enabled}
                      onClick={() => onLock(provider)}
                    >
                      {isLocked ? "取消首选" : "设为首选"}
                    </button>
                  </div>
                  {snapshot?.last_error && (
                    <div className="monitor-row-error">
                      {snapshot.last_error}
                      {!failoverEnabled && snapshot.state === "open"
                        ? "。自动切换已关闭，新请求仍会继续尝试该中转"
                        : ""}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        ) : (
          <div className="empty-state compact">
            <span className="empty-icon">
              <Activity size={22} />
            </span>
            <h3>暂无中转状态</h3>
            <p>请先前往“中转管理”添加并启用中转。</p>
          </div>
        )}
      </section>

      <section className="panel monitor-events">
        <div className="monitor-section-head compact">
          <div>
            <p className="section-kicker">RECENT ACTIVITY</p>
            <h2>最近运行事件</h2>
          </div>
          <span className="privacy-note">不记录请求正文和密钥</span>
        </div>
        {logs.length ? (
          <div className="monitor-event-list">
            {logs.slice(0, 6).map((entry, index) => {
              const provider = providers.find(
                (item) => item.id === entry.provider_id,
              );
              const sourceProvider = providers.find(
                (item) => item.id === entry.routing_event?.source_provider_id,
              );
              const targetProvider = providers.find(
                (item) => item.id === entry.routing_event?.target_provider_id,
              );
              return (
                <div
                  className={`monitor-event-row ${entry.routing_event ? "clickable" : ""}`}
                  key={`${entry.timestamp}-${index}`}
                  onClick={() => entry.routing_event && onOpenRoutingEvent(entry.routing_event)}
                >
                  <time>
                    {new Date(entry.timestamp).toLocaleTimeString("zh-CN", {
                      hour: "2-digit",
                      minute: "2-digit",
                      second: "2-digit",
                      hour12: false,
                    })}
                  </time>
                  <span className={`event-indicator ${entry.level}`} />
                  <div>
                    <strong>
                      {entry.routing_event
                        ? entry.routing_event.event_type === "auto_switch"
                          ? `${sourceProvider?.name ?? "原中转"} → ${targetProvider?.name ?? "备用中转"}`
                          : sourceProvider?.name ?? provider?.name ?? "未知中转"
                        : provider?.name ?? "本地网关"}
                    </strong>
                    <span>{entry.message}</span>
                  </div>
                  <span className={`event-result ${entry.level}`}>
                    {entry.level === "error"
                      ? "错误"
                      : entry.level === "warning"
                        ? "已处理"
                        : "正常"}
                  </span>
                </div>
              );
            })}
          </div>
        ) : (
          <div className="monitor-empty-event">运行事件将在这里按时间显示</div>
        )}
      </section>
    </div>
  );
}

function MonitorMetric({
  label,
  value,
  detail,
  tone,
}: {
  label: string;
  value: string;
  detail: string;
  tone: "green" | "blue" | "slate" | "red";
}) {
  return (
    <div className={`monitor-metric ${tone}`}>
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{detail}</small>
    </div>
  );
}

function HealthStrip({ samples }: { samples: HealthProbeSample[] }) {
  const placeholders = Math.max(0, MAX_HEALTH_PROBES - samples.length);
  return (
    <div className="health-strip" aria-label="本次会话最近状态">
      {Array.from({ length: placeholders }).map((_, index) => (
        <span className="empty" key={`empty-${index}`} title="尚无状态样本" />
      ))}
      {samples.map((sample, index) => (
        <span
          className={sample.state}
          key={sample.sequence}
          title={`${new Date(sample.timestamp).toLocaleTimeString("zh-CN", { hour12: false })} · ${statusLabels[sample.state]}${sample.latency_ms !== null ? ` · ${sample.latency_ms} ms` : ""}${sample.error ? ` · ${sample.error}` : ""}`}
        />
      ))}
    </div>
  );
}
function EmptyState({ onAdd }: { onAdd: () => void }) {
  return (
    <div className="empty-state">
      <span className="empty-icon">
        <Server size={22} />
      </span>
      <h3>还没有配置中转</h3>
      <p>添加第一个上游中转，网关就可以接管 Codex 的请求。</p>
      <button className="secondary-button" onClick={onAdd}>
        <Plus size={15} />
        添加中转
      </button>
    </div>
  );
}

function ProviderRow({
  provider,
  health,
  index,
  last,
  selected,
  onSelect,
  onEdit,
  onToggle,
  onMoveUp,
  onMoveDown,
  onTest,
  onDelete,
}: {
  provider: Provider;
  health?: HealthSnapshot;
  index: number;
  last: boolean;
  selected: boolean;
  onSelect: () => void;
  onEdit: () => void;
  onToggle: () => void;
  onMoveUp: () => void;
  onMoveDown: () => void;
  onTest: () => void;
  onDelete: () => void;
}) {
  const state = health?.state ?? "unknown";
  return (
    <div
      className={`provider-row ${selected ? "selected" : ""}`}
      onClick={onSelect}
    >
      <div className="priority">{index + 1}</div>
      <div className="provider-main">
        <div className="provider-title">
          <strong>{provider.name}</strong>
          <span className={`status-pill ${state}`}>{statusLabels[state]}</span>
        </div>
        <div className="provider-meta-line">
          <span className="provider-url" title={provider.base_url}>
            {provider.base_url}
          </span>
          <span className="provider-meta-separator" aria-hidden="true">·</span>
          <span
            className="provider-models"
            title={provider.models.length ? provider.models.join(" · ") : "匹配所有模型"}
          >
            {provider.models.length
              ? provider.models.join(" · ")
              : "匹配所有模型"}
          </span>
        </div>
      </div>
      <div className="provider-metrics">
        <strong>{health?.latency_ms ? `${health.latency_ms} ms` : "--"}</strong>
        <span>延迟</span>
      </div>
      <button
        className={`toggle ${provider.enabled ? "checked" : ""}`}
        title={provider.enabled ? "停用中转" : "启用中转"}
        onClick={(event) => {
          event.stopPropagation();
          onToggle();
        }}
      >
        <span />
      </button>
      <div className="row-menu">
        <button
          className="icon-button"
          title="提高优先级"
          disabled={index === 0}
          onClick={(event) => {
            event.stopPropagation();
            onMoveUp();
          }}
        >
          <ArrowUp size={15} />
        </button>
        <button
          className="icon-button"
          title="降低优先级"
          disabled={last}
          onClick={(event) => {
            event.stopPropagation();
            onMoveDown();
          }}
        >
          <ArrowDown size={15} />
        </button>
        <button
          className="icon-button"
          title="测试连接"
          onClick={(event) => {
            event.stopPropagation();
            onTest();
          }}
        >
          <Activity size={16} />
        </button>
        <button
          className="icon-button"
          title="编辑中转"
          onClick={(event) => {
            event.stopPropagation();
            onEdit();
          }}
        >
          <Pencil size={15} />
        </button>
        <button
          className="icon-button danger-icon"
          title="删除中转"
          onClick={(event) => {
            event.stopPropagation();
            onDelete();
          }}
        >
          <Trash2 size={16} />
        </button>
      </div>
    </div>
  );
}

function ProviderDrawer({
  mode,
  provider,
  health,
  locked,
  saving,
  closing,
  gatewayUrl,
  onClose,
  onDirtyChange,
  onSavingChange,
  onEdit,
  onTest,
  onLock,
  onSaved,
}: {
  mode: "detail" | "create" | "edit";
  provider: Provider | null;
  health?: HealthSnapshot;
  locked: boolean;
  saving: boolean;
  closing: boolean;
  gatewayUrl: string;
  onClose: () => void;
  onDirtyChange: (dirty: boolean) => void;
  onSavingChange: (saving: boolean) => void;
  onEdit: () => void;
  onTest: () => void;
  onLock: () => void;
  onSaved: (provider: Provider) => Promise<void>;
}) {
  useEffect(() => {
    function closeOnEscape(event: KeyboardEvent) {
      if (event.key === "Escape") onClose();
    }
    window.addEventListener("keydown", closeOnEscape);
    return () => window.removeEventListener("keydown", closeOnEscape);
  }, [onClose]);

  const title =
    mode === "create" ? "添加中转" : mode === "edit" ? "编辑中转" : "中转详情";

  return (
    <div
      className={`provider-drawer-backdrop ${closing ? "closing" : ""}`}
      role="presentation"
      onClick={onClose}
    >
      <aside
        className={`provider-drawer ${closing ? "closing" : ""}`}
        role="dialog"
        aria-modal="true"
        aria-labelledby="provider-drawer-title"
        onClick={(event) => event.stopPropagation()}
      >
        <header className="provider-drawer-head">
          <div>
            <p className="eyebrow">
              {mode === "detail" ? "PROVIDER DETAIL" : "PROVIDER CONFIGURE"}
            </p>
            <h2 id="provider-drawer-title">{title}</h2>
          </div>
          <button
            type="button"
            className="icon-button"
            title={saving ? "正在保存" : "关闭"}
            disabled={saving}
            onClick={onClose}
          >
            <X size={18} />
          </button>
        </header>
        <div className="provider-drawer-body">
          {mode === "detail" && provider ? (
            <ProviderDetail
              provider={provider}
              health={health}
              locked={locked}
              gatewayUrl={gatewayUrl}
              onEdit={onEdit}
              onTest={onTest}
              onLock={onLock}
            />
          ) : mode === "create" || provider ? (
            <ProviderForm
              key={mode === "create" ? "create" : provider?.id}
              provider={mode === "edit" ? provider : null}
              onSaved={onSaved}
              onCancel={onClose}
              onDirtyChange={onDirtyChange}
              onSavingChange={onSavingChange}
            />
          ) : null}
        </div>
      </aside>
    </div>
  );
}

function ProviderUnsavedModal({
  onContinue,
  onDiscard,
}: {
  onContinue: () => void;
  onDiscard: () => void;
}) {
  return (
    <div className="modal-backdrop provider-unsaved-backdrop" role="presentation">
      <div
        className="unsaved-modal panel"
        role="dialog"
        aria-modal="true"
        aria-labelledby="provider-unsaved-title"
      >
        <div className="unsaved-modal-icon">
          <Save size={20} />
        </div>
        <h2 id="provider-unsaved-title">中转配置尚未保存</h2>
        <p>关闭抽屉会丢失刚才填写的内容。</p>
        <div className="unsaved-actions">
          <button type="button" className="secondary-button" onClick={onContinue}>
            继续编辑
          </button>
          <button
            type="button"
            className="secondary-button danger-text"
            onClick={onDiscard}
          >
            放弃修改
          </button>
        </div>
      </div>
    </div>
  );
}

function ProviderDetail({
  provider,
  health,
  locked,
  gatewayUrl,
  onEdit,
  onTest,
  onLock,
}: {
  provider: Provider;
  health?: HealthSnapshot;
  locked: boolean;
  gatewayUrl: string;
  onEdit: () => void;
  onTest: () => void;
  onLock: () => void;
}) {
  return (
    <div className="detail-content">
      <div className="detail-head">
        <div>
          <p className="eyebrow">
            PROVIDER {provider.priority.toString().padStart(2, "0")}
          </p>
          <h2>{provider.name}</h2>
        </div>
        <div className="detail-actions">
          <button className="secondary-button" onClick={onLock}>
            {locked ? <PinOff size={14} /> : <Pin size={14} />}
            {locked ? "取消首选" : "设为首选"}
          </button>
          <button className="secondary-button" onClick={onEdit}>
            编辑
          </button>
        </div>
      </div>
      <div className="detail-status">
        <span className={`large-status ${health?.state ?? "unknown"}`} />
        <div>
          <strong>
            {locked ? "当前首选 · " : ""}
            {statusLabels[health?.state ?? "unknown"]}
          </strong>
          <span>{health?.last_error ?? "最近一次探测没有错误"}</span>
        </div>
        <button className="secondary-button small" onClick={onTest}>
          <Activity size={15} />
          测试连接
        </button>
      </div>
      <div className="detail-section">
        <h3>连接信息</h3>
        <DetailLine label="Base URL" value={provider.base_url} />
        <DetailLine
          label="支持模型"
          value={
            provider.models.length ? provider.models.join(", ") : "所有模型"
          }
        />
        <DetailLine
          label="API Key"
          value={
            provider.api_key_error ??
            (provider.has_api_key ? "已安全保存" : "未配置，请重新填写")
          }
          icon={<KeyRound size={15} />}
        />
        <DetailLine label="响应头超时" value={`${provider.timeout_ms} ms`} />
      </div>
      <div className="detail-section">
        <h3>本地接入</h3>
        <div className="copy-field">
          <span>{gatewayUrl}</span>
          <button
            className="icon-button"
            title="复制地址"
            onClick={() => void navigator.clipboard?.writeText(gatewayUrl)}
          >
            <Copy size={15} />
          </button>
        </div>
        <p className="field-hint">
          在“网关设置”中复制本地 API Key，与此 Base URL 一起填入 cc-switch；真实中转切换无需重启 Codex。
        </p>
      </div>
    </div>
  );
}
function DetailLine({
  label,
  value,
  icon,
}: {
  label: string;
  value: string;
  icon?: ReactNode;
}) {
  return (
    <div className="detail-line">
      <span>{label}</span>
      <strong>
        {icon}
        {value}
      </strong>
    </div>
  );
}

function LogsPanel({
  logs,
  providers,
  runtime,
  onOpenRoutingEvent,
  onOpenDiagnostic,
}: {
  logs: EventLog[];
  providers: Provider[];
  runtime: RuntimeStats | null;
  onOpenRoutingEvent: (event: RoutingEvent) => void;
  onOpenDiagnostic: (detail: DiagnosticDetail) => void;
}) {
  const providerNames = new Map(
    providers.map((provider) => [provider.id, provider.name]),
  );
  const successRate = runtime?.total_requests
    ? Math.round((runtime.successful_requests / runtime.total_requests) * 100)
    : 0;
  const [pageSize, setPageSize] = useState<50 | 100 | 150>(50);
  const [page, setPage] = useState(1);
  const totalPages = Math.max(1, Math.ceil(logs.length / pageSize));
  const pageLogs = logs.slice((page - 1) * pageSize, page * pageSize);

  // 页数较多时保留首尾和当前页邻近页，避免分页栏被完整页码撑开。
  const pageItems: Array<number | "ellipsis"> = totalPages <= 7
    ? Array.from({ length: totalPages }, (_, index) => index + 1)
    : [
        1,
        ...(page > 3 ? ["ellipsis" as const] : []),
        ...Array.from(
          { length: 5 },
          (_, index) => Math.min(Math.max(page - 2 + index, 2), totalPages - 1),
        ).filter((item, index, items) => items.indexOf(item) === index),
        ...(page < totalPages - 2 ? ["ellipsis" as const] : []),
        totalPages,
      ];

  useEffect(() => {
    setPage((current) => Math.min(current, totalPages));
  }, [totalPages]);

  return (
    <section className="logs-layout">
      <div className="panel log-panel">
        <div className="panel-heading">
          <h2>最近事件</h2>
        </div>
        {logs.length ? (
          <>
            <div className="log-table-scroll">
              <div className="log-table-header">
                <span />
                <span>时间</span>
                <span>中转</span>
                <span>模型</span>
                <span>事件</span>
              </div>
              <div className="log-list">
                {pageLogs.map((entry, index) => (
                  <div
                    className={`log-row ${entry.routing_event || entry.diagnostic_detail ? "clickable" : ""}`}
                    key={`${entry.timestamp}-${index}`}
                    onClick={() => {
                      if (entry.routing_event) onOpenRoutingEvent(entry.routing_event);
                      else if (entry.diagnostic_detail) onOpenDiagnostic(entry.diagnostic_detail);
                    }}
                  >
                    <span className={`log-level ${entry.level}`} />
                    <time>
                      {new Date(entry.timestamp).toLocaleTimeString("zh-CN", {
                        hour12: false,
                      })}
                    </time>
                    <strong>
                      {entry.routing_event
                        ? entry.routing_event.event_type === "auto_switch"
                          ? `${providerNames.get(entry.routing_event.source_provider_id ?? "") ?? "原中转"} → ${providerNames.get(entry.routing_event.target_provider_id ?? "") ?? "备用中转"}`
                          : providerNames.get(entry.routing_event.source_provider_id ?? "") ?? "未知中转"
                        : entry.provider_id
                          ? (providerNames.get(entry.provider_id) ?? "未知中转")
                          : "本地网关"}
                    </strong>
                    <code className="log-model">{entry.model ?? "--"}</code>
                    <span className="log-message">{entry.message}</span>
                  </div>
                ))}
              </div>
            </div>
            <div className="log-pagination">
              <span>
                共 {logs.length} 条，第 {page} / {totalPages} 页
              </span>
              <div className="log-pagination-controls">
                <button
                  className="icon-button"
                  aria-label="上一页"
                  title="上一页"
                  disabled={page === 1}
                  onClick={() => setPage((current) => Math.max(1, current - 1))}
                >
                  <ChevronLeft size={15} />
                </button>
                <nav className="log-page-nav" aria-label="日志分页">
                  {pageItems.map((item, index) => item === "ellipsis" ? (
                    <span className="log-page-ellipsis" key={`ellipsis-${index}`} aria-hidden="true">…</span>
                  ) : (
                    <button
                      className={`log-page-button ${item === page ? "active" : ""}`}
                      aria-current={item === page ? "page" : undefined}
                      aria-label={`第 ${item} 页`}
                      key={item}
                      onClick={() => setPage(item)}
                    >
                      {item}
                    </button>
                  ))}
                </nav>
                <button
                  className="icon-button"
                  aria-label="下一页"
                  title="下一页"
                  disabled={page === totalPages}
                  onClick={() => setPage((current) => Math.min(totalPages, current + 1))}
                >
                  <ChevronRight size={15} />
                </button>
                <label className="page-size-control">
                  <span>每页</span>
                  <select
                    value={pageSize}
                    onChange={(event) => {
                      setPageSize(Number(event.target.value) as 50 | 100 | 150);
                      setPage(1);
                    }}
                  >
                    <option value={50}>50 条</option>
                    <option value={100}>100 条</option>
                    <option value={150}>150 条</option>
                  </select>
                </label>
              </div>
            </div>
          </>
        ) : (
          <div className="empty-state compact">
            <span className="empty-icon">
              <ScrollText size={22} />
            </span>
            <h3>还没有运行事件</h3>
            <p>中转探测或请求发生后，状态会显示在这里。</p>
          </div>
        )}
      </div>
      <div className="panel runtime-panel">
        <h3>运行统计</h3>
        <DetailLine
          label="请求总数"
          value={String(runtime?.total_requests ?? 0)}
        />
        <DetailLine label="成功率" value={`${successRate}%`} />
        <DetailLine
          label="平均延迟"
          value={
            runtime?.average_latency_ms
              ? `${runtime.average_latency_ms} ms`
              : "--"
          }
        />
      </div>
    </section>
  );
}

function ErrorHandlingMatrix() {
  return (
    <div className="panel rules-matrix">
      <div className="panel-heading">
        <div>
          <p className="section-kicker">ERROR MATRIX</p>
          <h2>错误处理矩阵</h2>
          <p>每类错误只在适合的阶段重试或切换。</p>
        </div>
      </div>
      {/* 矩阵与 Rust 的错误分类保持同一口径，避免页面说明和实际路由行为分叉。 */}
      <div className="matrix-table">
        <div className="matrix-row matrix-head">
          <span>错误类型</span><span>当前处理</span><span>重试</span><span>切换</span>
        </div>
        {[
          ["模型容量 / 账号并发", "只在当前中转按统一策略重试", "是", "否"],
          ["连接失败 / 超时", "按统一策略等待后重试", "是", "开关开启后"],
          ["408 / 429", "按 Retry-After 和统一策略重试", "是", "开关开启后"],
          ["SSE failed / 临时 incomplete", "仅在业务输出前按分类重试", "是", "开关开启后"],
          ["未知 SSE / 协议错误", "受兜底开关控制；输出后委托 Codex", "输出前", "开关开启后"],
          ["max_output_tokens / 内容过滤", "按业务终态原样透传", "否", "否"],
          ["413", "开关关闭时原样返回", "否", "开关开启且策略允许"],
          ["5xx", "按统一策略等待后重试", "是", "开关开启后"],
          ["400 / 401 / 403 / 404 / 422", "原样返回请求错误", "否", "否"],
        ].map(([kind, handling, retry, switchTo]) => (
          <div className="matrix-row" key={kind}>
            <strong>{kind}</strong><span>{handling}</span><span>{retry}</span><span>{switchTo}</span>
          </div>
        ))}
      </div>
      <div className="matrix-explanation">
        <CircleHelp size={17} />
        <div>
          <strong>为什么默认不自动切换？</strong>
          <p>关闭开关可以保证请求始终落在你选择的中转，避免普通 503、并发限制或熔断状态造成意外换线。打开后才会按优先级尝试备用线路。</p>
        </div>
      </div>
      <div className="matrix-explanation">
        <CircleHelp size={17} />
        <div>
          <strong>模型容量不足为什么不切换？</strong>
          <p>该错误按模型和中转单独处理，只在可见输出前重试原中转。文本、推理或工具调用已经开始后不再重放，避免重复内容和重复计费。</p>
        </div>
      </div>
    </div>
  );
}

function FieldHint({ label, hint }: { label: string; hint: string }) {
  const tooltipId = useId();
  const triggerRef = useRef<HTMLButtonElement>(null);
  const tooltipRef = useRef<HTMLDivElement>(null);
  const [open, setOpen] = useState(false);
  const [pinned, setPinned] = useState(false);
  const [position, setPosition] = useState<{ top: number; left: number; placement: "top" | "bottom" } | null>(null);

  const updatePosition = useCallback(() => {
    const trigger = triggerRef.current;
    const tooltip = tooltipRef.current;
    if (!trigger || !tooltip) return;

    // Tooltip 通过 Portal 脱离面板裁剪，需要按视口空间决定上下方向并限制在窗口内。
    const viewportPadding = 12;
    const gap = 8;
    const triggerRect = trigger.getBoundingClientRect();
    const tooltipRect = tooltip.getBoundingClientRect();
    const contentRect = trigger.closest(".main-content")?.getBoundingClientRect();
    const preferredLeft = triggerRect.left + triggerRect.width / 2 - tooltipRect.width / 2;
    const minLeft = Math.max(viewportPadding, (contentRect?.left ?? 0) + viewportPadding);
    const contentRight = Math.min(window.innerWidth, contentRect?.right ?? window.innerWidth);
    const maxLeft = Math.max(minLeft, contentRight - viewportPadding - tooltipRect.width);
    const left = Math.min(Math.max(preferredLeft, minLeft), maxLeft);
    const bottomTop = triggerRect.bottom + gap;
    const hasRoomBelow = bottomTop + tooltipRect.height <= window.innerHeight - viewportPadding;
    const placement = hasRoomBelow ? "bottom" : "top";
    const top = placement === "bottom"
      ? bottomTop
      : Math.max(viewportPadding, triggerRect.top - gap - tooltipRect.height);

    setPosition({ top, left, placement });
  }, []);

  useLayoutEffect(() => {
    if (!open) {
      setPosition(null);
      return;
    }

    updatePosition();
    window.addEventListener("resize", updatePosition);
    window.addEventListener("scroll", updatePosition, true);
    return () => {
      window.removeEventListener("resize", updatePosition);
      window.removeEventListener("scroll", updatePosition, true);
    };
  }, [open, updatePosition]);

  useEffect(() => {
    if (!open) return;

    function closeOnOutsidePointer(event: PointerEvent) {
      const target = event.target;
      if (!(target instanceof Node)) return;
      if (triggerRef.current?.contains(target) || tooltipRef.current?.contains(target)) return;
      setPinned(false);
      setOpen(false);
    }

    document.addEventListener("pointerdown", closeOnOutsidePointer, true);
    return () => document.removeEventListener("pointerdown", closeOnOutsidePointer, true);
  }, [open]);

  const tooltip = open
    ? createPortal(
      <div
        ref={tooltipRef}
        id={tooltipId}
        className="field-hint-tooltip"
        role="tooltip"
        data-placement={position?.placement ?? "bottom"}
        style={{
          top: position?.top ?? 0,
          left: position?.left ?? 0,
          visibility: position ? "visible" : "hidden",
        }}
      >
        {hint}
      </div>,
      document.body,
    )
    : null;

  return (
    <>
      <button
        ref={triggerRef}
        type="button"
        className="field-hint-trigger"
        aria-label={`${label}说明`}
        aria-describedby={open ? tooltipId : undefined}
        aria-expanded={open}
        onPointerEnter={() => setOpen(true)}
        onPointerLeave={() => {
          if (!pinned && document.activeElement !== triggerRef.current) setOpen(false);
        }}
        onFocus={() => setOpen(true)}
        onBlur={() => {
          setPinned(false);
          setOpen(false);
        }}
        onClick={() => {
          const nextPinned = !pinned;
          setPinned(nextPinned);
          setOpen(nextPinned);
        }}
        onKeyDown={(event) => {
          if (event.key !== "Escape") return;
          event.stopPropagation();
          setPinned(false);
          setOpen(false);
        }}
      >
        <Info size={15} aria-hidden="true" />
      </button>
      {tooltip}
    </>
  );
}

function ConfigurationFieldRow({
  label,
  hint,
  inputId,
  children,
  className = "",
}: {
  label: string;
  hint?: string;
  inputId?: string;
  children: ReactNode;
  className?: string;
}) {
  return (
    <div className={`configuration-field-row ${className}`.trim()}>
      <div className="configuration-field-copy">
        <div className="configuration-field-label-line">
          {inputId
            ? <label className="field-label-text" htmlFor={inputId}>{label}</label>
            : <span className="field-label-text">{label}</span>}
          {hint && <FieldHint label={label} hint={hint} />}
        </div>
      </div>
      <div className="configuration-field-control">{children}</div>
    </div>
  );
}

function NumberStepper({ label, hint, value, min, max, onChange }: { label: string; hint?: string; value: number; min: number; max?: number; onChange: (value: number) => void }) {
  const inputId = useId();
  const [draft, setDraft] = useState(String(value));

  useEffect(() => {
    setDraft(String(value));
  }, [value]);

  function normalize(next: number) {
    const rounded = Math.round(next);
    if (!Number.isSafeInteger(rounded)) return null;
    return max === undefined
      ? Math.max(min, rounded)
      : Math.min(max, Math.max(min, rounded));
  }

  function commitDraft() {
    // 编辑多位数字时允许输入框暂时为空；只有失焦或提交时才恢复旧值并限制到合法范围。
    if (!draft.trim()) {
      setDraft(String(value));
      return;
    }
    const parsed = Number(draft);
    const next = normalize(parsed);
    if (next === null) {
      setDraft(String(value));
      return;
    }
    setDraft(String(next));
    if (next !== value) onChange(next);
  }

  function stepBy(delta: number) {
    const parsed = Number(draft);
    const base = Number.isFinite(parsed) && draft.trim() ? parsed : value;
    const next = normalize(base + delta);
    if (next === null) return;
    setDraft(String(next));
    if (next !== value) onChange(next);
  }

  const numericDraft = Number(draft);
  const current = Number.isFinite(numericDraft) && draft.trim() ? numericDraft : value;

  return (
    <ConfigurationFieldRow label={label} hint={hint} inputId={inputId} className="number-field">
      <div className="number-stepper">
        <input
          id={inputId}
          type="text"
          inputMode="numeric"
          pattern="[0-9]*"
          value={draft}
          onChange={(event) => {
            const next = event.target.value;
            if (next === "" || /^\d+$/.test(next)) {
              setDraft(next);
              const parsed = Number(next);
              if (
                next
                && Number.isSafeInteger(parsed)
                && parsed >= min
                && (max === undefined || parsed <= max)
                && parsed !== value
              ) {
                onChange(parsed);
              }
            }
          }}
          onBlur={commitDraft}
          onKeyDown={(event) => {
            if (event.key === "ArrowDown") {
              event.preventDefault();
              stepBy(-1);
            } else if (event.key === "ArrowUp") {
              event.preventDefault();
              stepBy(1);
            } else if (event.key === "Enter") {
              event.preventDefault();
              commitDraft();
              event.currentTarget.blur();
            }
          }}
        />
        <button type="button" title={`减少${label}`} aria-label={`减少${label}`} disabled={current <= min} onClick={() => stepBy(-1)}>
          <Minus size={14} />
        </button>
        <button type="button" title={`增加${label}`} aria-label={`增加${label}`} disabled={max !== undefined && current >= max} onClick={() => stepBy(1)}>
          <Plus size={14} />
        </button>
      </div>
    </ConfigurationFieldRow>
  );
}

function RoutingDetailModal({ event, onClose }: { event: RoutingEvent; onClose: () => void }) {
  const isModelRetryEvent = [
    "model_availability_recovered",
    "model_temporarily_unavailable",
    "capacity_recovered",
    "capacity_exhausted",
  ].includes(event.event_type);
  const resultLabel = event.final_result === "switched"
    ? "已切换并成功"
    : event.final_result === "model_availability_recovered" || event.final_result === "capacity_recovered"
      ? "同一中转重试成功"
      : event.final_result === "model_temporarily_unavailable" || event.final_result === "capacity_exhausted"
        ? "模型级重试已耗尽"
        : event.final_result;
  return <div className="modal-backdrop" role="presentation" onClick={onClose}><div className="routing-modal panel" role="dialog" aria-modal="true" onClick={(click) => click.stopPropagation()}>
    <div className="panel-heading"><div><p className="section-kicker">ROUTING DETAIL</p><h2>{isModelRetryEvent ? "模型可用性重试详情" : "自动切换处理详情"}</h2><p>{new Date(event.timestamp).toLocaleString("zh-CN")}</p></div><button className="icon-button" title="关闭详情" onClick={onClose}><X size={17} /></button></div>
    <div className="routing-detail-body">
      <div className="routing-detail-grid"><DetailLine label="模型" value={event.model ?? "未提供"} /><DetailLine label="接口路径" value={event.request_path} /><DetailLine label="请求体大小" value={`${event.request_body_bytes.toLocaleString()} 字节`} /><DetailLine label="重试次数" value={String(event.retry_count)} /><DetailLine label="最终状态" value={event.status_code ? String(event.status_code) : "未收到响应"} /><DetailLine label="最终结果" value={resultLabel} /></div>
      <div className="routing-reason"><strong>处理原因</strong><span>{event.reason}</span></div>
      <h3>{isModelRetryEvent ? "同一中转重试过程" : "中转尝试过程"}</h3>
      <div className="attempt-list">{event.attempts.map((attempt, index) => <div className="attempt-row" key={`${attempt.provider_id}-${index}`}><span className="attempt-index">{index + 1}</span><div><strong>{attempt.provider_name}</strong><span>{attempt.error ?? `响应 ${attempt.status_code ?? "成功"}`} · {attempt.latency_ms ?? "--"} ms</span></div><b>{attempt.switched ? "已切换" : attempt.retry_count ? `重试 ${attempt.retry_count}` : "已尝试"}</b></div>)}</div>
    </div>
  </div></div>;
}

function DiagnosticDetailModal({ detail, onClose }: { detail: DiagnosticDetail; onClose: () => void }) {
  const classLabels: Record<DiagnosticDetail["error_class"], string> = {
    retryable: "明确临时错误",
    permanent: "明确永久错误",
    incomplete_business: "业务性不完整结束",
    unknown: "未识别错误",
  };
  return <div className="modal-backdrop" role="presentation" onClick={onClose}><div className="routing-modal panel" role="dialog" aria-modal="true" onClick={(click) => click.stopPropagation()}>
    <div className="panel-heading"><div><p className="section-kicker">DIAGNOSTIC DETAIL</p><h2>流式错误诊断详情</h2><p>请求标识 {detail.request_id}</p></div><button className="icon-button" title="关闭详情" onClick={onClose}><X size={17} /></button></div>
    <div className="routing-detail-body">
      <div className="routing-detail-grid">
        <DetailLine label="错误分类" value={classLabels[detail.error_class]} />
        <DetailLine label="最终处理" value={detail.action} />
        <DetailLine label="模型" value={detail.model ?? "未提供"} />
        <DetailLine label="接口路径" value={detail.request_path} />
        <DetailLine label="HTTP 状态" value={detail.http_status ? String(detail.http_status) : "未收到"} />
        <DetailLine label="上游请求标识" value={detail.upstream_request_id ?? "未提供"} />
        <DetailLine label="错误代码" value={detail.error_code ?? "未提供"} />
        <DetailLine label="错误类型" value={detail.error_type ?? "未提供"} />
        <DetailLine label="Incomplete 原因" value={detail.incomplete_reason ?? "不适用"} />
        <DetailLine label="最后 SSE 事件" value={detail.last_sse_event ?? "无"} />
        <DetailLine label="合法终态" value={detail.terminal_received ? (detail.terminal_event ?? "已收到") : "未收到"} />
        <DetailLine label="业务输出已开始" value={detail.business_output_started ? "是，不由网关重放" : "否"} />
        <DetailLine label="接收字节" value={detail.bytes_received.toLocaleString()} />
        <DetailLine label="耗时" value={`${detail.elapsed_ms} ms`} />
        <DetailLine label="当前尝试" value={`${detail.attempt_index} / ${detail.max_attempts}`} />
        <DetailLine label="自动切换" value={detail.failover_enabled ? "已开启" : "已关闭"} />
      </div>
      <div className="routing-reason"><strong>标准错误消息</strong><span>{detail.error_message ?? "上游未提供标准错误消息"}</span></div>
      {detail.parse_error && <div className="routing-reason"><strong>解析错误</strong><span>{detail.parse_error}</span></div>}
      <h3>已记录的尝试</h3>
      <div className="attempt-list">{detail.attempts.length ? detail.attempts.map((attempt, index) => <div className="attempt-row" key={`${attempt.provider_id}-${index}`}><span className="attempt-index">{index + 1}</span><div><strong>{attempt.provider_name}</strong><span>{attempt.error ?? `响应 ${attempt.status_code ?? "成功"}`} · {attempt.latency_ms ?? "--"} ms</span></div><b>{attempt.switched ? "准备切换" : attempt.retry_count ? `重试 ${attempt.retry_count}` : "已尝试"}</b></div>) : <p className="empty-state">本次诊断发生在尝试记录写入前。</p>}</div>
    </div>
  </div></div>;
}

function providerModelsUrlPreview(value: string) {
  const cleaned = value
    .trim()
    .replace(/^["'“”‘’]+|["'“”‘’]+$/g, "")
    .replace(/\/+$/, "");
  return cleaned.endsWith("/v1")
    ? `${cleaned}/models`
    : `${cleaned}/v1/models`;
}

function ProviderForm({
  provider,
  onSaved,
  onCancel,
  onDirtyChange,
  onSavingChange,
}: {
  provider: Provider | null;
  onSaved: (savedProvider: Provider) => Promise<void>;
  onCancel: () => void;
  onDirtyChange: (dirty: boolean) => void;
  onSavingChange: (saving: boolean) => void;
}) {
  const [name, setName] = useState(provider?.name ?? "");
  const [baseUrl, setBaseUrl] = useState(provider?.base_url ?? "");
  const [models, setModels] = useState(provider?.models.join(", ") ?? "");
  const [apiKey, setApiKey] = useState("");
  const [timeoutMs, setTimeoutMs] = useState(provider?.timeout_ms ?? 120000);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const dirty =
    name !== (provider?.name ?? "") ||
    baseUrl !== (provider?.base_url ?? "") ||
    models !== (provider?.models.join(", ") ?? "") ||
    apiKey !== "" ||
    timeoutMs !== (provider?.timeout_ms ?? 120000);

  useEffect(() => {
    // 表单组件可能在新增和编辑之间复用，按中转 ID 同步字段，避免残留上一次输入。
    setName(provider?.name ?? "");
    setBaseUrl(provider?.base_url ?? "");
    setModels(provider?.models.join(", ") ?? "");
    setApiKey("");
    setTimeoutMs(provider?.timeout_ms ?? 120000);
    setError(null);
  }, [provider?.id]);

  useEffect(() => {
    onDirtyChange(dirty);
  }, [dirty, onDirtyChange]);

  useEffect(() => {
    onSavingChange(saving);
  }, [saving, onSavingChange]);

  async function submit(event: FormEvent) {
    event.preventDefault();
    setError(null);
    if (!name.trim() || !baseUrl.trim()) {
      setError("名称和 Base URL 不能为空");
      return;
    }
    setSaving(true);
    try {
      const savedProvider = await saveProvider({
        id: provider?.id,
        name: name.trim(),
        base_url: baseUrl.trim(),
        models: models
          .split(",")
          .map((item) => item.trim())
          .filter(Boolean),
        timeout_ms: timeoutMs,
        api_key: apiKey || undefined,
      });
      await onSaved(savedProvider);
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  }
  return (
    <form className="form-content" onSubmit={submit}>
      <Field
        label="显示名称"
        value={name}
        onChange={setName}
        placeholder="例如：主力中转"
      />
      <Field
        label="Base URL"
        value={baseUrl}
        onChange={setBaseUrl}
        placeholder="https://api.example.com/v1"
        hint={
          baseUrl.trim()
            ? `连接测试地址：${providerModelsUrlPreview(baseUrl)}`
            : "支持填写 https://example.com 或 https://example.com/v1"
        }
      />
      <Field
        label="API Key"
        value={apiKey}
        onChange={setApiKey}
        placeholder={provider?.has_api_key ? "已配置，留空保持不变" : "sk-..."}
        type="password"
        hint={
          provider?.api_key_error ??
          (provider && !provider.has_api_key
            ? "旧版未持久化的密钥无法恢复，请重新填写一次。"
            : undefined)
        }
      />
      <Field
        label="模型匹配"
        value={models}
        onChange={setModels}
        placeholder="留空匹配所有模型，多个模型用逗号分隔"
        hint="只影响路由匹配，不会修改请求中的模型名。"
      />
      <label className="field">
        <span>响应头超时（毫秒）</span>
        <input
          type="number"
          min={1000}
          max={600000}
          value={timeoutMs}
          onChange={(event) => setTimeoutMs(Number(event.target.value))}
        />
        <span className="field-hint">
          只限制连接、发送请求和等待响应头；流式响应建立后不限制总时长。
        </span>
      </label>
      {error && <div className="form-error">{error}</div>}
      <div className="form-actions">
        <button type="button" className="secondary-button" onClick={onCancel}>
          取消
        </button>
        <button type="submit" className="primary-button" disabled={saving}>
          <Save size={15} />
          {saving ? "保存中…" : "保存配置"}
        </button>
      </div>
    </form>
  );
}
function Field({
  label,
  value,
  onChange,
  placeholder,
  type = "text",
  hint,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  placeholder: string;
  type?: string;
  hint?: string;
}) {
  return (
    <label className="field">
      <span>{label}</span>
      <input
        type={type}
        value={value}
        onChange={(event) => onChange(event.target.value)}
        placeholder={placeholder}
      />
      {hint && <small>{hint}</small>}
    </label>
  );
}

function ConfigurationCenter({
  settings,
  draft,
  dirty,
  saveStatus,
  saveError,
  savedAt,
  onChange,
  onSave,
}: {
  settings: RouterSettings | null;
  draft: ConfigurationInput | null;
  dirty: boolean;
  saveStatus: ConfigurationSaveStatus;
  saveError: string | null;
  savedAt: Date | null;
  onChange: (next: ConfigurationInput) => void;
  onSave: () => void;
}) {
  const [testingGateway, setTestingGateway] = useState(false);
  const [gatewayTest, setGatewayTest] = useState<GatewayTestResult | null>(null);
  const port = draft?.port ?? settings?.port ?? 8765;
  const gatewayUrl = `http://127.0.0.1:${port}/v1`;

  function setPolicyNumber(key: keyof RoutingPolicy, value: number) {
    if (!draft) return;
    onChange({
      ...draft,
      routing_policy: {
        ...draft.routing_policy,
        [key]: Number.isFinite(value) ? value : 0,
      },
    });
  }

  function submit(event: FormEvent) {
    event.preventDefault();
    onSave();
  }

  async function runGatewayTest() {
    setTestingGateway(true);
    setGatewayTest(null);
    try {
      setGatewayTest(await testLocalGateway());
    } catch (error) {
      setGatewayTest({
        ok: false,
        status_code: null,
        message: String(error),
        provider_count: 0,
        enabled_provider_count: 0,
      });
    } finally {
      setTestingGateway(false);
    }
  }

  if (!draft) {
    return <div className="panel configuration-loading">正在读取配置…</div>;
  }

  const policy = draft.routing_policy ?? DEFAULT_ROUTING_POLICY;
  const statusText = saveStatus === "saving"
    ? "正在保存全部配置…"
    : saveStatus === "error"
      ? "保存失败"
      : dirty
        ? "有未保存修改"
        : savedAt
          ? `已保存于 ${savedAt.toLocaleTimeString("zh-CN", { hour12: false })}`
          : "配置已同步";
  const saveDetailText = saveError
    ?? (policy.max_retry_wait_sec === 0 && policy.retry_count > 0
      ? "当前重试不限制累计等待，将仅按配置的重试次数停止。"
      : dirty
        ? "离开配置中心前会提示保存或放弃修改。"
        : "日志与用量保留期限立即生效，健康探测间隔在下一轮生效。");

  return (
    <form className="configuration-layout" onSubmit={submit} noValidate>
      <div className="panel configuration-panel">
        <div className="panel-heading">
          <div>
            <p className="section-kicker">GATEWAY</p>
            <h2>基础网关</h2>
            <p>本地监听、客户端接入和运行自检。</p>
          </div>
        </div>
        <div className="configuration-body">
          <div className="configuration-form-list">
            <ConfigurationFieldRow
              label="监听地址"
              hint="仅允许本机访问，不开放局域网监听。"
              inputId="gateway-bind-address"
            >
              <input id="gateway-bind-address" value="127.0.0.1" disabled />
            </ConfigurationFieldRow>
            <NumberStepper
              label="监听端口"
              hint="允许 1024–65535，默认 8765；保存前会检查端口是否可用。"
              value={draft.port}
              min={1024}
              max={65535}
              onChange={(value) => onChange({ ...draft, port: value })}
            />
          </div>
          <div className="token-block">
            <div>
              <span className="field-label">客户端接入信息</span>
            </div>
            <div className="connection-field">
              <span>Base URL</span>
              <code>{gatewayUrl}</code>
              <button type="button" className="icon-button" title="复制 Base URL" onClick={() => void navigator.clipboard?.writeText(gatewayUrl)}>
                <Copy size={15} />
              </button>
            </div>
            <div className="connection-field">
              <span>API Key</span>
              <code>{settings?.local_token ?? "启动后生成"}</code>
              <button type="button" className="icon-button" title="复制 API Key" disabled={!settings?.local_token} onClick={() => void navigator.clipboard?.writeText(settings?.local_token ?? "")}>
                <Copy size={15} />
              </button>
            </div>
          </div>
          <div className="gateway-test-block">
            <div>
              <strong>本地转发自检</strong>
              <span>使用上方本地令牌请求 /v1/models，验证本地端口、路由和上游。</span>
            </div>
            <button type="button" className="secondary-button" disabled={testingGateway} onClick={() => void runGatewayTest()}>
              <Activity size={15} />
              {testingGateway ? "测试中…" : "测试本地网关"}
            </button>
          </div>
          {gatewayTest && (
            <div className={`gateway-test-result ${gatewayTest.ok ? "success" : "error"}`}>
              <strong>{gatewayTest.ok ? "转发正常" : "转发不可用"}</strong>
              <span>{gatewayTest.message}</span>
              <small>已保存 {gatewayTest.provider_count} 个中转，其中 {gatewayTest.enabled_provider_count} 个可参与路由</small>
            </div>
          )}
        </div>
      </div>

      <div className="configuration-pair">
        <div className="panel configuration-panel">
          <div className="panel-heading">
            <div>
              <p className="section-kicker">HEALTH CHECK</p>
              <h2>健康探测、数据与启动</h2>
              <p>探测间隔、日志和 Token 用量保留期限统一保存。</p>
            </div>
          </div>
          <div className="configuration-body configuration-form-list">
            <NumberStepper label="健康探测间隔（秒）" hint="允许 5–3600 秒；正在等待的旧周期不会被强制中断。" value={draft.probe_interval_sec} min={5} max={3600} onChange={(value) => onChange({ ...draft, probe_interval_sec: value })} />
            <NumberStepper label="运行日志保留天数" hint="允许 1–365 天，默认 7 天；缩短期限后立即清理超期日志。" value={draft.log_retention_days} min={1} max={365} onChange={(value) => onChange({ ...draft, log_retention_days: value })} />
            <NumberStepper label="用量数据保留天数" hint="允许 30–3650 天，默认 365 天；仅保存 Token 数字，不保存请求正文。" value={draft.usage_retention_days} min={30} max={3650} onChange={(value) => onChange({ ...draft, usage_retention_days: value })} />
            <ConfigurationFieldRow label="开机自动启动" hint="登录 Windows 后自动在托盘运行本地网关">
              <button
                type="button"
                className={`toggle ${draft.autostart_enabled ? "checked" : ""}`}
                role="switch"
                aria-checked={draft.autostart_enabled}
                title={draft.autostart_enabled ? "关闭开机启动" : "开启开机启动"}
                onClick={() => onChange({ ...draft, autostart_enabled: !draft.autostart_enabled })}
              >
                <span />
              </button>
            </ConfigurationFieldRow>
          </div>
        </div>

        <div className="panel configuration-panel">
          <div className="panel-heading">
            <div>
              <p className="section-kicker">RETRY</p>
              <h2>请求重试</h2>
              <p>默认只在当前中转重试；只有开启自动切换后才会尝试备用中转。</p>
            </div>
          </div>
          <div className="configuration-body configuration-form-list">
            <NumberStepper label="统一重试次数" hint="0 表示不重试，默认额外重试 5 次；不限制次数，较大的值会延长请求时间并产生更多日志。" value={policy.retry_count} min={0} onChange={(value) => setPolicyNumber("retry_count", value)} />
            <NumberStepper label="重试间隔（秒）" hint="429 和容量错误优先遵循 Retry-After。" value={policy.retry_interval_sec} min={1} max={60} onChange={(value) => setPolicyNumber("retry_interval_sec", value)} />
            <NumberStepper label="最大累计等待（秒）" hint="0 表示不限制累计等待；1–600 秒会在达到预算后提前停止重试。" value={policy.max_retry_wait_sec} min={0} max={600} onChange={(value) => setPolicyNumber("max_retry_wait_sec", value)} />
            <NumberStepper label="Codex 流恢复次数" hint="用于已经开始输出后的 SSE 断流恢复，默认 5 次；修改后需要完全退出并重新启动 Codex。普通请求仍由 AI Gateway 重试。" value={policy.codex_stream_max_retries} min={0} onChange={(value) => setPolicyNumber("codex_stream_max_retries", value)} />
            <ConfigurationFieldRow label="未识别临时错误兜底重试" hint="仅在尚未输出文本、推理或工具调用时生效；是否尝试备用中转仍服从自动切换总开关。">
              <button type="button" className={`toggle ${policy.retry_unknown_transient_errors ? "checked" : ""}`} role="switch" aria-checked={policy.retry_unknown_transient_errors} title={policy.retry_unknown_transient_errors ? "关闭未知错误兜底重试" : "开启未知错误兜底重试"} onClick={() => onChange({ ...draft, routing_policy: { ...policy, retry_unknown_transient_errors: !policy.retry_unknown_transient_errors } })}>
                <span />
              </button>
            </ConfigurationFieldRow>
          </div>
        </div>
      </div>

      <div className="panel configuration-panel">
        <div className="panel-heading">
          <div>
            <p className="section-kicker">CIRCUIT BREAKER</p>
            <h2>熔断、恢复与自动切换</h2>
            <p>新请求立即使用保存后的规则，正在处理的请求保持旧策略。</p>
          </div>
        </div>
        <div className="configuration-body configuration-form-list">
          <NumberStepper label="连续失败熔断阈值" value={policy.circuit_failure_threshold} min={1} max={10} onChange={(value) => setPolicyNumber("circuit_failure_threshold", value)} />
          <NumberStepper label="熔断冷却时间（秒）" value={policy.circuit_cooldown_sec} min={10} max={3600} onChange={(value) => setPolicyNumber("circuit_cooldown_sec", value)} />
          <NumberStepper label="413 相同/更大请求跳过（秒）" hint="更小请求仍会继续尝试该中转。" value={policy.payload_too_large_skip_sec} min={0} max={86400} onChange={(value) => setPolicyNumber("payload_too_large_skip_sec", value)} />
          <ConfigurationFieldRow label="413 处理策略" hint="只有打开“允许自动切换中转”且选择切换策略时，413 才会尝试备用中转。">
            <div className="segmented-control" role="group" aria-label="413 处理策略">
              <button type="button" aria-pressed={policy.payload_too_large_action === "failover"} className={policy.payload_too_large_action === "failover" ? "selected" : ""} onClick={() => onChange({ ...draft, routing_policy: { ...policy, payload_too_large_action: "failover" } })}>立即切换备用中转</button>
              <button type="button" aria-pressed={policy.payload_too_large_action === "return"} className={policy.payload_too_large_action === "return" ? "selected" : ""} onClick={() => onChange({ ...draft, routing_policy: { ...policy, payload_too_large_action: "return" } })}>直接返回 413</button>
            </div>
          </ConfigurationFieldRow>
          <ConfigurationFieldRow label="允许自动切换中转" hint="默认关闭；关闭后熔断或重试耗尽也不会改走备用线路。">
            <button type="button" className={`toggle ${policy.allow_provider_failover ? "checked" : ""}`} role="switch" aria-checked={policy.allow_provider_failover} title={policy.allow_provider_failover ? "关闭自动切换中转" : "开启自动切换中转"} onClick={() => onChange({ ...draft, routing_policy: { ...policy, allow_provider_failover: !policy.allow_provider_failover } })}>
              <span />
            </button>
          </ConfigurationFieldRow>
        </div>
      </div>

      <ErrorHandlingMatrix />

      <div className={`configuration-save-bar ${dirty ? "dirty" : ""} ${saveStatus === "error" ? "error" : ""}`}>
        <div>
          <strong>{statusText}</strong>
          <span>{saveDetailText}</span>
        </div>
        <button className="primary-button" disabled={!dirty || saveStatus === "saving"}>
          <Save size={15} />
          {saveStatus === "saving" ? "保存中…" : "保存全部配置"}
        </button>
      </div>
    </form>
  );
}

function ExportBackupModal({
  onClose,
  onExport,
}: {
  onClose: () => void;
  onExport: (password: string) => Promise<BackupExportResult | null>;
}) {
  const [password, setPassword] = useState("");
  const [confirmation, setConfirmation] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function submit(event: FormEvent) {
    event.preventDefault();
    if (password.length < 8) {
      setError("备份密码至少需要 8 个字符");
      return;
    }
    if (password !== confirmation) {
      setError("两次输入的备份密码不一致");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      await onExport(password);
    } catch (nextError) {
      setError(String(nextError));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div
      className="modal-backdrop backup-modal-backdrop"
      role="presentation"
      onClick={() => !busy && onClose()}
    >
      <form
        className="backup-modal panel"
        role="dialog"
        aria-modal="true"
        aria-labelledby="export-backup-title"
        onClick={(event) => event.stopPropagation()}
        onSubmit={(event) => void submit(event)}
      >
        <div className="backup-modal-head">
          <div className="backup-modal-icon"><FileKey size={20} /></div>
          <div>
            <h2 id="export-backup-title">导出加密配置</h2>
            <p>备份包含中转 API Key、路由规则和本地访问令牌。</p>
          </div>
          <button type="button" className="icon-button" title="关闭" disabled={busy} onClick={onClose}>
            <X size={16} />
          </button>
        </div>
        <div className="backup-security-note">
          <ShieldCheck size={16} />
          <span>文件使用 Argon2id 和 XChaCha20-Poly1305 加密。密码无法找回，请妥善保管。</span>
        </div>
        <label className="field">
          <span>备份密码</span>
          <input
            type="password"
            autoFocus
            autoComplete="new-password"
            value={password}
            disabled={busy}
            placeholder="至少 8 个字符"
            onChange={(event) => {
              setPassword(event.target.value);
              setError(null);
            }}
          />
        </label>
        <label className="field">
          <span>确认密码</span>
          <input
            type="password"
            autoComplete="new-password"
            value={confirmation}
            disabled={busy}
            placeholder="再次输入备份密码"
            onChange={(event) => {
              setConfirmation(event.target.value);
              setError(null);
            }}
          />
        </label>
        {error && <div className="backup-error">{error}</div>}
        <div className="backup-modal-actions">
          <button type="button" className="secondary-button" disabled={busy} onClick={onClose}>取消</button>
          <button type="submit" className="primary-button" disabled={busy}>
            <Download size={15} />
            {busy ? "正在加密…" : "选择位置并导出"}
          </button>
        </div>
      </form>
    </div>
  );
}

function ImportBackupModal({
  path,
  onClose,
  onInspect,
  onImport,
}: {
  path: string;
  onClose: () => void;
  onInspect: (path: string, password: string) => Promise<BackupPreview>;
  onImport: (
    path: string,
    password: string,
    mode: BackupImportMode,
  ) => Promise<BackupImportResult>;
}) {
  const [password, setPassword] = useState("");
  const [preview, setPreview] = useState<BackupPreview | null>(null);
  const [mode, setMode] = useState<BackupImportMode>("replace");
  const [confirmReplace, setConfirmReplace] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function submit(event: FormEvent) {
    event.preventDefault();
    if (password.length < 8) {
      setError("备份密码至少需要 8 个字符");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      if (!preview) {
        setPreview(await onInspect(path, password));
        return;
      }
      if (mode === "replace" && !confirmReplace) {
        setConfirmReplace(true);
        return;
      }
      await onImport(path, password, mode);
    } catch (nextError) {
      setError(String(nextError));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div
      className="modal-backdrop backup-modal-backdrop"
      role="presentation"
      onClick={() => !busy && onClose()}
    >
      <form
        className="backup-modal panel"
        role="dialog"
        aria-modal="true"
        aria-labelledby="import-backup-title"
        onClick={(event) => event.stopPropagation()}
        onSubmit={(event) => void submit(event)}
      >
        <div className="backup-modal-head">
          <div className="backup-modal-icon"><Upload size={20} /></div>
          <div>
            <h2 id="import-backup-title">导入加密配置</h2>
            <p className="backup-file-name" title={path}>{backupFileName(path)}</p>
          </div>
          <button type="button" className="icon-button" title="关闭" disabled={busy} onClick={onClose}>
            <X size={16} />
          </button>
        </div>
        <label className="field">
          <span>备份密码</span>
          <input
            type="password"
            autoFocus
            autoComplete="current-password"
            value={password}
            disabled={busy || Boolean(preview)}
            placeholder="输入导出时设置的密码"
            onChange={(event) => {
              setPassword(event.target.value);
              setPreview(null);
              setError(null);
            }}
          />
        </label>
        {preview && (
          <>
            <div className="backup-preview">
              <div><span>导出时间</span><strong>{new Date(preview.exported_at).toLocaleString("zh-CN")}</strong></div>
              <div><span>应用版本</span><strong>v{preview.app_version}</strong></div>
              <div><span>中转数量</span><strong>{preview.provider_count} 个</strong></div>
              <div><span>冲突中转</span><strong>{preview.conflict_count} 个</strong></div>
              <div><span>本地访问令牌</span><strong>{preview.includes_local_token ? "包含" : "不包含"}</strong></div>
            </div>
            <div className="backup-mode" role="group" aria-label="导入方式">
              <button
                type="button"
                className={mode === "replace" ? "selected" : ""}
                onClick={() => {
                  setMode("replace");
                  setConfirmReplace(false);
                }}
              >
                <strong>完整覆盖</strong>
                <span>恢复全部中转、规则和本地令牌</span>
              </button>
              <button
                type="button"
                className={mode === "merge" ? "selected" : ""}
                onClick={() => {
                  setMode("merge");
                  setConfirmReplace(false);
                }}
              >
                <strong>合并中转</strong>
                <span>保留当前全局设置和本地令牌</span>
              </button>
            </div>
            {mode === "replace" && (
              <div className={`backup-replace-warning ${confirmReplace ? "confirming" : ""}`}>
                <ShieldCheck size={16} />
                <span>{confirmReplace ? "再次点击“确认覆盖并导入”，当前中转将被替换。" : "完整覆盖会替换当前中转，并恢复备份中的本地访问令牌。"}</span>
              </div>
            )}
          </>
        )}
        {error && <div className="backup-error">{error}</div>}
        <div className="backup-modal-actions">
          <button type="button" className="secondary-button" disabled={busy} onClick={onClose}>取消</button>
          <button type="submit" className="primary-button" disabled={busy}>
            <Upload size={15} />
            {busy
              ? preview ? "正在导入…" : "正在检查…"
              : !preview
                ? "解密并检查"
                : mode === "replace" && confirmReplace
                  ? "确认覆盖并导入"
                  : mode === "replace"
                    ? "继续完整覆盖"
                    : "合并中转"}
          </button>
        </div>
      </form>
    </div>
  );
}

function UnsavedChangesModal({
  saving,
  error,
  onSaveAndLeave,
  onDiscard,
  onCancel,
}: {
  saving: boolean;
  error: string | null;
  onSaveAndLeave: () => void;
  onDiscard: () => void;
  onCancel: () => void;
}) {
  return (
    <div className="modal-backdrop" role="presentation" onClick={onCancel}>
      <div className="unsaved-modal panel" role="dialog" aria-modal="true" aria-labelledby="unsaved-title" onClick={(event) => event.stopPropagation()}>
        <div className="unsaved-modal-icon"><Save size={20} /></div>
        <h2 id="unsaved-title">配置尚未保存</h2>
        <p>离开后当前修改不会自动生效。你可以先保存、放弃修改，或继续留在配置中心。</p>
        {error && <div className="unsaved-error">{error}</div>}
        <div className="unsaved-actions">
          <button type="button" className="secondary-button" disabled={saving} onClick={onCancel}>取消</button>
          <button type="button" className="secondary-button danger-text" disabled={saving} onClick={onDiscard}>放弃修改</button>
          <button type="button" className="primary-button" disabled={saving} onClick={onSaveAndLeave}>
            <Save size={15} />{saving ? "保存中…" : "保存并离开"}
          </button>
        </div>
      </div>
    </div>
  );
}

export default App;
