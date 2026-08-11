import { useCallback, useEffect, useId, useState } from "react";
import type { FormEvent, ReactNode } from "react";
import { listen } from "@tauri-apps/api/event";
import { open, save } from "@tauri-apps/plugin-dialog";
import {
  Activity,
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
} from "./api";
import { buildDemoData } from "./demo";

const statusLabels: Record<ProviderHealth, string> = {
  healthy: "运行正常",
  degraded: "性能下降",
  open: "已熔断",
  half_open: "恢复探测",
  unknown: "等待探测",
};

const MAX_HEALTH_PROBES = 24;
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
type AppView = "monitor" | "providers" | "logs" | "settings";
type ConfigurationSaveStatus = "idle" | "saving" | "saved" | "error";

const DEFAULT_ROUTING_POLICY: RoutingPolicy = {
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
};

function configurationInputFromSnapshot(
  snapshot: ConfigurationSnapshot,
): ConfigurationInput {
  return {
    probe_interval_sec: snapshot.router_settings.probe_interval_sec,
    log_retention_days: snapshot.router_settings.log_retention_days,
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
    left.probe_interval_sec === right.probe_interval_sec &&
    left.log_retention_days === right.log_retention_days &&
    left.autostart_enabled === right.autostart_enabled &&
    JSON.stringify(left.routing_policy) === JSON.stringify(right.routing_policy)
  );
}

function validateConfigurationDraft(draft: ConfigurationInput) {
  const checks: Array<[number, number, number, string]> = [
    [draft.probe_interval_sec, 5, 3600, "健康探测间隔"],
    [draft.log_retention_days, 1, 365, "日志保留天数"],
    [draft.routing_policy.retry_count, 0, 3, "瞬时错误重试次数"],
    [draft.routing_policy.retry_interval_sec, 0, 60, "重试间隔"],
    [draft.routing_policy.max_retry_wait_sec, 1, 300, "单次请求最大等待"],
    [draft.routing_policy.capacity_retry_count, 0, 5, "模型容量重试次数"],
    [draft.routing_policy.capacity_retry_interval_sec, 1, 60, "模型容量重试间隔"],
    [draft.routing_policy.capacity_max_retry_wait_sec, 1, 600, "模型容量最大等待"],
    [draft.routing_policy.circuit_failure_threshold, 1, 10, "连续失败阈值"],
    [draft.routing_policy.circuit_cooldown_sec, 10, 3600, "熔断冷却时间"],
    [draft.routing_policy.payload_too_large_skip_sec, 0, 86400, "413 跳过时间"],
  ];
  for (const [value, min, max, label] of checks) {
    if (!Number.isFinite(value) || value < min || value > max) {
      return `${label}必须在 ${min} 到 ${max} 之间`;
    }
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
  const [routingPolicy, setRoutingPolicy] = useState<RoutingPolicy | null>(null);
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
  const [lastUpdatedAt, setLastUpdatedAt] = useState<Date | null>(null);
  const configurationDirty = !configurationInputsEqual(
    configurationDraft,
    configurationSnapshot
      ? configurationInputFromSnapshot(configurationSnapshot)
      : null,
  );

  const refresh = useCallback(async () => {
    try {
      const [nextProviders, nextHealth, nextConfiguration, groups, stats, events] =
        await Promise.all([
          listProviders(),
          getHealthSnapshots(),
          getConfiguration(),
          listRouteGroups(),
          getRuntimeStats(),
          getRecentLogs(),
        ]);
      setProviders(nextProviders);
      setHealth(nextHealth);
      setSettings(nextConfiguration.router_settings);
      setRoutingPolicy(nextConfiguration.routing_policy);
      setConfigurationSnapshot(nextConfiguration);
      setRouteGroup(groups[0] ?? null);
      setRuntime(stats);
      setLogs(events);
      setLastUpdatedAt(new Date());
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
        });
        setRouteGroup(demo.routeGroup);
        setRuntime(demo.runtime);
        setLogs(demo.logs);
        setLastUpdatedAt(new Date());
      }
    }
  }, []);

  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => void refresh(), STATUS_REFRESH_MS);
    return () => window.clearInterval(timer);
  }, [refresh]);

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
    // 路由事件由 Rust 网关统一产生；容量重试只刷新日志，只有真实切换才显示 Toast。
    void listen<RoutingEvent>("routing-event", (event) => {
      const routingEvent = event.payload;
      const isAutoSwitch = routingEvent.event_type === "auto_switch";
      const eventMessage = routingEvent.event_type === "capacity_recovered"
        ? "模型容量恢复，同一中转重试成功"
        : routingEvent.event_type === "capacity_exhausted"
          ? "模型容量重试已耗尽"
          : "主力中转不可用，已切换到备用中转";
      setLogs((current) => current.some((item) => item.routing_event?.id === routingEvent.id)
        ? current
        : [{
            timestamp: routingEvent.timestamp,
            level: routingEvent.event_type === "capacity_recovered"
              ? "info" as const
              : routingEvent.event_type === "capacity_exhausted"
                ? "error" as const
                : "warning" as const,
            message: eventMessage,
            provider_id: routingEvent.target_provider_id,
            model: routingEvent.model,
            event_type: routingEvent.event_type,
            routing_event: routingEvent,
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
        ? `${provider.name} 连接成功，延迟 ${result.latency_ms ?? "-"} ms`
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
    setNotice(`已导出 ${result.provider_count} 个中转到 ${result.file_name}`);
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
      const next = await saveConfiguration(configurationDraft);
      setConfigurationSnapshot(next);
      setConfigurationDraft(configurationInputFromSnapshot(next));
      setSettings(next.router_settings);
      setRoutingPolicy(next.routing_policy);
      setConfigurationSaveStatus("saved");
      setConfigurationSavedAt(new Date());
      setNotice("配置中心已保存，日志保留期限已立即生效");
      return true;
    } catch (error) {
      if (import.meta.env.DEV && location.search.includes("demo=1") && settings) {
        const next: ConfigurationSnapshot = {
          router_settings: {
            ...settings,
            probe_interval_sec: configurationDraft.probe_interval_sec,
            log_retention_days: configurationDraft.log_retention_days,
          },
          routing_policy: { ...configurationDraft.routing_policy },
          autostart_enabled: configurationDraft.autostart_enabled,
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
            className={`nav-item ${view === "providers" ? "active" : ""}`}
            onClick={() => requestView("providers")}
          >
            <Server size={17} />
            中转管理
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
          <span className="version">v0.2.4</span>
        </div>
      </aside>
      <main className="main-content">
        <header className="topbar">
          <div>
            <p className="eyebrow">LOCAL ROUTER</p>
            <h1>
              {view === "monitor"
                ? "运行监控"
                : view === "providers"
                  ? "中转管理"
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
              onClick={() => void refresh()}
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
            lastUpdatedAt={lastUpdatedAt}
            onOpenRoutingEvent={setRoutingDetail}
            onTest={(provider) => void handleTest(provider)}
            onLock={(provider) => void handleLock(provider)}
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
                    <p>按优先级尝试，异常时自动进入下一个可用中转</p>
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
        ) : view === "logs" ? (
          <LogsPanel logs={logs} providers={providers} runtime={runtime} onOpenRoutingEvent={setRoutingDetail} />
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

function MonitorPanel({
  providers,
  health,
  settings,
  routeGroup,
  runtime,
  logs,
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
  lastUpdatedAt: Date | null;
  onOpenRoutingEvent: (event: RoutingEvent) => void;
  onTest: (provider: Provider) => void;
  onLock: (provider: Provider) => void;
}) {
  const enabledProviders = providers.filter((provider) => provider.enabled);
  const snapshots = new Map(
    health.map((snapshot) => [snapshot.provider_id, snapshot]),
  );
  const healthyProviders = enabledProviders.filter(
    (provider) => snapshots.get(provider.id)?.state === "healthy",
  );
  const usableProviders = enabledProviders.filter(
    (provider) => snapshots.get(provider.id)?.state !== "open",
  );
  const lockedProvider = providers.find(
    (provider) => provider.id === routeGroup?.locked_provider_id,
  );
  // 监控页按网关的实际候选规则展示当前路由：固定中转优先，否则选择首个未熔断的启用中转。
  const activeProvider =
    (lockedProvider?.enabled &&
    snapshots.get(lockedProvider.id)?.state !== "open"
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
    overallDescription = `${healthyProviders.length}/${enabledProviders.length} 个中转健康，自动故障转移已就绪`;
    overallTone = "healthy";
  } else if (healthyProviders.length || usableProviders.length) {
    overallTitle = "路由可用，部分中转状态异常";
    overallDescription = `${usableProviders.length}/${enabledProviders.length} 个中转仍可参与路由，异常中转将被自动跳过`;
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
                  <div className={`monitor-state ${state}`}>
                    <span />
                    {provider.enabled ? statusLabels[state] : "已停用"}
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
                    <div className="monitor-row-error">{snapshot.last_error}</div>
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
  onEdit,
  onTest,
  onLock,
}: {
  provider: Provider;
  health?: HealthSnapshot;
  locked: boolean;
  onEdit: () => void;
  onTest: () => void;
  onLock: () => void;
}) {
  const token = "http://127.0.0.1:8765/v1";
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
          value={provider.has_api_key ? "已安全保存" : "未配置"}
          icon={<KeyRound size={15} />}
        />
        <DetailLine label="响应头超时" value={`${provider.timeout_ms} ms`} />
      </div>
      <div className="detail-section">
        <h3>本地接入</h3>
        <div className="copy-field">
          <span>{token}</span>
          <button
            className="icon-button"
            title="复制地址"
            onClick={() => void navigator.clipboard?.writeText(token)}
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
}: {
  logs: EventLog[];
  providers: Provider[];
  runtime: RuntimeStats | null;
  onOpenRoutingEvent: (event: RoutingEvent) => void;
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

  useEffect(() => {
    setPage((current) => Math.min(current, totalPages));
  }, [totalPages]);

  return (
    <section className="logs-layout">
      <div className="panel log-panel">
        <div className="panel-heading">
          <div>
            <h2>最近事件</h2>
            <p>记录模型、状态、延迟和错误类型，不保存请求正文</p>
          </div>
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
        {logs.length ? (
          <>
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
                  className={`log-row ${entry.routing_event ? "clickable" : ""}`}
                  key={`${entry.timestamp}-${index}`}
                  onClick={() => entry.routing_event && onOpenRoutingEvent(entry.routing_event)}
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
            <div className="log-pagination">
              <span>
                共 {logs.length} 条，第 {page} / {totalPages} 页
              </span>
              <div>
                <button
                  className="icon-button"
                  title="上一页"
                  disabled={page === 1}
                  onClick={() => setPage((current) => Math.max(1, current - 1))}
                >
                  <ChevronLeft size={15} />
                </button>
                <button
                  className="icon-button"
                  title="下一页"
                  disabled={page === totalPages}
                  onClick={() =>
                    setPage((current) => Math.min(totalPages, current + 1))
                  }
                >
                  <ChevronRight size={15} />
                </button>
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
          ["模型容量不足", "仅在当前中转按独立策略重试", "是", "否"],
          ["连接失败 / 超时", "按策略等待后重试", "是", "重试失败后"],
          ["408", "按策略等待后重试", "是", "重试失败后"],
          ["429", "遵循 Retry-After，过长则切换", "是", "等待过长后"],
          ["413", "记录请求体阈值并立即切换", "否", "是"],
          ["5xx", "按策略等待后重试", "是", "重试失败后"],
          ["400 / 401 / 403 / 404", "原样返回请求错误", "否", "否"],
        ].map(([kind, handling, retry, switchTo]) => (
          <div className="matrix-row" key={kind}>
            <strong>{kind}</strong><span>{handling}</span><span>{retry}</span><span>{switchTo}</span>
          </div>
        ))}
      </div>
      <div className="matrix-explanation">
        <CircleHelp size={17} />
        <div>
          <strong>为什么 413 不重复等待？</strong>
          <p>413 是请求体超过上游限制。相同请求在一分钟后仍然一样大，重复发送只会延迟 Codex；网关会记录被拒绝的大小，后续相同或更大的请求在跳过期内直接尝试下一条线路。</p>
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

function NumberStepper({ label, hint, value, min, max, onChange }: { label: string; hint?: string; value: number; min: number; max: number; onChange: (value: number) => void }) {
  const inputId = useId();
  const [draft, setDraft] = useState(String(value));

  useEffect(() => {
    setDraft(String(value));
  }, [value]);

  function clamp(next: number) {
    return Math.min(max, Math.max(min, Math.round(next)));
  }

  function commitDraft() {
    // 编辑多位数字时允许输入框暂时为空；只有失焦或提交时才恢复旧值并限制到合法范围。
    if (!draft.trim()) {
      setDraft(String(value));
      return;
    }
    const parsed = Number(draft);
    if (!Number.isFinite(parsed)) {
      setDraft(String(value));
      return;
    }
    const next = clamp(parsed);
    setDraft(String(next));
    if (next !== value) onChange(next);
  }

  function stepBy(delta: number) {
    const parsed = Number(draft);
    const base = Number.isFinite(parsed) && draft.trim() ? parsed : value;
    const next = clamp(base + delta);
    setDraft(String(next));
    if (next !== value) onChange(next);
  }

  const numericDraft = Number(draft);
  const current = Number.isFinite(numericDraft) && draft.trim() ? numericDraft : value;

  return (
    <div className="field number-field">
      <label className="field-label-text" htmlFor={inputId}>{label}</label>
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
              if (next && parsed >= min && parsed <= max && parsed !== value) {
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
        <button type="button" title={`增加${label}`} aria-label={`增加${label}`} disabled={current >= max} onClick={() => stepBy(1)}>
          <Plus size={14} />
        </button>
      </div>
      {hint && <small>{hint}</small>}
    </div>
  );
}

function RoutingDetailModal({ event, onClose }: { event: RoutingEvent; onClose: () => void }) {
  const isCapacityEvent = event.event_type === "capacity_recovered" || event.event_type === "capacity_exhausted";
  const resultLabel = event.final_result === "switched"
    ? "已切换并成功"
    : event.final_result === "capacity_recovered"
      ? "同一中转重试成功"
      : event.final_result === "capacity_exhausted"
        ? "容量重试已耗尽"
        : event.final_result;
  return <div className="modal-backdrop" role="presentation" onClick={onClose}><div className="routing-modal panel" role="dialog" aria-modal="true" onClick={(click) => click.stopPropagation()}>
    <div className="panel-heading"><div><p className="section-kicker">ROUTING DETAIL</p><h2>{isCapacityEvent ? "模型容量重试详情" : "自动切换处理详情"}</h2><p>{new Date(event.timestamp).toLocaleString("zh-CN")}</p></div><button className="icon-button" title="关闭详情" onClick={onClose}><X size={17} /></button></div>
    <div className="routing-detail-body">
      <div className="routing-detail-grid"><DetailLine label="模型" value={event.model ?? "未提供"} /><DetailLine label="接口路径" value={event.request_path} /><DetailLine label="请求体大小" value={`${event.request_body_bytes.toLocaleString()} 字节`} /><DetailLine label="重试次数" value={String(event.retry_count)} /><DetailLine label="最终状态" value={event.status_code ? String(event.status_code) : "未收到响应"} /><DetailLine label="最终结果" value={resultLabel} /></div>
      <div className="routing-reason"><strong>处理原因</strong><span>{event.reason}</span></div>
      <h3>{isCapacityEvent ? "同一中转重试过程" : "中转尝试过程"}</h3>
      <div className="attempt-list">{event.attempts.map((attempt, index) => <div className="attempt-row" key={`${attempt.provider_id}-${index}`}><span className="attempt-index">{index + 1}</span><div><strong>{attempt.provider_name}</strong><span>{attempt.error ?? `响应 ${attempt.status_code ?? "成功"}`} · {attempt.latency_ms ?? "--"} ms</span></div><b>{attempt.switched ? "已切换" : attempt.retry_count ? `重试 ${attempt.retry_count}` : "已尝试"}</b></div>)}</div>
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
  const port = settings?.port ?? 8765;
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

  return (
    <form className="configuration-layout" onSubmit={submit} noValidate>
      <div className="panel configuration-panel">
        <div className="panel-heading">
          <div>
            <p className="section-kicker">GATEWAY</p>
            <h2>基础网关</h2>
            <p>本地监听地址、cc-switch 接入信息和运行自检。</p>
          </div>
        </div>
        <div className="configuration-body">
          <div className="configuration-fields two-columns">
            <label className="field">
              <span>监听地址</span>
              <input value="127.0.0.1" disabled />
            </label>
            <label className="field">
              <span>监听端口</span>
              <input value={String(port)} disabled />
            </label>
          </div>
          <div className="token-block">
            <div>
              <span className="field-label">cc-switch 接入信息</span>
              <p>两项都需要填写。此令牌只用于访问本机网关，不是真实中转的 API Key。</p>
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
              <h2>健康探测、日志与启动</h2>
              <p>健康探测间隔和日志保留期限统一保存。</p>
            </div>
          </div>
          <div className="configuration-body">
          <NumberStepper label="健康探测间隔（秒）" hint="允许 5–3600 秒；正在等待的旧周期不会被强制中断。" value={draft.probe_interval_sec} min={5} max={3600} onChange={(value) => onChange({ ...draft, probe_interval_sec: value })} />
          <NumberStepper label="运行日志保留天数" hint="允许 1–365 天，默认 7 天；缩短期限后立即清理超期日志。" value={draft.log_retention_days} min={1} max={365} onChange={(value) => onChange({ ...draft, log_retention_days: value })} />
          <div className="option-row">
            <div>
              <strong>开机自动启动</strong>
              <span>登录 Windows 后自动在托盘运行本地网关</span>
            </div>
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
          </div>
          </div>
        </div>

        <div className="panel configuration-panel">
          <div className="panel-heading">
            <div>
              <p className="section-kicker">RETRY</p>
              <h2>请求重试</h2>
              <p>仅对连接失败、408、429 和 5xx 等瞬时错误生效。</p>
            </div>
          </div>
          <div className="configuration-body rules-fields">
            <NumberStepper label="瞬时错误重试次数" hint="每个中转最多一次初始请求加 0–3 次重试。" value={policy.retry_count} min={0} max={3} onChange={(value) => setPolicyNumber("retry_count", value)} />
            <NumberStepper label="重试间隔（秒）" hint="429 优先遵循 Retry-After。" value={policy.retry_interval_sec} min={0} max={60} onChange={(value) => setPolicyNumber("retry_interval_sec", value)} />
            <NumberStepper label="单次请求最大重试等待（秒）" hint="超过该值直接尝试备用中转。" value={policy.max_retry_wait_sec} min={1} max={300} onChange={(value) => setPolicyNumber("max_retry_wait_sec", value)} />
            <NumberStepper label="模型容量重试次数" hint="只重试原中转，初始请求外最多再试 0–5 次。" value={policy.capacity_retry_count} min={0} max={5} onChange={(value) => setPolicyNumber("capacity_retry_count", value)} />
            <NumberStepper label="模型容量重试间隔（秒）" hint="存在 Retry-After 时优先遵循上游要求。" value={policy.capacity_retry_interval_sec} min={1} max={60} onChange={(value) => setPolicyNumber("capacity_retry_interval_sec", value)} />
            <NumberStepper label="模型容量最大等待（秒）" hint="耗尽后保留上游最后一次容量错误，不切换中转。" value={policy.capacity_max_retry_wait_sec} min={1} max={600} onChange={(value) => setPolicyNumber("capacity_max_retry_wait_sec", value)} />
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
        <div className="configuration-body">
          <div className="configuration-fields three-columns">
            <NumberStepper label="连续失败熔断阈值" value={policy.circuit_failure_threshold} min={1} max={10} onChange={(value) => setPolicyNumber("circuit_failure_threshold", value)} />
            <NumberStepper label="熔断冷却时间（秒）" value={policy.circuit_cooldown_sec} min={10} max={3600} onChange={(value) => setPolicyNumber("circuit_cooldown_sec", value)} />
            <NumberStepper label="413 相同/更大请求跳过（秒）" hint="更小请求仍会继续尝试该中转。" value={policy.payload_too_large_skip_sec} min={0} max={86400} onChange={(value) => setPolicyNumber("payload_too_large_skip_sec", value)} />
          </div>
          <div className="field segmented-field">
            <span className="field-label-text">413 处理策略</span>
            <div className="segmented-control" role="group" aria-label="413 处理策略">
              <button type="button" aria-pressed={policy.payload_too_large_action === "failover"} className={policy.payload_too_large_action === "failover" ? "selected" : ""} onClick={() => onChange({ ...draft, routing_policy: { ...policy, payload_too_large_action: "failover" } })}>立即切换备用中转</button>
              <button type="button" aria-pressed={policy.payload_too_large_action === "return"} className={policy.payload_too_large_action === "return" ? "selected" : ""} onClick={() => onChange({ ...draft, routing_policy: { ...policy, payload_too_large_action: "return" } })}>直接返回 413</button>
            </div>
            <small>请求体大小不会随等待改变，因此默认立即尝试备用中转。</small>
          </div>
          <div className="option-row">
            <div>
              <strong>自动切换提示</strong>
              <span>应用内 Toast 和托盘 tooltip 会显示切换结果，运行日志始终保留。</span>
            </div>
            <button type="button" className={`toggle ${policy.notify_switches ? "checked" : ""}`} role="switch" aria-checked={policy.notify_switches} title={policy.notify_switches ? "关闭自动切换提示" : "开启自动切换提示"} onClick={() => onChange({ ...draft, routing_policy: { ...policy, notify_switches: !policy.notify_switches } })}>
              <span />
            </button>
          </div>
        </div>
      </div>

      <ErrorHandlingMatrix />

      <div className={`configuration-save-bar ${dirty ? "dirty" : ""} ${saveStatus === "error" ? "error" : ""}`}>
        <div>
          <strong>{statusText}</strong>
          <span>{saveError ?? (dirty ? "离开配置中心前会提示保存或放弃修改。" : "日志期限立即生效，健康探测间隔在下一轮生效。")}</span>
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
