mod backup;
mod codex_integration;
mod codex_usage;
mod usage_estimation;

use axum::{
    body::{to_bytes, Body},
    extract::State,
    http::{header, HeaderMap, HeaderName, Method, Request, StatusCode},
    response::Response,
    routing::{any, get},
    Router,
};
use bytes::Bytes;
use chrono::{
    DateTime, Datelike, Duration, Local, NaiveDate, SecondsFormat, TimeZone, Timelike, Utc,
};
use futures_util::{Stream, StreamExt};
use keyring::{credential::CredentialPersistence, default, Entry};
use reqwest::Client;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, VecDeque},
    error::Error as StdError,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
    task::{Context, Poll},
    time::{Duration as StdDuration, Instant},
};
use tauri::{
    menu::{Menu, MenuItem},
    tray::TrayIconBuilder,
    Emitter, Manager, State as TauriState,
};
use tauri_plugin_autostart::ManagerExt as AutostartManagerExt;
use tokio::{
    sync::oneshot,
    time::{sleep, timeout},
};
use tracing::{error, info, warn};
use url::Url;
use uuid::Uuid;

const DEFAULT_BIND: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 8765;
const MIN_CONFIGURABLE_PORT: u16 = 1024;
const KEYRING_SERVICE: &str = "ai-gateway-provider";
const MAX_EVENT_LOGS: usize = 2_000;
const DEFAULT_LOG_RETENTION_DAYS: u16 = 7;
const MAX_LOG_RETENTION_DAYS: u16 = 365;
const DEFAULT_USAGE_RETENTION_DAYS: u16 = 365;
const MIN_USAGE_RETENTION_DAYS: u16 = 30;
const MAX_USAGE_RETENTION_DAYS: u16 = 3_650;
// usage 解析与 tokenizer 输出观察共用 8 MiB 上限；超限只停止统计，响应仍原样透传。
const MAX_USAGE_CAPTURE_BYTES: usize = usage_estimation::MAX_ESTIMATION_TEXT_BYTES;
const MAX_LOCAL_REQUEST_BODY_BYTES: usize = 200 * 1024 * 1024;
const MAX_HEALTH_PROBES: usize = 24;
const MAX_ERROR_BODY_BYTES: usize = 1024 * 1024;
const MAX_SSE_EVENT_BYTES: usize = 256 * 1024;
const MAX_SSE_PREFLIGHT_BYTES: usize = 1024 * 1024;
// 配置经由 JavaScript 前端传输；不设置业务上限，但必须保持整数可无损往返。
const MAX_SAFE_CONFIG_INTEGER: u64 = 9_007_199_254_740_991;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderConfig {
    pub id: String,
    pub name: String,
    pub base_url: String,
    pub models: Vec<String>,
    pub priority: i64,
    pub enabled: bool,
    pub timeout_ms: u64,
    #[serde(skip)]
    pub api_key: String,
    #[serde(skip)]
    pub api_key_error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProviderDto {
    pub id: String,
    pub name: String,
    pub base_url: String,
    pub models: Vec<String>,
    pub priority: i64,
    pub enabled: bool,
    pub timeout_ms: u64,
    pub has_api_key: bool,
    pub api_key_error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthState {
    Healthy,
    Degraded,
    Open,
    HalfOpen,
    Unknown,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthIssueKind {
    #[default]
    None,
    Transport,
    Upstream,
    Model,
    RateLimit,
    Stream,
    Payload,
    Configuration,
}

#[derive(Clone, Debug, Serialize)]
pub struct HealthProbeSample {
    pub sequence: u64,
    pub timestamp: DateTime<Utc>,
    pub state: HealthState,
    pub latency_ms: Option<u64>,
    pub error: Option<String>,
}

#[derive(Clone, Serialize)]
pub struct HealthSnapshot {
    pub provider_id: String,
    pub state: HealthState,
    pub latency_ms: Option<u64>,
    pub consecutive_failures: u32,
    pub last_error: Option<String>,
    pub last_issue_kind: HealthIssueKind,
    pub cooldown_until: Option<DateTime<Utc>>,
    pub total_requests: u64,
    pub successful_requests: u64,
    pub payload_rejected_size: Option<usize>,
    pub payload_rejected_until: Option<DateTime<Utc>>,
    pub recent_probes: Vec<HealthProbeSample>,
}

#[derive(Clone, Serialize)]
pub struct RouterSettings {
    pub bind_address: String,
    pub port: u16,
    pub local_token: String,
    pub probe_interval_sec: u64,
    pub log_retention_days: u16,
    pub usage_retention_days: u16,
    pub gateway_running: bool,
    pub gateway_listening: bool,
    pub gateway_error: Option<String>,
}

#[derive(Clone, Serialize)]
pub struct ConfigurationSnapshot {
    pub router_settings: RouterSettings,
    pub routing_policy: RoutingPolicy,
    pub autostart_enabled: bool,
}

#[derive(Clone, Deserialize)]
pub struct ConfigurationInput {
    pub port: u16,
    pub probe_interval_sec: u64,
    pub log_retention_days: u16,
    pub usage_retention_days: u16,
    pub routing_policy: RoutingPolicy,
    pub autostart_enabled: bool,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageQueryMode {
    Today,
    Range,
    Month,
}

#[derive(Clone, Deserialize)]
pub struct UsageQuery {
    pub mode: UsageQueryMode,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub month: Option<String>,
    #[serde(default)]
    pub source: UsageSource,
}

#[derive(Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UsageSource {
    #[default]
    All,
    Official,
    Gateway,
    Unconfirmed,
}

#[derive(Clone, Serialize, Default)]
pub struct TokenMetric {
    pub value: u64,
    pub estimated_value: u64,
    pub reported_count: u64,
}

#[derive(Clone, Serialize, Default)]
pub struct UsageMetrics {
    pub input_tokens: TokenMetric,
    pub output_tokens: TokenMetric,
    pub cache_hit_tokens: TokenMetric,
    pub total_tokens: TokenMetric,
}

#[derive(Clone, Serialize)]
pub struct UsagePeriod {
    pub mode: String,
    pub start_date: String,
    pub end_date: String,
    pub label: String,
    pub resolution: String,
}

#[derive(Clone, Serialize)]
pub struct ProviderUsageStatistics {
    pub provider_id: String,
    pub provider_name: String,
    pub deleted: bool,
    pub completed_requests: u64,
    pub reported_requests: u64,
    pub coverage_percent: f64,
    pub data_quality: String,
    pub metrics: UsageMetrics,
}

#[derive(Clone, Serialize)]
pub struct UsageSeriesPoint {
    pub key: String,
    pub label: String,
    pub completed_requests: u64,
    pub reported_requests: u64,
    pub metrics: UsageMetrics,
}

#[derive(Clone, Serialize)]
pub struct UsageStatistics {
    pub period: UsagePeriod,
    pub totals: UsageMetrics,
    pub providers: Vec<ProviderUsageStatistics>,
    pub series: Vec<UsageSeriesPoint>,
    pub completed_requests: u64,
    pub reported_requests: u64,
    pub coverage_percent: f64,
    pub data_quality: String,
    pub codex_sync_status: codex_usage::CodexUsageSyncStatus,
}

#[derive(Clone, Default, Deserialize, Serialize)]
struct CapturedUsage {
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    cache_read_tokens: Option<u64>,
    cache_write_tokens: Option<u64>,
    reasoning_tokens: Option<u64>,
    total_tokens: Option<u64>,
}

impl CapturedUsage {
    fn is_reported(&self) -> bool {
        self.input_tokens.is_some() || self.output_tokens.is_some() || self.total_tokens.is_some()
    }
}

struct UsageRecord {
    completed_at: DateTime<Utc>,
    provider_id: String,
    provider_name: String,
    model: Option<String>,
    request_path: String,
    usage: CapturedUsage,
    data_source: &'static str,
    usage_origin: &'static str,
    client_origin: &'static str,
    codex_event_key: Option<String>,
    gateway_request_id: Option<String>,
    attempt_index: u64,
    is_final_attempt: bool,
    http_status: Option<u16>,
    outcome: &'static str,
    token_source: &'static str,
    estimate_method: Option<String>,
}

#[derive(Clone, Serialize)]
pub struct GatewayTestResult {
    pub ok: bool,
    pub status_code: Option<u16>,
    pub message: String,
    pub provider_count: usize,
    pub enabled_provider_count: usize,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadTooLargeAction {
    Failover,
    Return,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RoutingPolicy {
    pub retry_count: u64,
    pub retry_interval_sec: u64,
    pub max_retry_wait_sec: u64,
    pub retry_unknown_transient_errors: bool,
    pub allow_provider_failover: bool,
    // 旧版备份仍包含容量专用字段；保存时与统一重试配置同步，避免破坏格式版本 1。
    pub capacity_retry_count: u64,
    pub capacity_retry_interval_sec: u64,
    pub capacity_max_retry_wait_sec: u64,
    pub circuit_failure_threshold: u8,
    pub circuit_cooldown_sec: u64,
    pub payload_too_large_skip_sec: u64,
    pub payload_too_large_action: PayloadTooLargeAction,
    pub notify_switches: bool,
}

impl Default for RoutingPolicy {
    fn default() -> Self {
        Self {
            retry_count: 5,
            retry_interval_sec: 1,
            max_retry_wait_sec: 30,
            retry_unknown_transient_errors: true,
            allow_provider_failover: false,
            capacity_retry_count: 5,
            capacity_retry_interval_sec: 1,
            capacity_max_retry_wait_sec: 30,
            circuit_failure_threshold: 3,
            circuit_cooldown_sec: 60,
            payload_too_large_skip_sec: 600,
            payload_too_large_action: PayloadTooLargeAction::Failover,
            notify_switches: true,
        }
    }
}

#[derive(Clone, Serialize)]
pub struct RouteGroupDto {
    pub id: String,
    pub name: String,
    pub provider_ids: Vec<String>,
    pub locked_provider_id: Option<String>,
}

#[derive(Clone, Serialize)]
pub struct RuntimeStats {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub average_latency_ms: Option<u64>,
    pub active_provider_id: Option<String>,
}

#[derive(Clone, Serialize)]
pub struct EventLog {
    pub timestamp: DateTime<Utc>,
    pub level: String,
    pub message: String,
    pub provider_id: Option<String>,
    pub model: Option<String>,
    pub event_type: Option<String>,
    pub routing_event: Option<RoutingEvent>,
    pub diagnostic_detail: Option<DiagnosticDetail>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DiagnosticDetail {
    pub request_id: String,
    pub provider_id: String,
    pub model: Option<String>,
    pub request_path: String,
    pub error_class: String,
    pub http_status: Option<u16>,
    pub error_code: Option<String>,
    pub error_type: Option<String>,
    pub error_message: Option<String>,
    pub incomplete_reason: Option<String>,
    pub upstream_request_id: Option<String>,
    pub last_sse_event: Option<String>,
    pub terminal_event: Option<String>,
    pub terminal_received: bool,
    pub business_output_started: bool,
    pub bytes_received: u64,
    pub elapsed_ms: u64,
    pub attempt_index: u64,
    pub max_attempts: u64,
    pub retry_wait_sec: Option<u64>,
    pub fallback_retry_enabled: bool,
    pub failover_enabled: bool,
    pub action: String,
    pub parse_error: Option<String>,
    // 每条诊断保留当时已经发生的脱敏路由尝试，详情弹窗无需查询请求正文即可还原过程。
    pub attempts: Vec<RoutingAttempt>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RoutingAttempt {
    pub provider_id: String,
    pub provider_name: String,
    pub status_code: Option<u16>,
    pub error: Option<String>,
    pub latency_ms: Option<u64>,
    pub retry_count: u64,
    pub switched: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RoutingEvent {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub event_type: String,
    pub model: Option<String>,
    pub request_path: String,
    pub request_body_bytes: usize,
    pub source_provider_id: Option<String>,
    pub target_provider_id: Option<String>,
    pub status_code: Option<u16>,
    pub retry_count: u64,
    pub attempts: Vec<RoutingAttempt>,
    pub reason: String,
    pub final_result: String,
}

#[derive(Clone)]
struct HealthRecord {
    state: HealthState,
    latency_ms: Option<u64>,
    consecutive_failures: u32,
    last_error: Option<String>,
    last_issue_kind: HealthIssueKind,
    cooldown_until: Option<DateTime<Utc>>,
    total_requests: u64,
    successful_requests: u64,
    payload_rejected_size: Option<usize>,
    payload_rejected_until: Option<DateTime<Utc>>,
    probe_sequence: u64,
    recent_probes: VecDeque<HealthProbeSample>,
}

impl Default for HealthRecord {
    fn default() -> Self {
        Self {
            state: HealthState::Unknown,
            latency_ms: None,
            consecutive_failures: 0,
            last_error: None,
            last_issue_kind: HealthIssueKind::None,
            cooldown_until: None,
            total_requests: 0,
            successful_requests: 0,
            payload_rejected_size: None,
            payload_rejected_until: None,
            probe_sequence: 0,
            recent_probes: VecDeque::with_capacity(MAX_HEALTH_PROBES),
        }
    }
}

struct Store {
    connection: Mutex<Connection>,
}

trait ProviderSecretStore: Send + Sync {
    fn get(&self, id: &str) -> Result<Option<String>, String>;
    fn set(&self, id: &str, value: &str) -> Result<(), String>;
    fn delete(&self, id: &str) -> Result<(), String>;
}

struct SystemProviderSecretStore;

fn validate_system_secret_store() -> Result<(), String> {
    // keyring 没有默认平台 feature；若错误构建退回 Mock，它只会保存到 Entry 生命周期内。
    // 启动前强制检查持久性，防止再次出现“保存成功但重启丢失”的静默数据损坏。
    if matches!(
        default::default_credential_builder().persistence(),
        CredentialPersistence::UntilDelete
    ) {
        Ok(())
    } else {
        Err(
            "Windows 凭据后端不可用：当前程序未连接到可持久化的 Windows Credential Manager"
                .to_string(),
        )
    }
}

impl ProviderSecretStore for SystemProviderSecretStore {
    fn get(&self, id: &str) -> Result<Option<String>, String> {
        let entry = Entry::new(KEYRING_SERVICE, id).map_err(|error| error.to_string())?;
        match entry.get_password() {
            Ok(value) => Ok(Some(value)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(error) => Err(error.to_string()),
        }
    }

    fn set(&self, id: &str, value: &str) -> Result<(), String> {
        Entry::new(KEYRING_SERVICE, id)
            .map_err(|error| error.to_string())?
            .set_password(value)
            .map_err(|error| error.to_string())
    }

    fn delete(&self, id: &str) -> Result<(), String> {
        let entry = Entry::new(KEYRING_SERVICE, id).map_err(|error| error.to_string())?;
        match entry.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(error) => Err(error.to_string()),
        }
    }
}

fn save_and_verify_provider_secret(
    secrets: &dyn ProviderSecretStore,
    id: &str,
    value: &str,
) -> Result<(), String> {
    secrets.set(id, value)?;
    // 写入后立即回读，避免界面显示“已配置”但凭据项实际上无法用于重启或导出。
    match secrets.get(id)? {
        Some(saved) if saved == value => Ok(()),
        Some(_) => Err("系统密钥库回读结果与刚保存的 API Key 不一致".to_string()),
        None => Err("系统密钥库写入后未找到对应的 API Key".to_string()),
    }
}

fn restore_provider_secret(
    secrets: &dyn ProviderSecretStore,
    id: &str,
    previous: Option<&str>,
) -> Result<(), String> {
    match previous {
        Some(value) => save_and_verify_provider_secret(secrets, id, value),
        None => secrets.delete(id),
    }
}

#[derive(Default)]
struct GatewayRuntime {
    generation: u64,
    port: Option<u16>,
    shutdown: Option<oneshot::Sender<()>>,
}

#[derive(Default)]
struct GatewayController {
    runtime: Mutex<GatewayRuntime>,
}

#[cfg(test)]
#[derive(Default)]
struct MemoryProviderSecretStore {
    values: Mutex<HashMap<String, String>>,
}

#[cfg(test)]
impl ProviderSecretStore for MemoryProviderSecretStore {
    fn get(&self, id: &str) -> Result<Option<String>, String> {
        Ok(self.values.lock().unwrap().get(id).cloned())
    }

    fn set(&self, id: &str, value: &str) -> Result<(), String> {
        self.values
            .lock()
            .unwrap()
            .insert(id.to_string(), value.to_string());
        Ok(())
    }

    fn delete(&self, id: &str) -> Result<(), String> {
        self.values.lock().unwrap().remove(id);
        Ok(())
    }
}

#[derive(Clone)]
struct AppState {
    config: Arc<RwLock<Vec<ProviderConfig>>>,
    health: Arc<Mutex<HashMap<String, HealthRecord>>>,
    settings: Arc<RwLock<RouterSettings>>,
    store: Arc<Store>,
    client: Client,
    running: Arc<AtomicBool>,
    locked_provider_id: Arc<RwLock<Option<String>>>,
    logs: Arc<Mutex<VecDeque<EventLog>>>,
    policy: Arc<RwLock<RoutingPolicy>>,
    secrets: Arc<dyn ProviderSecretStore>,
    gateway: Arc<GatewayController>,
    // 仅跟踪 config.toml 的内容指纹，用于识别 CC-Switch 或用户的外部修改。
    codex_config_fingerprint: Arc<Mutex<Option<String>>>,
    // 迁移预览只在当前进程内有效；执行时还会重新扫描磁盘并核对完整指纹。
    codex_history_preview: Arc<Mutex<Option<codex_integration::HistoryPreviewSnapshot>>>,
    // 桌面端、CLI 和本地登录凭据只在启动或用户手动刷新时检测，避免轮询反复查询系统进程。
    codex_environment_status: Arc<Mutex<codex_integration::CodexEnvironmentStatus>>,
    // 自动同步、手动同步和重建必须串行，避免重建清游标时另一轮正在回填。
    codex_usage_sync_lock: Arc<tokio::sync::Mutex<()>>,
    codex_usage_status: Arc<Mutex<codex_usage::CodexUsageSyncStatus>>,
    // 单元测试不启动 Tauri runtime，因此事件发送句柄允许为空。
    app_handle: Option<tauri::AppHandle>,
}

#[derive(Deserialize)]
struct ProviderInput {
    id: Option<String>,
    name: String,
    base_url: String,
    models: Vec<String>,
    priority: Option<i64>,
    enabled: Option<bool>,
    timeout_ms: Option<u64>,
    api_key: Option<String>,
}

#[derive(Deserialize)]
struct RouteGroupInput {
    provider_ids: Vec<String>,
    locked_provider_id: Option<String>,
}

impl AppState {
    fn new(app: &tauri::AppHandle) -> Result<Self, String> {
        validate_system_secret_store()?;
        let data_dir = app.path().app_data_dir().map_err(|e| e.to_string())?;
        std::fs::create_dir_all(&data_dir).map_err(|e| e.to_string())?;
        let db_path = data_dir.join("gateway.sqlite3");
        let connection = Connection::open(db_path).map_err(|e| e.to_string())?;
        // SQLite 保存公开配置和脱敏运行日志；API Key 始终由系统密钥库单独管理。
        connection
            .execute_batch(
                "PRAGMA journal_mode = WAL;
                PRAGMA synchronous = NORMAL;
                PRAGMA busy_timeout = 3000;
                CREATE TABLE IF NOT EXISTS providers (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    base_url TEXT NOT NULL,
                    models_json TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    enabled INTEGER NOT NULL,
                    timeout_ms INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS event_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL,
                    provider_id TEXT,
                    model TEXT,
                    event_type TEXT,
                    routing_event_json TEXT,
                    diagnostic_json TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_event_logs_timestamp ON event_logs(timestamp);
                CREATE TABLE IF NOT EXISTS usage_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    completed_at TEXT NOT NULL,
                    provider_id TEXT NOT NULL,
                    provider_name TEXT NOT NULL,
                    model TEXT,
                    request_path TEXT NOT NULL,
                    input_tokens INTEGER,
                    output_tokens INTEGER,
                    cached_tokens INTEGER,
                    cache_write_tokens INTEGER,
                    reasoning_tokens INTEGER,
                    total_tokens INTEGER,
                    usage_reported INTEGER NOT NULL,
                    data_source TEXT NOT NULL DEFAULT 'gateway_response',
                    usage_origin TEXT NOT NULL DEFAULT 'gateway',
                    client_origin TEXT NOT NULL DEFAULT 'unknown',
                    codex_event_key TEXT,
                    codex_usage_supplemented INTEGER NOT NULL DEFAULT 0,
                    gateway_request_id TEXT,
                    attempt_index INTEGER NOT NULL DEFAULT 1,
                    is_final_attempt INTEGER NOT NULL DEFAULT 1,
                    http_status INTEGER,
                    outcome TEXT NOT NULL DEFAULT 'completed',
                    token_source TEXT NOT NULL DEFAULT 'none',
                    estimate_method TEXT,
                    codex_previous_usage_json TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_usage_records_completed_at
                    ON usage_records(completed_at);
                CREATE INDEX IF NOT EXISTS idx_usage_records_provider_time
                    ON usage_records(provider_id, completed_at);
                CREATE TABLE IF NOT EXISTS codex_usage_sync_state (
                    relative_path TEXT PRIMARY KEY,
                    file_identity TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    modified_at_ms INTEGER NOT NULL,
                    line_offset INTEGER NOT NULL,
                    byte_offset INTEGER NOT NULL,
                    last_total_json TEXT,
                    session_id TEXT,
                    model_provider TEXT,
                    current_model TEXT,
                    parent_session_id TEXT,
                    last_synced_at TEXT NOT NULL,
                    last_warning TEXT
                );
                CREATE TABLE IF NOT EXISTS codex_mode_history (
                    changed_at TEXT PRIMARY KEY,
                    mode TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS codex_usage_events (
                    event_key TEXT PRIMARY KEY,
                    event_time TEXT NOT NULL,
                    model TEXT NOT NULL,
                    input_tokens INTEGER,
                    output_tokens INTEGER,
                    cached_tokens INTEGER,
                    cache_write_tokens INTEGER,
                    reasoning_tokens INTEGER,
                    total_tokens INTEGER,
                    model_provider TEXT,
                    observed_mode TEXT,
                    has_rate_limits INTEGER NOT NULL DEFAULT 0,
                    classification TEXT NOT NULL,
                    counted INTEGER NOT NULL DEFAULT 0,
                    usage_record_id INTEGER,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_codex_usage_events_time
                    ON codex_usage_events(event_time);
                CREATE INDEX IF NOT EXISTS idx_codex_usage_events_usage_record
                    ON codex_usage_events(usage_record_id)
                    WHERE usage_record_id IS NOT NULL;
                CREATE TABLE IF NOT EXISTS codex_usage_event_sources (
                    event_key TEXT NOT NULL,
                    source_key TEXT NOT NULL,
                    file_identity TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    PRIMARY KEY(event_key, source_key)
                );",
            )
            .map_err(|e| e.to_string())?;
        migrate_usage_schema(&connection)?;
        migrate_event_log_schema(&connection)?;
        let store = Arc::new(Store {
            connection: Mutex::new(connection),
        });
        migrate_unified_retry_policy(&store)?;
        let secrets: Arc<dyn ProviderSecretStore> = Arc::new(SystemProviderSecretStore);
        let token =
            setting(&store, "local_token")?.unwrap_or_else(|| Uuid::new_v4().simple().to_string());
        save_setting(&store, "local_token", &token)?;
        let probe_interval_sec = setting(&store, "probe_interval_sec")?
            .and_then(|v| v.parse().ok())
            .unwrap_or(30);
        let port = load_port(&store)?;
        let log_retention_days = setting(&store, "log_retention_days")?
            .and_then(|value| value.parse().ok())
            .filter(|days| (1..=MAX_LOG_RETENTION_DAYS).contains(days))
            .unwrap_or(DEFAULT_LOG_RETENTION_DAYS);
        let usage_retention_days = setting(&store, "usage_retention_days")?
            .and_then(|value| value.parse().ok())
            .filter(|days| (MIN_USAGE_RETENTION_DAYS..=MAX_USAGE_RETENTION_DAYS).contains(days))
            .unwrap_or(DEFAULT_USAGE_RETENTION_DAYS);
        // 启动时先清理过期记录，避免页面短暂看到已超过本机保留策略的数据。
        prune_expired_logs(&store, log_retention_days)?;
        prune_expired_usage(&store, usage_retention_days)?;
        let logs = load_recent_logs(&store)?;
        let policy = load_routing_policy(&store)?;
        let providers = load_providers(&store, secrets.as_ref())?;
        let locked_provider_id =
            setting(&store, "locked_provider_id")?.filter(|value| !value.is_empty());
        let settings = RouterSettings {
            bind_address: DEFAULT_BIND.to_string(),
            port,
            local_token: token,
            probe_interval_sec,
            log_retention_days,
            usage_retention_days,
            gateway_running: true,
            gateway_listening: false,
            gateway_error: None,
        };
        if setting(&store, "codex_mode_history_initialized")?.is_none() {
            // 升级后的第一条模式基线来自当前 config.toml；无法检测时保持为空，避免猜错旧历史来源。
            let configuration = gateway_codex_configuration(&settings);
            let mode = codex_integration::integration_state(
                &configuration,
                codex_integration::CodexEnvironmentStatus {
                    desktop_status: codex_integration::CodexApplicationStatus::NotInstalled,
                    cli_status: codex_integration::CodexApplicationStatus::NotInstalled,
                    login_status: codex_integration::CodexLoginStatus::Unknown,
                },
            )
            .current_mode;
            match mode {
                codex_integration::CodexConnectionMode::Official => {
                    codex_usage::record_mode(&store, "official")?
                }
                codex_integration::CodexConnectionMode::Gateway => {
                    codex_usage::record_mode(&store, "gateway")?
                }
                codex_integration::CodexConnectionMode::External => {}
            }
            save_setting(&store, "codex_mode_history_initialized", "1")?;
        }
        let codex_usage_status = codex_usage::stored_status(&store);
        Ok(Self {
            config: Arc::new(RwLock::new(providers)),
            health: Arc::new(Mutex::new(HashMap::new())),
            settings: Arc::new(RwLock::new(settings)),
            store,
            client: Client::builder().build().map_err(|e| e.to_string())?,
            running: Arc::new(AtomicBool::new(true)),
            locked_provider_id: Arc::new(RwLock::new(locked_provider_id)),
            logs: Arc::new(Mutex::new(logs)),
            policy: Arc::new(RwLock::new(policy)),
            secrets,
            gateway: Arc::new(GatewayController::default()),
            codex_config_fingerprint: Arc::new(Mutex::new(
                codex_integration::config_fingerprint().ok(),
            )),
            codex_history_preview: Arc::new(Mutex::new(None)),
            codex_environment_status: Arc::new(Mutex::new(codex_integration::environment_status())),
            codex_usage_sync_lock: Arc::new(tokio::sync::Mutex::new(())),
            codex_usage_status: Arc::new(Mutex::new(codex_usage_status)),
            app_handle: Some(app.clone()),
        })
    }
}

fn usage_column_exists(connection: &Connection, column: &str) -> Result<bool, String> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('usage_records') WHERE name = ?1)",
            [column],
            |row| row.get(0),
        )
        .map_err(|error| format!("无法检查用量数据库结构：{error}"))
}

fn migrate_usage_schema(connection: &Connection) -> Result<(), String> {
    // SQLite 的 ADD COLUMN 是非破坏性的；逐列检测保证 0.2.x/0.3.0 数据库可直接升级。
    for (column, definition) in [
        ("cache_write_tokens", "INTEGER"),
        ("data_source", "TEXT NOT NULL DEFAULT 'gateway_response'"),
        ("usage_origin", "TEXT NOT NULL DEFAULT 'gateway'"),
        ("client_origin", "TEXT NOT NULL DEFAULT 'unknown'"),
        ("codex_event_key", "TEXT"),
        ("codex_usage_supplemented", "INTEGER NOT NULL DEFAULT 0"),
        ("gateway_request_id", "TEXT"),
        ("attempt_index", "INTEGER NOT NULL DEFAULT 1"),
        ("is_final_attempt", "INTEGER NOT NULL DEFAULT 1"),
        ("http_status", "INTEGER"),
        ("outcome", "TEXT NOT NULL DEFAULT 'completed'"),
        ("token_source", "TEXT NOT NULL DEFAULT 'none'"),
        ("estimate_method", "TEXT"),
        ("codex_previous_usage_json", "TEXT"),
    ] {
        if !usage_column_exists(connection, column)? {
            connection
                .execute(
                    &format!("ALTER TABLE usage_records ADD COLUMN {column} {definition}"),
                    [],
                )
                .map_err(|error| format!("无法升级用量数据库字段 {column}：{error}"))?;
        }
    }
    connection
        .execute_batch(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_usage_records_codex_event_key
                 ON usage_records(codex_event_key) WHERE codex_event_key IS NOT NULL;
             CREATE INDEX IF NOT EXISTS idx_usage_records_origin_time
                 ON usage_records(usage_origin, completed_at);
             CREATE UNIQUE INDEX IF NOT EXISTS idx_usage_records_gateway_attempt
                 ON usage_records(gateway_request_id, provider_id, attempt_index)
                 WHERE gateway_request_id IS NOT NULL;
             CREATE TABLE IF NOT EXISTS codex_usage_events (
                 event_key TEXT PRIMARY KEY,
                 event_time TEXT NOT NULL,
                 model TEXT NOT NULL,
                 input_tokens INTEGER,
                 output_tokens INTEGER,
                 cached_tokens INTEGER,
                 cache_write_tokens INTEGER,
                 reasoning_tokens INTEGER,
                 total_tokens INTEGER,
                 model_provider TEXT,
                 observed_mode TEXT,
                 has_rate_limits INTEGER NOT NULL DEFAULT 0,
                 classification TEXT NOT NULL,
                 counted INTEGER NOT NULL DEFAULT 0,
                 usage_record_id INTEGER,
                 created_at TEXT NOT NULL,
                 updated_at TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_codex_usage_events_time
                 ON codex_usage_events(event_time);
             CREATE INDEX IF NOT EXISTS idx_codex_usage_events_usage_record
                 ON codex_usage_events(usage_record_id)
                 WHERE usage_record_id IS NOT NULL;
             CREATE TABLE IF NOT EXISTS codex_usage_event_sources (
                 event_key TEXT NOT NULL,
                 source_key TEXT NOT NULL,
                 file_identity TEXT NOT NULL,
                 observed_at TEXT NOT NULL,
                 PRIMARY KEY(event_key, source_key)
             );",
        )
        .map_err(|error| format!("无法创建用量去重索引：{error}"))?;
    // 旧版本没有记录 Token 来源；按现有来源和上报状态进行一次可重复的兼容回填。
    connection
        .execute_batch(
            "UPDATE usage_records
             SET token_source = CASE
                 WHEN data_source='codex_session' THEN 'codex_session'
                 WHEN usage_reported=1 THEN 'upstream'
                 ELSE 'none'
             END
             WHERE token_source='none' AND (data_source='codex_session' OR usage_reported=1);",
        )
        .map_err(|error| format!("无法回填用量来源：{error}"))
}

fn migrate_event_log_schema(connection: &Connection) -> Result<(), String> {
    let diagnostic_exists = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('event_logs') WHERE name = 'diagnostic_json')",
            [],
            |row| row.get::<_, bool>(0),
        )
        .map_err(|error| format!("无法检查运行日志数据库结构：{error}"))?;
    if !diagnostic_exists {
        // 诊断详情使用可空列，旧日志无需回填，升级后仍可按原格式读取。
        connection
            .execute("ALTER TABLE event_logs ADD COLUMN diagnostic_json TEXT", [])
            .map_err(|error| format!("无法升级运行日志诊断字段：{error}"))?;
    }
    Ok(())
}

fn load_port(store: &Store) -> Result<u16, String> {
    Ok(setting(store, "port")?
        .and_then(|value| value.parse::<u16>().ok())
        .filter(|value| *value >= MIN_CONFIGURABLE_PORT)
        .unwrap_or(DEFAULT_PORT))
}

fn setting(store: &Store, key: &str) -> Result<Option<String>, String> {
    let conn = store
        .connection
        .lock()
        .map_err(|_| "数据库锁已损坏".to_string())?;
    conn.query_row(
        "SELECT value FROM settings WHERE key = ?1",
        params![key],
        |row| row.get(0),
    )
    .optional()
    .map_err(|e| e.to_string())
}
fn save_setting(store: &Store, key: &str, value: &str) -> Result<(), String> {
    let conn = store
        .connection
        .lock()
        .map_err(|_| "数据库锁已损坏".to_string())?;
    conn.execute("INSERT INTO settings(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value", params![key, value]).map_err(|e| e.to_string())?;
    Ok(())
}

fn migrate_unified_retry_policy(store: &Store) -> Result<(), String> {
    if setting(store, "unified_retry_policy_v1")?.is_some() {
        return Ok(());
    }
    let defaults = RoutingPolicy::default();
    let mut conn = store
        .connection
        .lock()
        .map_err(|_| "数据库锁已损坏".to_string())?;
    let transaction = conn.transaction().map_err(|error| error.to_string())?;
    // 0.3.2 只迁移新定义的统一重试字段，保留用户已有的熔断、413 和通知偏好。
    for (key, value) in [
        ("retry_count", defaults.retry_count.to_string()),
        (
            "retry_interval_sec",
            defaults.retry_interval_sec.to_string(),
        ),
        (
            "max_retry_wait_sec",
            defaults.max_retry_wait_sec.to_string(),
        ),
        (
            "allow_provider_failover",
            defaults.allow_provider_failover.to_string(),
        ),
        (
            "capacity_retry_count",
            defaults.capacity_retry_count.to_string(),
        ),
        (
            "capacity_retry_interval_sec",
            defaults.capacity_retry_interval_sec.to_string(),
        ),
        (
            "capacity_max_retry_wait_sec",
            defaults.capacity_max_retry_wait_sec.to_string(),
        ),
        ("unified_retry_policy_v1", "1".to_string()),
    ] {
        transaction
            .execute(
                "INSERT INTO settings(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                params![key, value],
            )
            .map_err(|error| error.to_string())?;
    }
    transaction.commit().map_err(|error| error.to_string())
}

fn load_routing_policy(store: &Store) -> Result<RoutingPolicy, String> {
    let defaults = RoutingPolicy::default();
    let value = |key: &str| setting(store, key);
    let action = match value("payload_too_large_action")?.as_deref() {
        Some("return") => PayloadTooLargeAction::Return,
        _ => PayloadTooLargeAction::Failover,
    };
    let mut policy = RoutingPolicy {
        retry_count: value("retry_count")?
            .and_then(|item| item.parse().ok())
            .unwrap_or(defaults.retry_count),
        retry_interval_sec: value("retry_interval_sec")?
            .and_then(|item| item.parse().ok())
            .unwrap_or(defaults.retry_interval_sec),
        max_retry_wait_sec: value("max_retry_wait_sec")?
            .and_then(|item| item.parse().ok())
            .unwrap_or(defaults.max_retry_wait_sec),
        retry_unknown_transient_errors: value("retry_unknown_transient_errors")?
            .and_then(|item| item.parse().ok())
            .unwrap_or(defaults.retry_unknown_transient_errors),
        allow_provider_failover: value("allow_provider_failover")?
            .and_then(|item| item.parse().ok())
            .unwrap_or(defaults.allow_provider_failover),
        capacity_retry_count: value("capacity_retry_count")?
            .and_then(|item| item.parse().ok())
            .unwrap_or(defaults.capacity_retry_count),
        capacity_retry_interval_sec: value("capacity_retry_interval_sec")?
            .and_then(|item| item.parse().ok())
            .unwrap_or(defaults.capacity_retry_interval_sec),
        capacity_max_retry_wait_sec: value("capacity_max_retry_wait_sec")?
            .and_then(|item| item.parse().ok())
            .unwrap_or(defaults.capacity_max_retry_wait_sec),
        circuit_failure_threshold: value("circuit_failure_threshold")?
            .and_then(|item| item.parse().ok())
            .unwrap_or(defaults.circuit_failure_threshold),
        circuit_cooldown_sec: value("circuit_cooldown_sec")?
            .and_then(|item| item.parse().ok())
            .unwrap_or(defaults.circuit_cooldown_sec),
        payload_too_large_skip_sec: value("payload_too_large_skip_sec")?
            .and_then(|item| item.parse().ok())
            .unwrap_or(defaults.payload_too_large_skip_sec),
        payload_too_large_action: action,
        notify_switches: value("notify_switches")?
            .and_then(|item| item.parse().ok())
            .unwrap_or(defaults.notify_switches),
    };
    synchronize_legacy_retry_fields(&mut policy);
    validate_routing_policy(&policy)?;
    Ok(policy)
}

fn synchronize_legacy_retry_fields(policy: &mut RoutingPolicy) {
    // 容量和账号并发错误已归入统一重试策略，旧字段仅作为备份兼容镜像。
    policy.capacity_retry_count = policy.retry_count;
    policy.capacity_retry_interval_sec = policy.retry_interval_sec;
    policy.capacity_max_retry_wait_sec = policy.max_retry_wait_sec;
}

fn normalized_routing_policy(policy: &RoutingPolicy) -> RoutingPolicy {
    let mut normalized = policy.clone();
    synchronize_legacy_retry_fields(&mut normalized);
    normalized
}

fn validate_routing_policy(policy: &RoutingPolicy) -> Result<(), String> {
    if policy.retry_count > MAX_SAFE_CONFIG_INTEGER {
        return Err("重试次数超出可安全保存的整数范围".to_string());
    }
    if !(1..=60).contains(&policy.retry_interval_sec) {
        return Err("重试间隔必须在 1 到 60 秒之间".to_string());
    }
    if policy.max_retry_wait_sec > 600 {
        return Err("单次请求最大重试等待必须在 0 到 600 秒之间".to_string());
    }
    if policy.capacity_retry_count > MAX_SAFE_CONFIG_INTEGER {
        return Err("模型容量重试次数超出可安全保存的整数范围".to_string());
    }
    if !(1..=60).contains(&policy.capacity_retry_interval_sec) {
        return Err("模型容量重试间隔必须在 1 到 60 秒之间".to_string());
    }
    if policy.capacity_max_retry_wait_sec > 600 {
        return Err("模型容量最大等待必须在 0 到 600 秒之间".to_string());
    }
    if !(1..=10).contains(&policy.circuit_failure_threshold) {
        return Err("连续失败阈值必须在 1 到 10 之间".to_string());
    }
    if !(10..=3600).contains(&policy.circuit_cooldown_sec) {
        return Err("熔断冷却时间必须在 10 到 3600 秒之间".to_string());
    }
    if policy.payload_too_large_skip_sec > 86_400 {
        return Err("413 跳过时间必须在 0 到 86400 秒之间".to_string());
    }
    Ok(())
}

fn validate_probe_interval(probe_interval_sec: u64) -> Result<(), String> {
    if !(5..=3600).contains(&probe_interval_sec) {
        return Err("健康探测间隔必须在 5 到 3600 秒之间".to_string());
    }
    Ok(())
}

fn validate_log_retention_days(log_retention_days: u16) -> Result<(), String> {
    if !(1..=MAX_LOG_RETENTION_DAYS).contains(&log_retention_days) {
        return Err(format!(
            "日志保留天数必须在 1 到 {MAX_LOG_RETENTION_DAYS} 天之间"
        ));
    }
    Ok(())
}

fn validate_usage_retention_days(usage_retention_days: u16) -> Result<(), String> {
    if !(MIN_USAGE_RETENTION_DAYS..=MAX_USAGE_RETENTION_DAYS).contains(&usage_retention_days) {
        return Err(format!(
            "用量数据保留天数必须在 {MIN_USAGE_RETENTION_DAYS} 到 {MAX_USAGE_RETENTION_DAYS} 天之间"
        ));
    }
    Ok(())
}

fn validate_port(port: u16) -> Result<(), String> {
    if port < MIN_CONFIGURABLE_PORT {
        return Err(format!(
            "监听端口必须在 {MIN_CONFIGURABLE_PORT} 到 {} 之间",
            u16::MAX
        ));
    }
    Ok(())
}

fn routing_policy_values(policy: &RoutingPolicy) -> Vec<(&'static str, String)> {
    vec![
        ("retry_count", policy.retry_count.to_string()),
        ("retry_interval_sec", policy.retry_interval_sec.to_string()),
        ("max_retry_wait_sec", policy.max_retry_wait_sec.to_string()),
        (
            "retry_unknown_transient_errors",
            policy.retry_unknown_transient_errors.to_string(),
        ),
        (
            "allow_provider_failover",
            policy.allow_provider_failover.to_string(),
        ),
        (
            "capacity_retry_count",
            policy.capacity_retry_count.to_string(),
        ),
        (
            "capacity_retry_interval_sec",
            policy.capacity_retry_interval_sec.to_string(),
        ),
        (
            "capacity_max_retry_wait_sec",
            policy.capacity_max_retry_wait_sec.to_string(),
        ),
        (
            "circuit_failure_threshold",
            policy.circuit_failure_threshold.to_string(),
        ),
        (
            "circuit_cooldown_sec",
            policy.circuit_cooldown_sec.to_string(),
        ),
        (
            "payload_too_large_skip_sec",
            policy.payload_too_large_skip_sec.to_string(),
        ),
        (
            "payload_too_large_action",
            match policy.payload_too_large_action {
                PayloadTooLargeAction::Failover => "failover".to_string(),
                PayloadTooLargeAction::Return => "return".to_string(),
            },
        ),
        ("notify_switches", policy.notify_switches.to_string()),
    ]
}

fn persist_routing_policy(store: &Store, policy: &RoutingPolicy) -> Result<(), String> {
    let policy = normalized_routing_policy(policy);
    let mut conn = store
        .connection
        .lock()
        .map_err(|_| "数据库锁已损坏".to_string())?;
    let transaction = conn.transaction().map_err(|error| error.to_string())?;
    // 所有规则在同一事务中更新，避免新请求读取到一半是旧值、一半是新值的策略。
    for (key, value) in routing_policy_values(&policy) {
        transaction
            .execute(
                "INSERT INTO settings(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                params![key, value],
            )
            .map_err(|error| error.to_string())?;
    }
    transaction.commit().map_err(|error| error.to_string())
}

fn persist_configuration(
    store: &Store,
    port: u16,
    probe_interval_sec: u64,
    log_retention_days: u16,
    usage_retention_days: u16,
    policy: &RoutingPolicy,
) -> Result<(), String> {
    let policy = normalized_routing_policy(policy);
    let mut conn = store
        .connection
        .lock()
        .map_err(|_| "数据库锁已损坏".to_string())?;
    let transaction = conn.transaction().map_err(|error| error.to_string())?;
    let mut values = vec![
        ("port", port.to_string()),
        ("probe_interval_sec", probe_interval_sec.to_string()),
        ("log_retention_days", log_retention_days.to_string()),
        ("usage_retention_days", usage_retention_days.to_string()),
    ];
    values.extend(routing_policy_values(&policy));

    // 监听端口、探测间隔和路由规则属于同一份运行配置，必须一起提交，避免保存成功一半。
    for (key, value) in values {
        transaction
            .execute(
                "INSERT INTO settings(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                params![key, value],
            )
            .map_err(|error| error.to_string())?;
    }
    transaction.commit().map_err(|error| error.to_string())
}
fn load_providers(
    store: &Store,
    secrets: &dyn ProviderSecretStore,
) -> Result<Vec<ProviderConfig>, String> {
    let conn = store
        .connection
        .lock()
        .map_err(|_| "数据库锁已损坏".to_string())?;
    let mut stmt = conn.prepare("SELECT id,name,base_url,models_json,priority,enabled,timeout_ms FROM providers ORDER BY priority ASC, created_at ASC").map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([], |row| {
            let models_json: String = row.get(3)?;
            Ok(ProviderConfig {
                id: row.get(0)?,
                name: row.get(1)?,
                base_url: row.get(2)?,
                models: serde_json::from_str(&models_json).unwrap_or_default(),
                priority: row.get(4)?,
                enabled: row.get::<_, i64>(5)? != 0,
                timeout_ms: row.get::<_, i64>(6)? as u64,
                api_key: String::new(),
                api_key_error: None,
            })
        })
        .map_err(|e| e.to_string())?;
    let mut result = Vec::new();
    for item in rows {
        let mut provider = item.map_err(|e| e.to_string())?;
        // 凭据不存在是可修复的配置状态，系统读取失败则保留诊断信息；两者不能都伪装成空 Key。
        match secrets.get(&provider.id) {
            Ok(Some(value)) if !value.is_empty() => provider.api_key = value,
            Ok(_) => {}
            Err(error) => {
                provider.api_key_error = Some(format!("Windows 凭据读取失败：{error}"));
            }
        }
        result.push(provider);
    }
    Ok(result)
}
fn dto(provider: &ProviderConfig) -> ProviderDto {
    ProviderDto {
        id: provider.id.clone(),
        name: provider.name.clone(),
        base_url: provider.base_url.clone(),
        models: provider.models.clone(),
        priority: provider.priority,
        enabled: provider.enabled,
        timeout_ms: provider.timeout_ms,
        has_api_key: !provider.api_key.is_empty(),
        api_key_error: provider.api_key_error.clone(),
    }
}

fn health_snapshot(id: &str, record: &HealthRecord) -> HealthSnapshot {
    HealthSnapshot {
        provider_id: id.to_string(),
        state: record.state.clone(),
        latency_ms: record.latency_ms,
        consecutive_failures: record.consecutive_failures,
        last_error: record.last_error.clone(),
        last_issue_kind: record.last_issue_kind,
        cooldown_until: record.cooldown_until,
        total_requests: record.total_requests,
        successful_requests: record.successful_requests,
        payload_rejected_size: record.payload_rejected_size,
        payload_rejected_until: record.payload_rejected_until,
        recent_probes: record.recent_probes.iter().cloned().collect(),
    }
}

fn log_timestamp(timestamp: DateTime<Utc>) -> String {
    timestamp.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn persist_event_log(store: &Store, entry: &EventLog) -> Result<(), String> {
    let routing_event_json = entry
        .routing_event
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| format!("无法序列化路由事件：{error}"))?;
    let diagnostic_json = entry
        .diagnostic_detail
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| format!("无法序列化诊断详情：{error}"))?;
    let conn = store
        .connection
        .lock()
        .map_err(|_| "数据库锁已损坏".to_string())?;
    conn.execute(
        "INSERT INTO event_logs(timestamp,level,message,provider_id,model,event_type,routing_event_json,diagnostic_json)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
        params![
            log_timestamp(entry.timestamp),
            entry.level,
            entry.message,
            entry.provider_id,
            entry.model,
            entry.event_type,
            routing_event_json,
            diagnostic_json
        ],
    )
    .map_err(|error| format!("无法写入运行日志：{error}"))?;
    Ok(())
}

fn load_recent_logs(store: &Store) -> Result<VecDeque<EventLog>, String> {
    let conn = store
        .connection
        .lock()
        .map_err(|_| "数据库锁已损坏".to_string())?;
    let mut statement = conn
        .prepare(
            "SELECT timestamp,level,message,provider_id,model,event_type,routing_event_json,diagnostic_json
             FROM event_logs ORDER BY id DESC LIMIT ?1",
        )
        .map_err(|error| error.to_string())?;
    let rows = statement
        .query_map(params![MAX_EVENT_LOGS as i64], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
            ))
        })
        .map_err(|error| error.to_string())?;
    let mut newest_first = Vec::new();
    for row in rows {
        let (
            timestamp,
            level,
            message,
            provider_id,
            model,
            event_type,
            routing_event_json,
            diagnostic_json,
        ) = row.map_err(|error| error.to_string())?;
        let timestamp = DateTime::parse_from_rfc3339(&timestamp)
            .map_err(|error| format!("运行日志时间格式损坏：{error}"))?
            .with_timezone(&Utc);
        // 单条旧版详情损坏时仍保留基础日志，避免一行 JSON 阻止应用启动。
        let routing_event = routing_event_json
            .as_deref()
            .and_then(|json| serde_json::from_str::<RoutingEvent>(json).ok());
        let diagnostic_detail = diagnostic_json
            .as_deref()
            .and_then(|json| serde_json::from_str::<DiagnosticDetail>(json).ok());
        newest_first.push(EventLog {
            timestamp,
            level,
            message,
            provider_id,
            model,
            event_type,
            routing_event,
            diagnostic_detail,
        });
    }
    newest_first.reverse();
    Ok(VecDeque::from(newest_first))
}

fn prune_expired_logs(store: &Store, retention_days: u16) -> Result<usize, String> {
    let cutoff = Utc::now() - Duration::days(i64::from(retention_days));
    let conn = store
        .connection
        .lock()
        .map_err(|_| "数据库锁已损坏".to_string())?;
    conn.execute(
        "DELETE FROM event_logs WHERE timestamp < ?1",
        params![log_timestamp(cutoff)],
    )
    .map_err(|error| format!("无法清理过期日志：{error}"))
}

fn sqlite_token(value: Option<u64>) -> Result<Option<i64>, String> {
    value
        .map(|token| {
            i64::try_from(token).map_err(|_| "上游返回的 Token 数量超出可保存范围".to_string())
        })
        .transpose()
}

fn sqlite_attempt_index(value: u64) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| "上游尝试序号超出可保存范围".to_string())
}

fn persist_usage_record(store: &Store, record: &UsageRecord) -> Result<bool, String> {
    let conn = store
        .connection
        .lock()
        .map_err(|_| "数据库锁已损坏".to_string())?;
    conn.execute(
        "INSERT INTO usage_records(
            completed_at,provider_id,provider_name,model,request_path,
            input_tokens,output_tokens,cached_tokens,cache_write_tokens,reasoning_tokens,
            total_tokens,usage_reported,data_source,usage_origin,client_origin,codex_event_key,
            gateway_request_id,attempt_index,is_final_attempt,http_status,outcome,token_source,
            estimate_method
         ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,
                  ?17,?18,?19,?20,?21,?22,?23)
         ON CONFLICT(gateway_request_id,provider_id,attempt_index) WHERE gateway_request_id IS NOT NULL
         DO NOTHING",
        params![
            log_timestamp(record.completed_at),
            record.provider_id,
            record.provider_name,
            record.model,
            record.request_path,
            sqlite_token(record.usage.input_tokens)?,
            sqlite_token(record.usage.output_tokens)?,
            sqlite_token(record.usage.cache_read_tokens)?,
            sqlite_token(record.usage.cache_write_tokens)?,
            sqlite_token(record.usage.reasoning_tokens)?,
            sqlite_token(record.usage.total_tokens)?,
            i64::from(record.usage.is_reported()),
            record.data_source,
            record.usage_origin,
            record.client_origin,
            record.codex_event_key,
            record.gateway_request_id,
            sqlite_attempt_index(record.attempt_index)?,
            i64::from(record.is_final_attempt),
            record.http_status.map(i64::from),
            record.outcome,
            record.token_source,
            record.estimate_method,
        ],
    )
    .map(|inserted| inserted > 0)
    .map_err(|error| format!("无法保存 Token 用量：{error}"))
}

fn emit_usage_updated(state: &AppState, record: &UsageRecord) {
    if let Some(app) = state.app_handle.as_ref() {
        let _ = app.emit(
            "usage-updated",
            serde_json::json!({
                "provider_id": record.provider_id,
                "completed_at": record.completed_at,
            }),
        );
    }
}

fn persist_gateway_attempt(state: &AppState, record: UsageRecord) {
    // 统计失败不能改变代理结果；唯一索引保证流终态、Drop 和异常分支不会重复结算同一尝试。
    match persist_usage_record(&state.store, &record) {
        // 幂等冲突表示该尝试已经结算，不能再次触发页面刷新或同步连锁反应。
        Ok(true) => emit_usage_updated(state, &record),
        Ok(false) => {}
        Err(error) => warn!(%error, "usage persistence failed"),
    }
}

fn collect_prepared_usage(
    response: &PreparedUpstreamResponse,
    request_estimate: Option<usage_estimation::RequestTokenEstimate>,
    model: Option<String>,
    allow_estimate: bool,
) -> CollectedUsage {
    let mut collector =
        UsageCollector::new(response.usage_response_kind(), request_estimate, model);
    for chunk in &response.prefix {
        collector.observe(chunk);
    }
    collector.finish(allow_estimate)
}

#[allow(clippy::too_many_arguments)]
fn gateway_usage_record(
    request_id: &str,
    attempt_index: u64,
    provider_id: &str,
    provider_name: &str,
    model: Option<String>,
    request_path: &str,
    status: Option<StatusCode>,
    outcome: &'static str,
    is_final_attempt: bool,
    client_origin: &'static str,
    collected: CollectedUsage,
) -> UsageRecord {
    UsageRecord {
        completed_at: Utc::now(),
        provider_id: provider_id.to_string(),
        provider_name: provider_name.to_string(),
        model,
        request_path: request_path.to_string(),
        usage: collected.usage,
        data_source: "gateway_response",
        usage_origin: "gateway",
        client_origin,
        codex_event_key: None,
        gateway_request_id: Some(request_id.to_string()),
        attempt_index,
        is_final_attempt,
        http_status: status.map(|value| value.as_u16()),
        outcome,
        token_source: collected.token_source,
        estimate_method: collected.estimate_method,
    }
}

fn prune_expired_usage(store: &Store, retention_days: u16) -> Result<usize, String> {
    let cutoff = Utc::now() - Duration::days(i64::from(retention_days));
    let conn = store
        .connection
        .lock()
        .map_err(|_| "数据库锁已损坏".to_string())?;
    conn.execute(
        "DELETE FROM usage_records WHERE completed_at < ?1",
        params![log_timestamp(cutoff)],
    )
    .map_err(|error| format!("无法清理过期用量数据：{error}"))
}

#[derive(Default)]
struct UsageAggregate {
    metrics: UsageMetrics,
    completed_requests: u64,
    reported_requests: u64,
    exact_requests: u64,
    estimated_requests: u64,
}

impl UsageAggregate {
    fn add_metric(metric: &mut TokenMetric, value: Option<u64>, estimated: bool) {
        if let Some(value) = value {
            metric.value = metric.value.saturating_add(value);
            if estimated {
                metric.estimated_value = metric.estimated_value.saturating_add(value);
            }
            metric.reported_count = metric.reported_count.saturating_add(1);
        }
    }

    fn add(&mut self, usage: &CapturedUsage, token_source: &str) {
        self.completed_requests = self.completed_requests.saturating_add(1);
        if usage.is_reported() {
            self.reported_requests = self.reported_requests.saturating_add(1);
            if token_source == "estimated" {
                self.estimated_requests = self.estimated_requests.saturating_add(1);
            } else {
                self.exact_requests = self.exact_requests.saturating_add(1);
            }
        }
        let estimated = token_source == "estimated";
        Self::add_metric(
            &mut self.metrics.input_tokens,
            usage.input_tokens,
            estimated,
        );
        Self::add_metric(
            &mut self.metrics.output_tokens,
            usage.output_tokens,
            estimated,
        );
        Self::add_metric(
            &mut self.metrics.cache_hit_tokens,
            usage.cache_read_tokens,
            false,
        );
        Self::add_metric(
            &mut self.metrics.total_tokens,
            usage.total_tokens,
            estimated,
        );
    }

    fn data_quality(&self) -> String {
        match (self.exact_requests, self.estimated_requests) {
            (0, 0) => "unknown",
            (0, _) => "estimated",
            (_, 0) => "exact",
            _ => "mixed",
        }
        .to_string()
    }
}

fn coverage_percent(reported: u64, completed: u64) -> f64 {
    if completed == 0 {
        return 0.0;
    }
    ((reported as f64 / completed as f64) * 1_000.0).round() / 10.0
}

fn local_midnight(date: NaiveDate) -> Result<DateTime<Utc>, String> {
    let local = Local
        .from_local_datetime(
            &date
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| "无法构造本地日期边界".to_string())?,
        )
        .earliest()
        .ok_or_else(|| format!("本机时区无法表示日期 {date}"))?;
    Ok(local.with_timezone(&Utc))
}

fn usage_period_bounds(
    query: &UsageQuery,
) -> Result<
    (
        UsagePeriod,
        NaiveDate,
        NaiveDate,
        DateTime<Utc>,
        DateTime<Utc>,
    ),
    String,
> {
    let today = Local::now().date_naive();
    let (start, end, mode, label, resolution) = match query.mode {
        UsageQueryMode::Today => (today, today, "today", "今天".to_string(), "hour"),
        UsageQueryMode::Range => {
            let start = NaiveDate::parse_from_str(
                query
                    .start_date
                    .as_deref()
                    .ok_or_else(|| "日期范围缺少开始日期".to_string())?,
                "%Y-%m-%d",
            )
            .map_err(|_| "开始日期格式必须为 YYYY-MM-DD".to_string())?;
            let end = NaiveDate::parse_from_str(
                query
                    .end_date
                    .as_deref()
                    .ok_or_else(|| "日期范围缺少结束日期".to_string())?,
                "%Y-%m-%d",
            )
            .map_err(|_| "结束日期格式必须为 YYYY-MM-DD".to_string())?;
            if start > end {
                return Err("开始日期不能晚于结束日期".to_string());
            }
            if (end - start).num_days() + 1 > 30 {
                return Err("自定义日期范围最多查询 30 个自然日".to_string());
            }
            (start, end, "range", format!("{start} 至 {end}"), "day")
        }
        UsageQueryMode::Month => {
            let month = query
                .month
                .as_deref()
                .ok_or_else(|| "月份查询缺少月份".to_string())?;
            let start = NaiveDate::parse_from_str(&format!("{month}-01"), "%Y-%m-%d")
                .map_err(|_| "月份格式必须为 YYYY-MM".to_string())?;
            let next_month = if start.month() == 12 {
                NaiveDate::from_ymd_opt(start.year() + 1, 1, 1)
            } else {
                NaiveDate::from_ymd_opt(start.year(), start.month() + 1, 1)
            }
            .ok_or_else(|| "无法计算月份结束日期".to_string())?;
            (
                start,
                next_month - Duration::days(1),
                "month",
                format!("{}年{}月", start.year(), start.month()),
                "day",
            )
        }
    };
    let queries_future = match query.mode {
        // 当前月份需要展示完整自然月，其中尚未到来的日期只会返回零值。
        UsageQueryMode::Month => start > today,
        UsageQueryMode::Today | UsageQueryMode::Range => start > today || end > today,
    };
    if queries_future {
        return Err("不能查询未来日期或未来月份".to_string());
    }
    let end_exclusive_date = end
        .checked_add_signed(Duration::days(1))
        .ok_or_else(|| "查询结束日期超出支持范围".to_string())?;
    let start_at = local_midnight(start)?;
    let end_at = local_midnight(end_exclusive_date)?;
    Ok((
        UsagePeriod {
            mode: mode.to_string(),
            start_date: start.to_string(),
            end_date: end.to_string(),
            label,
            resolution: resolution.to_string(),
        },
        start,
        end,
        start_at,
        end_at,
    ))
}

fn usage_from_columns(
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    cached_tokens: Option<i64>,
    cache_write_tokens: Option<i64>,
    reasoning_tokens: Option<i64>,
    total_tokens: Option<i64>,
) -> CapturedUsage {
    let nonnegative = |value: Option<i64>| value.and_then(|item| u64::try_from(item).ok());
    CapturedUsage {
        input_tokens: nonnegative(input_tokens),
        output_tokens: nonnegative(output_tokens),
        cache_read_tokens: nonnegative(cached_tokens),
        cache_write_tokens: nonnegative(cache_write_tokens),
        reasoning_tokens: nonnegative(reasoning_tokens),
        total_tokens: nonnegative(total_tokens),
    }
}

fn query_usage_statistics(state: &AppState, query: &UsageQuery) -> Result<UsageStatistics, String> {
    let (period, start, end, start_at, end_at) = usage_period_bounds(query)?;
    // 中转写操作固定按“配置锁 → 数据库锁”执行；查询沿用同一顺序避免并发保存时锁顺序反转。
    let configured_names = state
        .config
        .read()
        .unwrap()
        .iter()
        .map(|provider| (provider.id.clone(), provider.name.clone()))
        .collect::<HashMap<_, _>>();
    let conn = state
        .store
        .connection
        .lock()
        .map_err(|_| "数据库锁已损坏".to_string())?;
    let codex_parser_current = codex_usage::parser_is_current(&conn);
    let mut statement = conn
        .prepare(
            "SELECT completed_at,provider_id,provider_name,
                    input_tokens,output_tokens,cached_tokens,cache_write_tokens,
                    reasoning_tokens,total_tokens,usage_origin,token_source
             FROM usage_records
             WHERE completed_at >= ?1 AND completed_at < ?2
               AND (data_source='codex_session' OR request_path='/v1/responses'
                    OR request_path='/v1/chat/completions')
               AND (?3=1 OR token_source<>'codex_session')
              ORDER BY completed_at ASC",
        )
        .map_err(|error| error.to_string())?;
    let rows = statement
        .query_map(
            params![
                log_timestamp(start_at),
                log_timestamp(end_at),
                i64::from(codex_parser_current)
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                    row.get::<_, Option<i64>>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, String>(10)?,
                ))
            },
        )
        .map_err(|error| error.to_string())?;

    let mut provider_aggregates = if matches!(query.source, UsageSource::All | UsageSource::Gateway)
    {
        configured_names
            .iter()
            .map(|(id, name)| (id.clone(), (name.clone(), UsageAggregate::default())))
            .collect::<HashMap<_, _>>()
    } else {
        HashMap::new()
    };
    if matches!(query.source, UsageSource::All | UsageSource::Official) {
        provider_aggregates
            .entry(codex_usage::OFFICIAL_PROVIDER_ID.to_string())
            .or_insert_with(|| {
                (
                    codex_usage::OFFICIAL_PROVIDER_NAME.to_string(),
                    UsageAggregate::default(),
                )
            });
    }
    let mut series_aggregates: HashMap<String, UsageAggregate> = HashMap::new();
    let mut totals = UsageAggregate::default();

    for row in rows {
        let (
            timestamp,
            provider_id,
            stored_name,
            input,
            output,
            cached,
            cache_write,
            reasoning,
            total,
            origin,
            token_source,
        ) = row.map_err(|error| error.to_string())?;
        let selected = match query.source {
            UsageSource::All => true,
            UsageSource::Official => origin == "official",
            UsageSource::Gateway => origin == "gateway",
            UsageSource::Unconfirmed => origin == "unconfirmed",
        };
        if !selected {
            continue;
        }
        let completed_at = match DateTime::parse_from_rfc3339(&timestamp) {
            Ok(value) => value.with_timezone(&Utc),
            Err(error) => {
                warn!(%error, "invalid usage timestamp ignored");
                continue;
            }
        };
        let usage = usage_from_columns(input, output, cached, cache_write, reasoning, total);
        totals.add(&usage, &token_source);
        let provider = provider_aggregates
            .entry(provider_id.clone())
            .or_insert_with(|| (stored_name.clone(), UsageAggregate::default()));
        if let Some(current_name) = configured_names.get(&provider_id) {
            provider.0 = current_name.clone();
        } else {
            // 已删除中转可能有多次改名，按时间顺序保留最后一次请求时的名称。
            provider.0 = stored_name;
        }
        provider.1.add(&usage, &token_source);

        let local = completed_at.with_timezone(&Local);
        let bucket = if period.resolution == "hour" {
            format!("{:02}", local.hour())
        } else {
            local.date_naive().to_string()
        };
        series_aggregates
            .entry(bucket)
            .or_default()
            .add(&usage, &token_source);
    }
    drop(statement);
    drop(conn);

    let mut providers = provider_aggregates
        .into_iter()
        .map(
            |(provider_id, (provider_name, aggregate))| ProviderUsageStatistics {
                deleted: !configured_names.contains_key(&provider_id)
                    && provider_id != codex_usage::OFFICIAL_PROVIDER_ID
                    && provider_id != codex_usage::UNCONFIRMED_PROVIDER_ID,
                provider_id,
                provider_name,
                completed_requests: aggregate.completed_requests,
                reported_requests: aggregate.reported_requests,
                coverage_percent: coverage_percent(
                    aggregate.reported_requests,
                    aggregate.completed_requests,
                ),
                data_quality: aggregate.data_quality(),
                metrics: aggregate.metrics,
            },
        )
        .collect::<Vec<_>>();
    providers.sort_by(|left, right| {
        let rank = |provider: &ProviderUsageStatistics| {
            if provider.provider_id == codex_usage::OFFICIAL_PROVIDER_ID {
                0
            } else if provider.provider_id == codex_usage::UNCONFIRMED_PROVIDER_ID {
                3
            } else if provider.deleted {
                2
            } else {
                1
            }
        };
        rank(left)
            .cmp(&rank(right))
            .then_with(|| {
                right
                    .metrics
                    .total_tokens
                    .value
                    .cmp(&left.metrics.total_tokens.value)
            })
            .then_with(|| left.provider_name.cmp(&right.provider_name))
    });

    let mut series = Vec::new();
    if period.resolution == "hour" {
        for hour in 0..24 {
            let key = format!("{hour:02}");
            let aggregate = series_aggregates.remove(&key).unwrap_or_default();
            series.push(UsageSeriesPoint {
                key,
                label: format!("{hour:02}:00"),
                completed_requests: aggregate.completed_requests,
                reported_requests: aggregate.reported_requests,
                metrics: aggregate.metrics,
            });
        }
    } else {
        let mut date = start;
        while date <= end {
            let key = date.to_string();
            let aggregate = series_aggregates.remove(&key).unwrap_or_default();
            series.push(UsageSeriesPoint {
                key,
                label: format!("{:02}-{:02}", date.month(), date.day()),
                completed_requests: aggregate.completed_requests,
                reported_requests: aggregate.reported_requests,
                metrics: aggregate.metrics,
            });
            date = date
                .checked_add_signed(Duration::days(1))
                .ok_or_else(|| "无法生成用量趋势日期".to_string())?;
        }
    }

    let data_quality = totals.data_quality();
    let total_metrics = totals.metrics;
    Ok(UsageStatistics {
        period,
        totals: total_metrics,
        providers,
        series,
        completed_requests: totals.completed_requests,
        reported_requests: totals.reported_requests,
        coverage_percent: coverage_percent(totals.reported_requests, totals.completed_requests),
        data_quality,
        codex_sync_status: state.codex_usage_status.lock().unwrap().clone(),
    })
}

fn append_event_log(state: &AppState, entry: EventLog) {
    {
        let mut logs = state.logs.lock().unwrap();
        if logs.len() == MAX_EVENT_LOGS {
            logs.pop_front();
        }
        logs.push_back(entry.clone());
    }
    // 单次短写入保证进程正常退出前日志已经落盘；失败只写系统诊断，避免递归生成日志。
    if let Err(error) = persist_event_log(&state.store, &entry) {
        warn!(%error, "event log persistence failed");
    }
}

fn prune_state_logs(state: &AppState, retention_days: u16) -> Result<usize, String> {
    let removed = prune_expired_logs(&state.store, retention_days)?;
    let cutoff = Utc::now() - Duration::days(i64::from(retention_days));
    state
        .logs
        .lock()
        .map_err(|_| "运行日志不可用".to_string())?
        .retain(|entry| entry.timestamp >= cutoff);
    Ok(removed)
}

fn push_log(state: &AppState, level: &str, message: impl Into<String>, provider_id: Option<&str>) {
    push_log_with_model(state, level, message, provider_id, None);
}

fn push_log_with_model(
    state: &AppState,
    level: &str,
    message: impl Into<String>,
    provider_id: Option<&str>,
    model: Option<&str>,
) {
    push_diagnostic_log(state, level, message, provider_id, model, None);
}

fn push_diagnostic_log(
    state: &AppState,
    level: &str,
    message: impl Into<String>,
    provider_id: Option<&str>,
    model: Option<&str>,
    event_type: Option<&str>,
) {
    append_event_log(
        state,
        EventLog {
            timestamp: Utc::now(),
            level: level.to_string(),
            message: message.into(),
            provider_id: provider_id.map(ToOwned::to_owned),
            // 模型名属于路由元数据，允许展示但限制长度；prompt 和响应正文始终不会进入日志。
            model: model.map(|value| value.chars().take(120).collect()),
            event_type: event_type.map(ToOwned::to_owned),
            routing_event: None,
            diagnostic_detail: None,
        },
    );
}

fn push_diagnostic_detail_log(
    state: &AppState,
    level: &str,
    message: impl Into<String>,
    event_type: &str,
    detail: DiagnosticDetail,
) {
    append_event_log(
        state,
        EventLog {
            timestamp: Utc::now(),
            level: level.to_string(),
            message: message.into(),
            provider_id: Some(detail.provider_id.clone()),
            model: detail.model.clone(),
            event_type: Some(event_type.to_string()),
            routing_event: None,
            diagnostic_detail: Some(detail),
        },
    );
}

#[allow(clippy::too_many_arguments)]
fn stream_diagnostic_detail(
    request_id: &str,
    provider_id: &str,
    model: Option<&str>,
    request_path: &str,
    status: Option<StatusCode>,
    upstream_request_id: Option<String>,
    failure: &StreamFailure,
    elapsed_ms: u64,
    attempt_index: u64,
    policy: &RoutingPolicy,
    retry_wait_sec: Option<u64>,
    action: &str,
    attempts: Vec<RoutingAttempt>,
    business_output_started: bool,
) -> DiagnosticDetail {
    DiagnosticDetail {
        request_id: request_id.to_string(),
        provider_id: provider_id.to_string(),
        model: model.map(|value| sanitized_diagnostic_text(value, 120)),
        request_path: sanitized_diagnostic_text(request_path, 180),
        error_class: failure.classification.as_str().to_string(),
        http_status: status.map(|status| status.as_u16()),
        error_code: failure.code.clone(),
        error_type: failure.error_type.clone(),
        error_message: Some(sanitized_diagnostic_text(&failure.message, 300)),
        incomplete_reason: failure.incomplete_reason.clone(),
        upstream_request_id,
        last_sse_event: failure.last_sse_event.clone(),
        terminal_event: failure.terminal_event.clone(),
        terminal_received: failure.terminal_received,
        business_output_started,
        bytes_received: failure.bytes_received,
        elapsed_ms,
        attempt_index,
        max_attempts: policy.retry_count.saturating_add(1),
        retry_wait_sec,
        fallback_retry_enabled: policy.retry_unknown_transient_errors,
        failover_enabled: policy.allow_provider_failover,
        action: action.to_string(),
        parse_error: failure.parse_error.clone(),
        attempts,
    }
}

fn push_routing_event(state: &AppState, message: String, event: RoutingEvent) {
    append_event_log(
        state,
        EventLog {
            timestamp: event.timestamp,
            level: match event.event_type.as_str() {
                "model_availability_recovered" => "info",
                "model_temporarily_unavailable" => "error",
                _ => "warning",
            }
            .to_string(),
            message,
            provider_id: event.target_provider_id.clone(),
            model: event.model.clone(),
            event_type: Some(event.event_type.clone()),
            routing_event: Some(event.clone()),
            diagnostic_detail: None,
        },
    );

    let notify = state.policy.read().unwrap().notify_switches;
    let Some(app) = state.app_handle.as_ref() else {
        return;
    };
    let _ = app.emit("routing-event", &event);
    // 模型级重试只进入日志和详情，不复用自动切换的 Toast 与托盘提示。
    if !notify || event.event_type != "auto_switch" {
        return;
    }
    let source = event
        .source_provider_id
        .as_deref()
        .and_then(|id| event.attempts.iter().find(|item| item.provider_id == id))
        .map(|item| item.provider_name.as_str())
        .unwrap_or("原中转");
    let target = event
        .target_provider_id
        .as_deref()
        .and_then(|id| event.attempts.iter().find(|item| item.provider_id == id))
        .map(|item| item.provider_name.as_str())
        .unwrap_or("备用中转");
    if let Some(tray) = app.tray_by_id("main-tray") {
        let _ = tray.set_tooltip(Some(format!("AI Gateway · 已从{source}切换到{target}")));
        let app = app.clone();
        tauri::async_runtime::spawn(async move {
            sleep(StdDuration::from_secs(10)).await;
            if let Some(tray) = app.tray_by_id("main-tray") {
                let _ = tray.set_tooltip(Some("AI Gateway"));
            }
        });
    }
}

fn push_model_retry_event(
    state: &AppState,
    provider: &ProviderConfig,
    model: Option<&str>,
    request_path: &str,
    request_body_bytes: usize,
    attempts: Vec<RoutingAttempt>,
    model_error: &ModelTemporaryError,
    recovered: bool,
    status_code: Option<u16>,
) {
    let retry_count = u64::try_from(attempts.len().saturating_sub(1)).unwrap_or(u64::MAX);
    let event_type = if recovered {
        "model_availability_recovered"
    } else {
        "model_temporarily_unavailable"
    };
    let event = RoutingEvent {
        id: Uuid::new_v4().to_string(),
        timestamp: Utc::now(),
        event_type: event_type.to_string(),
        model: model.map(ToOwned::to_owned),
        request_path: request_path.to_string(),
        request_body_bytes,
        source_provider_id: Some(provider.id.clone()),
        target_provider_id: Some(provider.id.clone()),
        status_code,
        retry_count,
        attempts,
        reason: model_error.message.clone(),
        final_result: event_type.to_string(),
    };
    let code = model_error
        .code
        .as_deref()
        .unwrap_or("model_temporarily_unavailable");
    let message = if recovered {
        format!("模型或分组恢复，已在 {} 完成重试（{code}）", provider.name)
    } else {
        format!(
            "模型或分组暂不可用，重试已耗尽：{}（{code}）",
            provider.name
        )
    };
    push_routing_event(state, message, event);
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CircuitImpact {
    None,
    DegradedOnly,
    ProviderCircuit,
}

fn refresh_circuit_state(record: &mut HealthRecord) -> bool {
    if record.state == HealthState::Open && record.cooldown_until.is_some_and(|at| at <= Utc::now())
    {
        // 冷却到期只转为半开；必须由下一次真实模型请求确认是否恢复。
        record.state = HealthState::HalfOpen;
        return true;
    }
    false
}

fn can_use(record: &mut HealthRecord) -> bool {
    refresh_circuit_state(record);
    record.state != HealthState::Open
}

fn mark_probe_success(state: &AppState, id: &str, latency: u64) -> HealthState {
    let (new_state, recovered) = {
        let mut health = state.health.lock().unwrap();
        let record = health.entry(id.to_string()).or_default();
        let connection_issue = matches!(
            record.last_issue_kind,
            HealthIssueKind::Transport
                | HealthIssueKind::Upstream
                | HealthIssueKind::Configuration
                | HealthIssueKind::None
        );
        let recovered = connection_issue
            && (matches!(record.state, HealthState::Open | HealthState::HalfOpen)
                || record.consecutive_failures > 0);
        record.latency_ms = Some(latency);
        record.total_requests += 1;
        record.successful_requests += 1;
        if connection_issue {
            // /models 只证明基础连接和鉴权可用，不能清除具体模型、限流或流式问题。
            record.state = HealthState::Healthy;
            record.consecutive_failures = 0;
            record.last_error = None;
            record.last_issue_kind = HealthIssueKind::None;
            record.cooldown_until = None;
        }
        (record.state.clone(), recovered)
    };
    if recovered {
        push_diagnostic_log(
            state,
            "info",
            "基础连接探测成功，连接级熔断已恢复",
            Some(id),
            None,
            Some("circuit_recovered"),
        );
    }
    new_state
}

fn mark_response_started(state: &AppState, id: &str, latency: u64) {
    let recovered = {
        let mut health = state.health.lock().unwrap();
        let record = health.entry(id.to_string()).or_default();
        let recovered = matches!(record.state, HealthState::Open | HealthState::HalfOpen)
            || record.consecutive_failures > 0
            || record.last_issue_kind != HealthIssueKind::None;
        // 收到成功响应头说明真实模型端点已接受请求；完整成功仍等响应体结束后计数。
        record.state = HealthState::Healthy;
        record.latency_ms = Some(latency);
        record.consecutive_failures = 0;
        record.last_error = None;
        record.last_issue_kind = HealthIssueKind::None;
        record.cooldown_until = None;
        recovered
    };
    if recovered {
        push_diagnostic_log(
            state,
            "info",
            "真实模型请求已建立响应，中转线路已恢复",
            Some(id),
            None,
            Some("circuit_recovered"),
        );
    }
}

fn mark_response_completed(state: &AppState, id: &str) {
    let mut health = state.health.lock().unwrap();
    let record = health.entry(id.to_string()).or_default();
    record.total_requests += 1;
    record.successful_requests += 1;
}

fn record_incomplete_business_response(state: &AppState, id: &str, latency: u64) {
    let mut health = state.health.lock().unwrap();
    let record = health.entry(id.to_string()).or_default();
    // 业务性 incomplete 是一次未完整产出的请求，但不是线路故障：只计请求量，不累计失败或改变熔断状态。
    record.total_requests += 1;
    record.latency_ms = Some(latency);
}

fn record_model_temporarily_unavailable(state: &AppState, id: &str, message: &str, latency: u64) {
    record_degraded_issue(state, id, message, latency, HealthIssueKind::Model, None);
}

fn mark_upstream_rejection(state: &AppState, id: &str, status: StatusCode, latency: u64) {
    let issue_kind = if matches!(
        status,
        StatusCode::BAD_REQUEST
            | StatusCode::UNAUTHORIZED
            | StatusCode::FORBIDDEN
            | StatusCode::NOT_FOUND
            | StatusCode::UNPROCESSABLE_ENTITY
    ) {
        HealthIssueKind::Configuration
    } else {
        HealthIssueKind::Upstream
    };
    record_degraded_issue(
        state,
        id,
        &format!("上游返回 {status}"),
        latency,
        issue_kind,
        None,
    );
}

fn record_degraded_issue(
    state: &AppState,
    id: &str,
    message: &str,
    latency: u64,
    issue_kind: HealthIssueKind,
    cooldown_until: Option<DateTime<Utc>>,
) -> HealthState {
    let mut health = state.health.lock().unwrap();
    let record = health.entry(id.to_string()).or_default();
    // 模型、限流和请求级问题只降低可观测状态，不增加线路连续失败次数。
    record.state = HealthState::Degraded;
    record.latency_ms = Some(latency);
    record.consecutive_failures = 0;
    record.last_error = Some(message.chars().take(180).collect());
    record.last_issue_kind = issue_kind;
    record.cooldown_until = cooldown_until;
    record.total_requests += 1;
    record.state.clone()
}

fn mark_provider_failure(
    state: &AppState,
    id: &str,
    message: String,
    issue_kind: HealthIssueKind,
    policy: &RoutingPolicy,
) -> HealthState {
    debug_assert!(matches!(
        issue_kind,
        HealthIssueKind::Transport | HealthIssueKind::Upstream | HealthIssueKind::Stream
    ));
    let (new_state, opened) = {
        let mut health = state.health.lock().unwrap();
        let record = health.entry(id.to_string()).or_default();
        let was_open = record.state == HealthState::Open;
        record.total_requests += 1;
        record.consecutive_failures += 1;
        record.last_error = Some(message.chars().take(180).collect());
        record.last_issue_kind = issue_kind;
        // 只有整个客户端请求的同一中转重试耗尽后才调用这里，因此一次请求至多累计一次。
        if record.consecutive_failures >= u32::from(policy.circuit_failure_threshold) {
            record.cooldown_until =
                Some(Utc::now() + Duration::seconds(policy.circuit_cooldown_sec as i64));
            record.state = HealthState::Open;
        } else {
            record.state = HealthState::Degraded;
        }
        (
            record.state.clone(),
            !was_open && record.state == HealthState::Open,
        )
    };
    if opened {
        push_diagnostic_log(
            state,
            "error",
            "线路级故障达到阈值，中转已进入冷却",
            Some(id),
            None,
            Some("circuit_opened"),
        );
    }
    new_state
}

fn record_health_probe(
    state: &AppState,
    id: &str,
    probe_state: HealthState,
    latency_ms: Option<u64>,
    error: Option<String>,
) {
    let mut health = state.health.lock().unwrap();
    let record = health.entry(id.to_string()).or_default();
    record.probe_sequence += 1;
    if record.recent_probes.len() == MAX_HEALTH_PROBES {
        record.recent_probes.pop_front();
    }
    // 探测历史只在定时探测完成后写入；普通请求即使改变健康状态也不会伪造探测短条。
    record.recent_probes.push_back(HealthProbeSample {
        sequence: record.probe_sequence,
        timestamp: Utc::now(),
        state: probe_state,
        latency_ms,
        error: error.map(|message| message.chars().take(180).collect()),
    });
}

fn mark_payload_too_large(
    state: &AppState,
    id: &str,
    body_bytes: usize,
    latency: u64,
    policy: &RoutingPolicy,
) {
    let mut health = state.health.lock().unwrap();
    let record = health.entry(id.to_string()).or_default();
    record.state = HealthState::Degraded;
    record.latency_ms = Some(latency);
    record.consecutive_failures = 0;
    record.last_error = Some(format!("上游拒绝了 {body_bytes} 字节的请求体（413）"));
    record.last_issue_kind = HealthIssueKind::Payload;
    record.cooldown_until = None;
    record.total_requests += 1;
    record.payload_rejected_size = Some(body_bytes);
    record.payload_rejected_until =
        Some(Utc::now() + Duration::seconds(policy.payload_too_large_skip_sec as i64));
}

fn payload_size_allowed(record: &mut HealthRecord, body_bytes: usize) -> bool {
    if record
        .payload_rejected_until
        .is_some_and(|until| until <= Utc::now())
    {
        record.payload_rejected_size = None;
        record.payload_rejected_until = None;
    }
    // 413 只屏蔽达到历史拒绝大小的请求；更小请求仍可通过该中转，避免把容量差异误判为宕机。
    !record
        .payload_rejected_until
        .is_some_and(|until| until > Utc::now())
        || record
            .payload_rejected_size
            .map(|rejected_size| body_bytes < rejected_size)
            .unwrap_or(true)
}

fn normalize_url(base: &str, path: &str, query: Option<&str>) -> Result<Url, String> {
    let base = base.trim_end_matches('/');
    let suffix = if base.ends_with("/v1") && path.starts_with("/v1") {
        &path[3..]
    } else {
        path
    };
    let full = format!(
        "{}{}{}",
        base,
        suffix,
        query.map(|q| format!("?{q}")).unwrap_or_default()
    );
    Url::parse(&full).map_err(|e| format!("Base URL 无效: {e}"))
}

fn sanitize_base_url(value: &str) -> Result<String, String> {
    // 用户常从带中文引号的 Markdown 或聊天窗口复制地址；只清理两端包装字符，绝不修改 URL 中间内容。
    let cleaned = value
        .trim()
        .trim_matches(|character: char| matches!(character, '"' | '\'' | '“' | '”' | '‘' | '’'))
        .trim()
        .trim_end_matches('/')
        .to_string();
    if cleaned.is_empty() {
        return Err("Base URL 不能为空".to_string());
    }
    let parsed = Url::parse(&cleaned).map_err(|error| format!("Base URL 无效：{error}"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err("Base URL 只支持 http 或 https".to_string());
    }
    if parsed.host_str().is_none() {
        return Err("Base URL 缺少主机名".to_string());
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err("Base URL 不应包含 query 或 fragment".to_string());
    }
    Ok(cleaned)
}
fn auth_valid(headers: &HeaderMap, token: &str) -> bool {
    let authorization_valid = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|value| value == format!("Bearer {token}") || value == token);
    let gateway_header_valid = headers
        .get("x-ai-gateway-token")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == token);
    authorization_valid || gateway_header_valid
}
fn should_retry_status(status: StatusCode) -> bool {
    status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

fn retry_after_seconds(headers: &HeaderMap) -> Option<u64> {
    let value = headers.get(header::RETRY_AFTER)?.to_str().ok()?.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(seconds);
    }
    DateTime::parse_from_rfc2822(value)
        .ok()
        .map(|at| (at.with_timezone(&Utc) - Utc::now()).num_seconds().max(0) as u64)
}

fn retry_budget_allows(
    retry_index: u64,
    retry_count: u64,
    waited_seconds: u64,
    next_wait_seconds: u64,
    max_retry_wait_sec: u64,
) -> bool {
    retry_index < retry_count
        && (max_retry_wait_sec == 0
            || waited_seconds.saturating_add(next_wait_seconds) <= max_retry_wait_sec)
}
#[derive(Deserialize)]
struct RequestMetadata {
    model: Option<String>,
}

fn request_model(body: &Bytes) -> Option<String> {
    // 路由只需要 model；忽略其余字段可避免为大型 prompt 再构造一份完整 JSON Value。
    serde_json::from_slice::<RequestMetadata>(body)
        .ok()
        .and_then(|metadata| metadata.model)
}

#[derive(Clone, Debug)]
struct ModelTemporaryError {
    code: Option<String>,
    message: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ErrorClassification {
    Retryable,
    Permanent,
    IncompleteBusiness,
    Unknown,
}

impl ErrorClassification {
    fn as_str(self) -> &'static str {
        match self {
            Self::Retryable => "retryable",
            Self::Permanent => "permanent",
            Self::IncompleteBusiness => "incomplete_business",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Debug)]
struct StreamFailure {
    classification: ErrorClassification,
    code: Option<String>,
    error_type: Option<String>,
    message: String,
    incomplete_reason: Option<String>,
    last_sse_event: Option<String>,
    terminal_event: Option<String>,
    terminal_received: bool,
    bytes_received: u64,
    parse_error: Option<String>,
}

fn sanitized_diagnostic_text(value: &str, max_chars: usize) -> String {
    value
        .chars()
        .filter(|character| !character.is_control() || character.is_whitespace())
        .take(max_chars)
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn unparsed_body_diagnostic(headers: &HeaderMap, body: &[u8], parse_error: &str) -> String {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| sanitized_diagnostic_text(value, 120))
        .unwrap_or_else(|| "unknown".to_string());
    let body_hash = format!("{:x}", Sha256::digest(body));
    format!(
        "{}；content_type={}；sha256={}",
        sanitized_diagnostic_text(parse_error, 180),
        content_type,
        body_hash
    )
}

fn upstream_request_id(headers: &HeaderMap) -> Option<String> {
    ["x-request-id", "request-id", "openai-request-id", "cf-ray"]
        .into_iter()
        .find_map(|name| headers.get(name))
        .and_then(|value| value.to_str().ok())
        .map(|value| sanitized_diagnostic_text(value, 160))
        .filter(|value| !value.is_empty())
}

fn classify_error_fields(
    code: Option<&str>,
    error_type: Option<&str>,
    message: Option<&str>,
) -> ErrorClassification {
    let normalized = [
        code.unwrap_or_default(),
        error_type.unwrap_or_default(),
        message.unwrap_or_default(),
    ]
    .join(" ")
    .to_ascii_lowercase();
    let permanent = [
        "invalid_request",
        "authentication",
        "unauthorized",
        "permission",
        "forbidden",
        "model_not_found",
        "context_length",
        "insufficient_quota",
        "billing",
        "balance",
        "content_policy",
        "safety",
    ];
    if permanent.iter().any(|marker| normalized.contains(marker)) {
        return ErrorClassification::Permanent;
    }
    let retryable = [
        "server_error",
        "internal_error",
        "upstream_error",
        "service_unavailable",
        "overloaded",
        "timeout",
        "timed out",
        "rate_limit",
        "capacity",
        "concurrency",
        "temporar",
        "try again",
        "processing error",
    ];
    if retryable.iter().any(|marker| normalized.contains(marker)) {
        return ErrorClassification::Retryable;
    }
    ErrorClassification::Unknown
}

fn classify_incomplete_reason(reason: Option<&str>) -> ErrorClassification {
    let normalized = reason.unwrap_or_default().to_ascii_lowercase();
    if [
        "max_output_tokens",
        "content_filter",
        "content_policy",
        "safety",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
    {
        return ErrorClassification::IncompleteBusiness;
    }
    if [
        "timeout",
        "overload",
        "interrupted",
        "upstream",
        "server_error",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
    {
        return ErrorClassification::Retryable;
    }
    ErrorClassification::Unknown
}

fn error_text_matches_model_temporary(
    code: Option<&str>,
    error_type: Option<&str>,
    message: Option<&str>,
) -> bool {
    [code, error_type, message]
        .into_iter()
        .flatten()
        .any(model_temporary_message_matches)
}

fn http_error_impact(
    status: StatusCode,
    failure: &StreamFailure,
) -> (CircuitImpact, HealthIssueKind) {
    if error_text_matches_model_temporary(
        failure.code.as_deref(),
        failure.error_type.as_deref(),
        Some(&failure.message),
    ) {
        return (CircuitImpact::DegradedOnly, HealthIssueKind::Model);
    }
    if status == StatusCode::TOO_MANY_REQUESTS {
        return (CircuitImpact::DegradedOnly, HealthIssueKind::RateLimit);
    }
    if matches!(
        status,
        StatusCode::REQUEST_TIMEOUT
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    ) {
        return (CircuitImpact::ProviderCircuit, HealthIssueKind::Upstream);
    }
    (CircuitImpact::None, HealthIssueKind::Upstream)
}

fn stream_failure_impact(failure: &StreamFailure) -> (CircuitImpact, HealthIssueKind) {
    if error_text_matches_model_temporary(
        failure.code.as_deref(),
        failure.error_type.as_deref(),
        Some(&failure.message),
    ) {
        return (CircuitImpact::DegradedOnly, HealthIssueKind::Model);
    }
    if failure.classification == ErrorClassification::IncompleteBusiness
        || failure.classification == ErrorClassification::Permanent
    {
        return (CircuitImpact::None, HealthIssueKind::Upstream);
    }
    let stream_transport = failure
        .code
        .as_deref()
        .is_some_and(|code| code.contains("stream") || code.contains("upstream"))
        || failure
            .error_type
            .as_deref()
            .is_some_and(|kind| kind.contains("stream") || kind.contains("unexpected_eof"));
    if stream_transport || failure.classification == ErrorClassification::Retryable {
        return (CircuitImpact::ProviderCircuit, HealthIssueKind::Stream);
    }
    (CircuitImpact::None, HealthIssueKind::Stream)
}

fn response_error_fields(
    value: &serde_json::Value,
) -> (Option<String>, Option<String>, Option<String>) {
    let error = value
        .pointer("/response/error")
        .or_else(|| value.get("error"));
    let code = error
        .and_then(|item| item.get("code"))
        .and_then(|item| item.as_str())
        .map(|value| sanitized_diagnostic_text(value, 120));
    let error_type = error
        .and_then(|item| item.get("type"))
        .and_then(|item| item.as_str())
        .map(|value| sanitized_diagnostic_text(value, 120));
    let message = error
        .and_then(|item| {
            item.get("message")
                .and_then(|message| message.as_str())
                .or_else(|| item.as_str())
        })
        .or_else(|| value.get("message").and_then(|item| item.as_str()))
        .map(|value| sanitized_diagnostic_text(value, 300));
    (code, error_type, message)
}

fn model_temporary_message_matches(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase().replace(['_', '-'], " ");
    let model_capacity = normalized.contains("model")
        && (normalized.contains("at capacity")
            || normalized.contains("capacity exceeded")
            || normalized.contains("capacity limit"));
    let account_concurrency = normalized.contains("concurrency")
        && normalized.contains("limit")
        && (normalized.contains("account") || normalized.contains("exceeded"));
    let model_route_unavailable = [
        "no available channel",
        "no channel available",
        "model unavailable",
        "group unavailable",
        "no route available for model",
    ]
    .iter()
    .any(|marker| normalized.contains(marker));
    model_capacity || account_concurrency || model_route_unavailable
}

fn model_temporary_error_from_json(value: &serde_json::Value) -> Option<ModelTemporaryError> {
    let error = if value.get("type").and_then(|item| item.as_str()) == Some("response.failed") {
        value.pointer("/response/error")
    } else {
        value
            .get("error")
            .or_else(|| value.pointer("/response/error"))
    };

    let code = error
        .and_then(|item| item.get("code"))
        .and_then(|item| item.as_str())
        .map(ToOwned::to_owned);
    let error_type = error
        .and_then(|item| item.get("type"))
        .and_then(|item| item.as_str());
    let message = error
        .and_then(|item| {
            item.get("message")
                .and_then(|message| message.as_str())
                .or_else(|| item.as_str())
        })
        .or_else(|| value.get("message").and_then(|item| item.as_str()))?;
    // 上游经常同时返回通用 code 和更具体的 type，必须分别判断，不能让通用 code 遮蔽容量类型。
    let identifier_matches = code
        .as_deref()
        .into_iter()
        .chain(error_type)
        .map(str::to_ascii_lowercase)
        .any(|identifier| {
            (identifier.contains("capacity") && identifier.contains("model"))
                || identifier.contains("concurrency_limit")
                || identifier.contains("account_concurrency")
                || identifier.contains("model_unavailable")
                || identifier.contains("group_unavailable")
                || identifier.contains("no_available_channel")
                || identifier.contains("no_route_available")
        });
    if !identifier_matches && !model_temporary_message_matches(message) {
        return None;
    }
    Some(ModelTemporaryError {
        code,
        message: message.chars().take(180).collect(),
    })
}

fn sse_frame_data(frame: &[u8]) -> Option<Vec<u8>> {
    let normalized = String::from_utf8_lossy(frame).replace("\r\n", "\n");
    let mut data = Vec::new();
    for line in normalized.lines() {
        let Some(value) = line.strip_prefix("data:") else {
            continue;
        };
        if !data.is_empty() {
            data.push(b'\n');
        }
        data.extend_from_slice(value.trim_start().as_bytes());
    }
    (!data.is_empty()).then_some(data)
}

fn next_sse_frame_end(buffer: &[u8]) -> Option<usize> {
    let lf = buffer
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|index| index + 2);
    let crlf = buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4);
    match (lf, crlf) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(end), None) | (None, Some(end)) => Some(end),
        (None, None) => None,
    }
}
fn matches_model(provider: &ProviderConfig, model: Option<&str>) -> bool {
    provider.models.is_empty()
        || model.is_some_and(|wanted| {
            provider.models.iter().any(|pattern| {
                pattern == wanted
                    || (pattern.ends_with('*') && wanted.starts_with(pattern.trim_end_matches('*')))
            })
        })
        || model.is_none()
}

fn is_hop_by_hop_header(name: &HeaderName) -> bool {
    // RFC 9110 规定这些头只描述当前连接，跨代理转发会破坏 Hyper 重新生成的消息边界。
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "proxy-connection"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn is_official_identity_header(name: &HeaderName) -> bool {
    let name = name.as_str();
    matches!(
        name,
        "cookie" | "chatgpt-account-id" | "openai-organization" | "openai-project"
    ) || name.starts_with("x-openai-")
        || name.starts_with("x-chatgpt-")
}

fn upstream_request(
    state: &AppState,
    provider: &ProviderConfig,
    method: &Method,
    url: Url,
    headers: &HeaderMap,
    body: &Bytes,
) -> reqwest::RequestBuilder {
    let mut builder = state.client.request(method.clone(), url).body(body.clone());
    for (name, value) in headers {
        if name != header::AUTHORIZATION
            && name.as_str() != "x-ai-gateway-token"
            && name != header::HOST
            && name != header::CONTENT_LENGTH
            && name != header::ACCEPT_ENCODING
            && !is_hop_by_hop_header(name)
            // Codex 保持 ChatGPT 登录后会携带官方身份信息；第三方上游只能收到中转自己的 API Key。
            && !is_official_identity_header(name)
        {
            builder = builder.header(name, value);
        }
    }
    builder
        .header(
            header::AUTHORIZATION,
            format!("Bearer {}", provider.api_key),
        )
        // 网关不解压上游内容，明确请求 identity 可避免压缩流被中途截断后产生二次解码错误。
        .header(header::ACCEPT_ENCODING, "identity")
}

enum UpstreamSendError {
    ResponseHeaderTimeout(u64),
    Request(reqwest::Error),
}

async fn send_upstream_request(
    request: reqwest::RequestBuilder,
    timeout_ms: u64,
) -> Result<reqwest::Response, UpstreamSendError> {
    let timeout_ms = timeout_ms.max(1_000);
    // RequestBuilder::timeout 会覆盖整个响应体；这里只限制 send()，收到响应头后 SSE 不再受总时长限制。
    match timeout(StdDuration::from_millis(timeout_ms), request.send()).await {
        Ok(Ok(response)) => Ok(response),
        Ok(Err(error)) => Err(UpstreamSendError::Request(error)),
        Err(_) => Err(UpstreamSendError::ResponseHeaderTimeout(timeout_ms)),
    }
}

type GatewayByteStream =
    Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static>>;

#[derive(Clone, Copy)]
enum UsageResponseKind {
    Json,
    Sse,
    Unsupported,
}

fn json_token(object: &serde_json::Map<String, serde_json::Value>, key: &str) -> Option<u64> {
    object.get(key)?.as_u64()
}

fn usage_from_object(value: &serde_json::Value) -> Option<CapturedUsage> {
    let object = value.as_object()?;
    let input_tokens =
        json_token(object, "input_tokens").or_else(|| json_token(object, "prompt_tokens"));
    let output_tokens =
        json_token(object, "output_tokens").or_else(|| json_token(object, "completion_tokens"));
    let cache_read_tokens = object
        .get("input_tokens_details")
        .or_else(|| object.get("prompt_tokens_details"))
        .and_then(serde_json::Value::as_object)
        .and_then(|details| json_token(details, "cached_tokens"));
    let reasoning_tokens = object
        .get("output_tokens_details")
        .or_else(|| object.get("completion_tokens_details"))
        .and_then(serde_json::Value::as_object)
        .and_then(|details| json_token(details, "reasoning_tokens"));
    let total_tokens = json_token(object, "total_tokens")
        .or_else(|| Some(input_tokens?.saturating_add(output_tokens?)));
    let usage = CapturedUsage {
        input_tokens,
        output_tokens,
        cache_read_tokens,
        cache_write_tokens: object
            .get("input_tokens_details")
            .or_else(|| object.get("prompt_tokens_details"))
            .and_then(serde_json::Value::as_object)
            .and_then(|details| {
                json_token(details, "cache_write_tokens")
                    .or_else(|| json_token(details, "cache_creation_tokens"))
            }),
        reasoning_tokens,
        total_tokens,
    };
    usage.is_reported().then_some(usage)
}

fn usage_from_response_json(value: &serde_json::Value) -> Option<CapturedUsage> {
    let usage =
        if value.get("type").and_then(serde_json::Value::as_str) == Some("response.completed") {
            value.get("response")?.get("usage")?
        } else {
            value.get("usage")?
        };
    usage_from_object(usage)
}

struct UsageCollector {
    kind: UsageResponseKind,
    buffer: Vec<u8>,
    disabled: bool,
    captured: Option<CapturedUsage>,
    request_estimate: Option<usage_estimation::RequestTokenEstimate>,
    output_capture: usage_estimation::OutputTextCapture,
    model: Option<String>,
}

struct CollectedUsage {
    usage: CapturedUsage,
    token_source: &'static str,
    estimate_method: Option<String>,
}

impl UsageCollector {
    fn new(
        kind: UsageResponseKind,
        request_estimate: Option<usage_estimation::RequestTokenEstimate>,
        model: Option<String>,
    ) -> Self {
        Self {
            kind,
            buffer: Vec::new(),
            disabled: false,
            captured: None,
            request_estimate,
            output_capture: usage_estimation::OutputTextCapture::default(),
            model,
        }
    }

    fn observe(&mut self, chunk: &Bytes) {
        if self.disabled {
            return;
        }
        match self.kind {
            UsageResponseKind::Json => {
                if self.buffer.len().saturating_add(chunk.len()) > MAX_USAGE_CAPTURE_BYTES {
                    self.buffer.clear();
                    self.disabled = true;
                } else {
                    self.buffer.extend_from_slice(chunk);
                }
            }
            UsageResponseKind::Sse => {
                self.buffer.extend_from_slice(chunk);
                while let Some(frame_end) = next_sse_frame_end(&self.buffer) {
                    if frame_end > MAX_USAGE_CAPTURE_BYTES {
                        self.buffer.clear();
                        self.disabled = true;
                        return;
                    }
                    let frame = self.buffer.drain(..frame_end).collect::<Vec<_>>();
                    let Some(data) = sse_frame_data(&frame) else {
                        continue;
                    };
                    if data == b"[DONE]" {
                        continue;
                    }
                    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&data) {
                        if let Some(usage) = usage_from_response_json(&value) {
                            self.captured = Some(usage);
                        }
                        self.output_capture.observe_event(&value);
                    }
                }
                if self.buffer.len() > MAX_USAGE_CAPTURE_BYTES {
                    self.buffer.clear();
                    self.disabled = true;
                }
            }
            UsageResponseKind::Unsupported => {}
        }
    }

    fn finish(&mut self, allow_estimate: bool) -> CollectedUsage {
        if !self.disabled && matches!(self.kind, UsageResponseKind::Json) {
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&self.buffer) {
                self.captured = usage_from_response_json(&value);
                self.output_capture.observe_json_response(&value);
            }
        }
        if let Some(usage) = self.captured.take() {
            return CollectedUsage {
                usage,
                token_source: "upstream",
                estimate_method: None,
            };
        }
        if allow_estimate {
            if let (Some(request), Some(output_tokens)) = (
                self.request_estimate.as_ref(),
                self.output_capture.estimated_tokens(self.model.as_deref()),
            ) {
                let total_tokens = request.input_tokens.saturating_add(output_tokens);
                return CollectedUsage {
                    usage: CapturedUsage {
                        input_tokens: Some(request.input_tokens),
                        output_tokens: Some(output_tokens),
                        cache_read_tokens: None,
                        cache_write_tokens: None,
                        reasoning_tokens: None,
                        total_tokens: Some(total_tokens),
                    },
                    token_source: "estimated",
                    estimate_method: Some(request.method.clone()),
                };
            }
        }
        CollectedUsage {
            usage: CapturedUsage::default(),
            token_source: "none",
            estimate_method: None,
        }
    }

    fn saw_output(&self) -> bool {
        self.output_capture.saw_output()
    }
}

struct PreparedUpstreamResponse {
    status: StatusCode,
    headers: HeaderMap,
    prefix: Vec<Bytes>,
    stream: GatewayByteStream,
}

impl PreparedUpstreamResponse {
    fn from_reqwest(response: reqwest::Response) -> Self {
        let status = response.status();
        let headers = response.headers().clone();
        let stream = response
            .bytes_stream()
            .map(|chunk| chunk.map_err(std::io::Error::other));
        Self {
            status,
            headers,
            prefix: Vec::new(),
            stream: Box::pin(stream),
        }
    }

    fn is_sse(&self) -> bool {
        self.headers
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.to_ascii_lowercase().starts_with("text/event-stream"))
    }

    fn usage_response_kind(&self) -> UsageResponseKind {
        let content_type = self
            .headers
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_ascii_lowercase();
        if content_type.starts_with("text/event-stream") {
            UsageResponseKind::Sse
        } else if content_type.contains("json") {
            UsageResponseKind::Json
        } else {
            UsageResponseKind::Unsupported
        }
    }

    fn into_stream(self) -> GatewayByteStream {
        let prefix = futures_util::stream::iter(self.prefix.into_iter().map(Ok));
        Box::pin(prefix.chain(self.stream))
    }

    fn preserve_stream_error(&mut self, error: std::io::Error) {
        let message = error.to_string();
        self.stream = Box::pin(futures_util::stream::once(async move {
            Err(std::io::Error::other(message))
        }));
    }
}

async fn inspect_json_error_response(
    mut response: PreparedUpstreamResponse,
) -> (
    PreparedUpstreamResponse,
    Option<ModelTemporaryError>,
    StreamFailure,
) {
    let mut body = Vec::new();
    let status = response.status;
    while let Some(chunk) = response.stream.next().await {
        match chunk {
            Ok(chunk) => {
                body.extend_from_slice(&chunk);
                response.prefix.push(chunk);
                if body.len() > MAX_ERROR_BODY_BYTES {
                    let parse_error = unparsed_body_diagnostic(
                        &response.headers,
                        &body,
                        "错误正文超过安全解析上限，哈希仅覆盖已读取部分",
                    );
                    return (
                        response,
                        None,
                        StreamFailure {
                            classification: if should_retry_status(status) {
                                ErrorClassification::Retryable
                            } else {
                                ErrorClassification::Permanent
                            },
                            code: Some(format!("http_{}", status.as_u16())),
                            error_type: Some("http_error_body_too_large".to_string()),
                            message: format!("上游返回 {status}"),
                            incomplete_reason: None,
                            last_sse_event: None,
                            terminal_event: None,
                            terminal_received: false,
                            bytes_received: body.len() as u64,
                            parse_error: Some(parse_error),
                        },
                    );
                }
            }
            Err(error) => {
                let parse_error = unparsed_body_diagnostic(
                    &response.headers,
                    &body,
                    &format!("读取错误正文失败：{error}"),
                );
                response.preserve_stream_error(error);
                return (
                    response,
                    None,
                    StreamFailure {
                        classification: if should_retry_status(status) {
                            ErrorClassification::Retryable
                        } else {
                            ErrorClassification::Permanent
                        },
                        code: Some(format!("http_{}", status.as_u16())),
                        error_type: Some("http_error_read_error".to_string()),
                        message: format!("上游返回 {status}，读取错误正文失败"),
                        incomplete_reason: None,
                        last_sse_event: None,
                        terminal_event: None,
                        terminal_received: false,
                        bytes_received: body.len() as u64,
                        parse_error: Some(parse_error),
                    },
                );
            }
        }
    }
    let parsed = serde_json::from_slice::<serde_json::Value>(&body);
    let capacity = parsed
        .as_ref()
        .ok()
        .and_then(model_temporary_error_from_json)
        .or_else(|| {
            let plain_text = String::from_utf8_lossy(&body);
            model_temporary_message_matches(&plain_text).then(|| ModelTemporaryError {
                code: Some("model_temporarily_unavailable".to_string()),
                // 纯文本正文只用于内存分类，日志使用固定消息，避免保存上游响应正文。
                message: "上游报告模型或分组暂不可用".to_string(),
            })
        });
    let (code, error_type, message) = parsed
        .as_ref()
        .ok()
        .map(response_error_fields)
        .unwrap_or_default();
    let classification = if should_retry_status(status) {
        // 408、429 和 5xx 的状态码本身具有最高优先级，不依赖第三方错误正文格式。
        ErrorClassification::Retryable
    } else if status.is_client_error() {
        ErrorClassification::Permanent
    } else {
        classify_error_fields(code.as_deref(), error_type.as_deref(), message.as_deref())
    };
    let failure = StreamFailure {
        classification,
        code: code.or_else(|| Some(format!("http_{}", status.as_u16()))),
        error_type,
        message: message.unwrap_or_else(|| format!("上游返回 {status}")),
        incomplete_reason: None,
        last_sse_event: None,
        terminal_event: None,
        terminal_received: false,
        bytes_received: body.len() as u64,
        parse_error: parsed
            .err()
            .map(|error| unparsed_body_diagnostic(&response.headers, &body, &error.to_string())),
    };
    (response, capacity, failure)
}

enum SsePreflightResult {
    Forward(PreparedUpstreamResponse),
    Capacity(PreparedUpstreamResponse, ModelTemporaryError),
    Failure(PreparedUpstreamResponse, StreamFailure),
    TrackingDisabled(PreparedUpstreamResponse, StreamFailure),
}

async fn inspect_sse_before_output(
    mut response: PreparedUpstreamResponse,
    validate_responses_terminal: bool,
) -> SsePreflightResult {
    let mut pending_event = Vec::new();
    let mut total_buffered = 0_usize;
    let mut last_sse_event = None;
    while let Some(chunk) = response.stream.next().await {
        match chunk {
            Ok(chunk) => {
                total_buffered = total_buffered.saturating_add(chunk.len());
                pending_event.extend_from_slice(&chunk);
                response.prefix.push(chunk);
            }
            Err(error) => {
                let detail = sanitized_diagnostic_text(&error.to_string(), 300);
                response.preserve_stream_error(error);
                return SsePreflightResult::Failure(
                    response,
                    StreamFailure {
                        classification: ErrorClassification::Retryable,
                        code: Some("upstream_stream_interrupted".to_string()),
                        error_type: Some("stream_read_error".to_string()),
                        message: "上游流在产生业务输出前中断".to_string(),
                        incomplete_reason: None,
                        last_sse_event,
                        terminal_event: None,
                        terminal_received: false,
                        bytes_received: total_buffered as u64,
                        parse_error: Some(detail),
                    },
                );
            }
        }

        if total_buffered > MAX_SSE_PREFLIGHT_BYTES {
            return SsePreflightResult::TrackingDisabled(
                response,
                StreamFailure {
                    classification: ErrorClassification::Unknown,
                    code: Some("sse_preflight_limit_exceeded".to_string()),
                    error_type: Some("semantic_tracking_disabled".to_string()),
                    message: "输出前预检缓冲达到上限，已降级为原始透传".to_string(),
                    incomplete_reason: None,
                    last_sse_event,
                    terminal_event: None,
                    terminal_received: false,
                    bytes_received: total_buffered as u64,
                    parse_error: None,
                },
            );
        }
        loop {
            let Some(frame_end) = next_sse_frame_end(&pending_event) else {
                if pending_event.len() > MAX_SSE_EVENT_BYTES {
                    return SsePreflightResult::TrackingDisabled(
                        response,
                        StreamFailure {
                            classification: ErrorClassification::Unknown,
                            code: Some("sse_event_limit_exceeded".to_string()),
                            error_type: Some("semantic_tracking_disabled".to_string()),
                            message: "单个 SSE 事件超过语义跟踪上限，已降级为原始透传".to_string(),
                            incomplete_reason: None,
                            last_sse_event,
                            terminal_event: None,
                            terminal_received: false,
                            bytes_received: total_buffered as u64,
                            parse_error: None,
                        },
                    );
                }
                break;
            };
            if frame_end > MAX_SSE_EVENT_BYTES {
                // 完整事件可能在单个网络块内到达，同样必须执行上限检查后立刻降级为原始透传。
                return SsePreflightResult::TrackingDisabled(
                    response,
                    StreamFailure {
                        classification: ErrorClassification::Unknown,
                        code: Some("sse_event_limit_exceeded".to_string()),
                        error_type: Some("semantic_tracking_disabled".to_string()),
                        message: "单个 SSE 事件超过语义跟踪上限，已降级为原始透传".to_string(),
                        incomplete_reason: None,
                        last_sse_event,
                        terminal_event: None,
                        terminal_received: false,
                        bytes_received: total_buffered as u64,
                        parse_error: None,
                    },
                );
            }
            let frame = pending_event.drain(..frame_end).collect::<Vec<_>>();
            let Some(data) = sse_frame_data(&frame) else {
                // SSE 注释和空事件不携带业务状态，可以继续等待首个真正输出。
                continue;
            };
            if data == b"[DONE]" {
                if validate_responses_terminal {
                    return SsePreflightResult::Failure(
                        response,
                        StreamFailure {
                            classification: ErrorClassification::Retryable,
                            code: Some("upstream_stream_incomplete".to_string()),
                            error_type: Some("protocol_incomplete".to_string()),
                            message: "上游在 response.completed 前发送结束标记".to_string(),
                            incomplete_reason: None,
                            last_sse_event,
                            terminal_event: Some("[DONE]".to_string()),
                            terminal_received: false,
                            bytes_received: total_buffered as u64,
                            parse_error: None,
                        },
                    );
                }
                // Chat Completions 使用 [DONE] 正常结束，不应用 Responses 的终态规则。
                return SsePreflightResult::Forward(response);
            }
            let value = match serde_json::from_slice::<serde_json::Value>(&data) {
                Ok(value) => value,
                Err(error) => {
                    return SsePreflightResult::Failure(
                        response,
                        StreamFailure {
                            classification: ErrorClassification::Unknown,
                            code: Some("invalid_sse_json".to_string()),
                            error_type: Some("parse_error".to_string()),
                            message: "输出前收到无法解析的 SSE JSON".to_string(),
                            incomplete_reason: None,
                            last_sse_event,
                            terminal_event: None,
                            terminal_received: false,
                            bytes_received: total_buffered as u64,
                            parse_error: Some(sanitized_diagnostic_text(&error.to_string(), 300)),
                        },
                    );
                }
            };
            if let Some(capacity) = model_temporary_error_from_json(&value) {
                return SsePreflightResult::Capacity(response, capacity);
            }
            let event_type = value.get("type").and_then(|item| item.as_str());
            if let Some(event_type) = event_type {
                last_sse_event = Some(event_type.to_string());
            }
            if matches!(
                event_type,
                Some("response.created" | "response.in_progress")
            ) {
                continue;
            }
            if event_type == Some("response.completed") {
                // completed 是合法终态，不要求文本或 usage；空响应也必须按成功透传。
                return SsePreflightResult::Forward(response);
            }
            if event_type == Some("response.failed") {
                let (code, error_type, message) = response_error_fields(&value);
                return SsePreflightResult::Failure(
                    response,
                    StreamFailure {
                        classification: classify_error_fields(
                            code.as_deref(),
                            error_type.as_deref(),
                            message.as_deref(),
                        ),
                        code,
                        error_type,
                        message: message
                            .unwrap_or_else(|| "上游返回未说明原因的 response.failed".to_string()),
                        incomplete_reason: None,
                        last_sse_event: Some("response.failed".to_string()),
                        terminal_event: Some("response.failed".to_string()),
                        terminal_received: true,
                        bytes_received: total_buffered as u64,
                        parse_error: None,
                    },
                );
            }
            if event_type == Some("response.incomplete") {
                let reason = value
                    .pointer("/response/incomplete_details/reason")
                    .and_then(|item| item.as_str())
                    .map(|value| sanitized_diagnostic_text(value, 180));
                return SsePreflightResult::Failure(
                    response,
                    StreamFailure {
                        classification: classify_incomplete_reason(reason.as_deref()),
                        code: None,
                        error_type: Some("response_incomplete".to_string()),
                        message: reason
                            .as_deref()
                            .map(|reason| format!("上游返回 response.incomplete：{reason}"))
                            .unwrap_or_else(|| {
                                "上游返回未说明原因的 response.incomplete".to_string()
                            }),
                        incomplete_reason: reason,
                        last_sse_event: Some("response.incomplete".to_string()),
                        terminal_event: Some("response.incomplete".to_string()),
                        terminal_received: true,
                        bytes_received: total_buffered as u64,
                        parse_error: None,
                    },
                );
            }
            // 第一个非生命周期事件可能包含文本、推理或工具调用；此后重放将不再安全。
            return SsePreflightResult::Forward(response);
        }
    }
    SsePreflightResult::Failure(
        response,
        StreamFailure {
            classification: ErrorClassification::Retryable,
            code: Some("upstream_stream_incomplete".to_string()),
            error_type: Some("unexpected_eof".to_string()),
            message: "上游流在产生业务输出前结束".to_string(),
            incomplete_reason: None,
            last_sse_event,
            terminal_event: None,
            terminal_received: false,
            bytes_received: total_buffered as u64,
            parse_error: None,
        },
    )
}

fn local_request_body_limit_label(limit: usize) -> String {
    const MIB: usize = 1024 * 1024;
    if limit % MIB == 0 {
        format!("{} MiB（{limit} 字节）", limit / MIB)
    } else {
        format!("{limit} 字节")
    }
}

fn local_request_body_too_large_response(limit: usize) -> Response<Body> {
    let label = local_request_body_limit_label(limit);
    let body = serde_json::json!({
        "error": {
            "message": format!("AI Gateway 本地请求体超过 {label} 限制"),
            "type": "local_request_body_too_large",
            "code": "local_request_body_too_large",
            "limit_bytes": limit,
        }
    });
    Response::builder()
        .status(StatusCode::PAYLOAD_TOO_LARGE)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn is_request_body_limit_error(error: &axum::Error) -> bool {
    let mut current: Option<&(dyn StdError + 'static)> = Some(error);
    while let Some(source) = current {
        if source.is::<http_body_util::LengthLimitError>() {
            return true;
        }
        current = source.source();
    }
    false
}

async fn read_local_request_body(
    state: &AppState,
    request_path: &str,
    headers: &HeaderMap,
    body: Body,
    limit: usize,
) -> Result<Bytes, Response<Body>> {
    let declared_size = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    if let Some(size) = declared_size.filter(|size| *size > limit as u64) {
        let label = local_request_body_limit_label(limit);
        push_log(
            state,
            "warning",
            format!("本地请求体被拒绝（{request_path}）：声明 {size} 字节，超过 {label} 限制"),
            None,
        );
        return Err(local_request_body_too_large_response(limit));
    }

    match to_bytes(body, limit).await {
        Ok(body) => Ok(body),
        Err(error) if is_request_body_limit_error(&error) => {
            let label = local_request_body_limit_label(limit);
            push_log(
                state,
                "warning",
                format!("本地请求体被拒绝（{request_path}）：实际读取超过 {label} 限制"),
                None,
            );
            Err(local_request_body_too_large_response(limit))
        }
        Err(error) => {
            // 正文传输损坏属于客户端请求错误，与本地大小上限和真实上游故障都无关。
            let detail: String = error.to_string().chars().take(180).collect();
            push_log(
                state,
                "warning",
                format!("无法读取本地请求体（{request_path}）：{detail}"),
                None,
            );
            Err(json_error(StatusCode::BAD_REQUEST, "无法读取请求体"))
        }
    }
}

async fn proxy_request(State(state): State<AppState>, request: Request<Body>) -> Response<Body> {
    if !state.running.load(Ordering::Relaxed) {
        return json_error(StatusCode::SERVICE_UNAVAILABLE, "本地网关已停止");
    }
    let (parts, request_body) = request.into_parts();
    let method = parts.method;
    let uri = parts.uri;
    let headers = parts.headers;
    let token = state.settings.read().unwrap().local_token.clone();
    if !auth_valid(&headers, &token) {
        return json_error(StatusCode::UNAUTHORIZED, "本地访问令牌无效");
    }
    // 只有专用请求头能可靠证明请求来自 Codex 集成；Bearer 兼容入口可能由其他客户端使用。
    let client_origin = if headers
        .get("x-ai-gateway-token")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == token)
    {
        "codex"
    } else {
        "other"
    };
    let request_path = uri.path().to_string();
    // 鉴权完成后才允许分配大请求缓冲区，避免未授权的本机进程消耗最多 200 MiB 内存。
    let body = match read_local_request_body(
        &state,
        &request_path,
        &headers,
        request_body,
        MAX_LOCAL_REQUEST_BODY_BYTES,
    )
    .await
    {
        Ok(body) => body,
        Err(response) => return response,
    };
    let model = request_model(&body);
    let body_bytes = body.len();
    let request_id = Uuid::new_v4().to_string();
    // 请求输入只估算一次并复用于所有重试；正文仍由原始 Bytes 共享，不为统计复制大请求体。
    let request_token_estimate =
        usage_estimation::estimate_request(&body, uri.path(), model.as_deref());
    // 每个请求固定使用进入网关时的策略快照；页面保存的新规则只影响后续请求。
    let policy = state.policy.read().unwrap().clone();
    let mut candidates = state.config.read().unwrap().clone();
    candidates.sort_by_key(|provider| provider.priority);
    candidates.retain(|provider| {
        provider.enabled
            && !provider.api_key.is_empty()
            && matches_model(provider, model.as_deref())
    });
    if let Some(locked_id) = state.locked_provider_id.read().unwrap().as_ref() {
        // 手动固定只改变首选顺序；是否继续尝试备用中转由全局开关决定。
        candidates.sort_by_key(|provider| {
            if &provider.id == locked_id {
                0
            } else {
                provider.priority + 1
            }
        });
    }
    if !policy.allow_provider_failover {
        candidates.truncate(1);
    }
    let mut last_error = "没有可用的中转".to_string();
    let mut last_status = None;
    let mut only_payload_failures = true;
    let mut first_failed_provider = None;
    let mut first_failure_reason = None;
    let mut attempts = Vec::new();
    let mut upstream_attempt_index = 0_u64;
    for provider in candidates {
        let (available, payload_allowed, circuit_bypassed, half_opened) = {
            let mut health = state.health.lock().unwrap();
            let record = health.entry(provider.id.clone()).or_default();
            let was_open = record.state == HealthState::Open;
            // 自动切换关闭时仍把当前首选交给上游，避免健康缓存悄悄改变用户选择。
            let route_available = can_use(record);
            let available = !policy.allow_provider_failover || route_available;
            let payload_allowed =
                !policy.allow_provider_failover || payload_size_allowed(record, body_bytes);
            (
                available,
                payload_allowed,
                !policy.allow_provider_failover && was_open && !route_available,
                was_open && record.state == HealthState::HalfOpen,
            )
        };
        if circuit_bypassed {
            push_diagnostic_log(
                &state,
                "warning",
                "自动切换已关闭，忽略内部熔断状态并继续尝试当前中转",
                Some(&provider.id),
                model.as_deref(),
                Some("circuit_bypassed_failover_disabled"),
            );
        }
        if half_opened {
            push_diagnostic_log(
                &state,
                "info",
                "中转冷却已结束，下一次真实请求进入半开试探",
                Some(&provider.id),
                model.as_deref(),
                Some("circuit_half_open"),
            );
        }
        if !available {
            continue;
        }
        if !payload_allowed {
            let reason = format!("近期曾拒绝相同或更大的请求体（{body_bytes} 字节），跳过中转");
            first_failed_provider.get_or_insert_with(|| provider.id.clone());
            first_failure_reason.get_or_insert_with(|| reason.clone());
            last_error = reason.clone();
            last_status = Some(StatusCode::PAYLOAD_TOO_LARGE);
            attempts.push(RoutingAttempt {
                provider_id: provider.id.clone(),
                provider_name: provider.name.clone(),
                status_code: Some(StatusCode::PAYLOAD_TOO_LARGE.as_u16()),
                error: Some(reason),
                latency_ms: None,
                retry_count: 0,
                switched: true,
            });
            continue;
        }
        let url = match normalize_url(&provider.base_url, uri.path(), uri.query()) {
            Ok(value) => value,
            Err(error) => {
                last_error = error.clone();
                only_payload_failures = false;
                first_failed_provider.get_or_insert_with(|| provider.id.clone());
                first_failure_reason.get_or_insert_with(|| error.clone());
                attempts.push(RoutingAttempt {
                    provider_id: provider.id.clone(),
                    provider_name: provider.name.clone(),
                    status_code: None,
                    error: Some(error),
                    latency_ms: None,
                    retry_count: 0,
                    switched: true,
                });
                if !policy.allow_provider_failover {
                    return json_error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        &format!("当前中转地址无效：{last_error}"),
                    );
                }
                continue;
            }
        };
        let mut waited_seconds = 0_u64;
        let mut provider_failed = false;
        let mut capacity_attempts = Vec::new();
        let mut last_capacity = None;
        'transient_retry: for retry_index in 0..=policy.retry_count {
            loop {
                upstream_attempt_index = upstream_attempt_index.saturating_add(1);
                let usage_attempt_index = upstream_attempt_index;
                let started = Instant::now();
                let result = send_upstream_request(
                    upstream_request(&state, &provider, &method, url.clone(), &headers, &body),
                    provider.timeout_ms,
                )
                .await;
                let latency = started.elapsed().as_millis() as u64;
                match result {
                    Ok(response) => {
                        let prepared = PreparedUpstreamResponse::from_reqwest(response);
                        let status = prepared.status;
                        let retry_after = retry_after_seconds(&prepared.headers);
                        let upstream_id = upstream_request_id(&prepared.headers);
                        let (prepared, capacity, stream_failure, tracking_disabled, http_failure) =
                            if !status.is_success() {
                                let (response, capacity, failure) =
                                    inspect_json_error_response(prepared).await;
                                (response, capacity, None, None, Some(failure))
                            } else if prepared.is_sse() {
                                match inspect_sse_before_output(
                                    prepared,
                                    request_path == "/v1/responses",
                                )
                                .await
                                {
                                    SsePreflightResult::Forward(response) => {
                                        (response, None, None, None, None)
                                    }
                                    SsePreflightResult::Capacity(response, capacity) => {
                                        (response, Some(capacity), None, None, None)
                                    }
                                    SsePreflightResult::Failure(response, failure) => {
                                        (response, None, Some(failure), None, None)
                                    }
                                    SsePreflightResult::TrackingDisabled(response, failure) => {
                                        (response, None, None, Some(failure), None)
                                    }
                                }
                            } else {
                                (prepared, None, None, None, None)
                            };

                        if let Some(failure) = tracking_disabled {
                            let detail = stream_diagnostic_detail(
                                &request_id,
                                &provider.id,
                                model.as_deref(),
                                &request_path,
                                Some(status),
                                upstream_id.clone(),
                                &failure,
                                latency,
                                retry_index.saturating_add(1),
                                &policy,
                                None,
                                "forward_with_semantic_tracking_disabled",
                                attempts.clone(),
                                false,
                            );
                            push_diagnostic_detail_log(
                                &state,
                                "warning",
                                failure.message,
                                "sse_semantic_tracking_disabled",
                                detail,
                            );
                        }

                        if let Some(failure) = stream_failure {
                            let wait_seconds = policy.retry_interval_sec;
                            let retry_allowed = failure.classification
                                == ErrorClassification::Retryable
                                || (failure.classification == ErrorClassification::Unknown
                                    && policy.retry_unknown_transient_errors);
                            let can_retry = retry_allowed
                                && retry_budget_allows(
                                    retry_index,
                                    policy.retry_count,
                                    waited_seconds,
                                    wait_seconds,
                                    policy.max_retry_wait_sec,
                                );
                            let collected = collect_prepared_usage(
                                &prepared,
                                request_token_estimate.clone(),
                                model.clone(),
                                false,
                            );
                            persist_gateway_attempt(
                                &state,
                                gateway_usage_record(
                                    &request_id,
                                    usage_attempt_index,
                                    &provider.id,
                                    &provider.name,
                                    model.clone(),
                                    &request_path,
                                    Some(status),
                                    if can_retry {
                                        "retryable_failure"
                                    } else {
                                        "failed"
                                    },
                                    !can_retry
                                        && (!retry_allowed || !policy.allow_provider_failover),
                                    client_origin,
                                    collected,
                                ),
                            );
                            let event_type = if failure.terminal_event.as_deref()
                                == Some("response.incomplete")
                            {
                                "sse_incomplete_detected"
                            } else {
                                "sse_failure_detected"
                            };
                            let attempt = RoutingAttempt {
                                provider_id: provider.id.clone(),
                                provider_name: provider.name.clone(),
                                status_code: Some(status.as_u16()),
                                error: Some(failure.message.clone()),
                                latency_ms: Some(latency),
                                retry_count: retry_index,
                                switched: !can_retry
                                    && retry_allowed
                                    && policy.allow_provider_failover,
                            };
                            attempts.push(attempt);
                            if can_retry {
                                waited_seconds += wait_seconds;
                                let action =
                                    if failure.classification == ErrorClassification::Unknown {
                                        "retry_unknown_on_current_provider"
                                    } else {
                                        "retry_current_provider"
                                    };
                                let detail = stream_diagnostic_detail(
                                    &request_id,
                                    &provider.id,
                                    model.as_deref(),
                                    &request_path,
                                    Some(status),
                                    upstream_id.clone(),
                                    &failure,
                                    latency,
                                    retry_index.saturating_add(1),
                                    &policy,
                                    Some(wait_seconds),
                                    action,
                                    attempts.clone(),
                                    false,
                                );
                                push_diagnostic_detail_log(
                                    &state,
                                    "warning",
                                    format!(
                                        "{}，{wait_seconds} 秒后重试当前中转（{}/{})",
                                        failure.message,
                                        retry_index + 1,
                                        policy.retry_count
                                    ),
                                    if failure.classification == ErrorClassification::Unknown {
                                        "unknown_error_retry_scheduled"
                                    } else {
                                        event_type
                                    },
                                    detail,
                                );
                                sleep(StdDuration::from_secs(wait_seconds)).await;
                                continue 'transient_retry;
                            }

                            let action = if !retry_allowed {
                                "forward_without_retry"
                            } else if policy.allow_provider_failover {
                                "failover_after_retry_exhausted"
                            } else {
                                "forward_after_retry_exhausted"
                            };
                            let detail = stream_diagnostic_detail(
                                &request_id,
                                &provider.id,
                                model.as_deref(),
                                &request_path,
                                Some(status),
                                upstream_id.clone(),
                                &failure,
                                latency,
                                retry_index.saturating_add(1),
                                &policy,
                                None,
                                action,
                                attempts.clone(),
                                false,
                            );
                            push_diagnostic_detail_log(
                                &state,
                                if retry_allowed { "error" } else { "warning" },
                                if retry_allowed {
                                    format!(
                                        "输出前流式响应重试已耗尽：{}（共 {} 次尝试）",
                                        failure.message,
                                        retry_index + 1
                                    )
                                } else {
                                    format!(
                                        "输出前流式响应不满足重试条件，原样透传：{}",
                                        failure.message
                                    )
                                },
                                if failure.classification == ErrorClassification::Unknown
                                    && retry_allowed
                                {
                                    "unknown_error_retry_exhausted"
                                } else {
                                    event_type
                                },
                                detail,
                            );
                            if !retry_allowed {
                                // 永久错误、业务性 incomplete 或关闭兜底后的未知错误均保留上游原始终态。
                                if failure.classification == ErrorClassification::IncompleteBusiness
                                {
                                    record_incomplete_business_response(
                                        &state,
                                        &provider.id,
                                        latency,
                                    );
                                }
                                return response_to_axum(prepared, None);
                            }
                            let (impact, issue_kind) = stream_failure_impact(&failure);
                            match impact {
                                CircuitImpact::ProviderCircuit => {
                                    mark_provider_failure(
                                        &state,
                                        &provider.id,
                                        failure.message.clone(),
                                        issue_kind,
                                        &policy,
                                    );
                                }
                                CircuitImpact::DegradedOnly => {
                                    record_degraded_issue(
                                        &state,
                                        &provider.id,
                                        &failure.message,
                                        latency,
                                        issue_kind,
                                        None,
                                    );
                                    if issue_kind == HealthIssueKind::Model {
                                        push_diagnostic_log(
                                            &state,
                                            "warning",
                                            "当前模型或分组暂不可用，未触发整个中转熔断",
                                            Some(&provider.id),
                                            model.as_deref(),
                                            Some("model_temporarily_unavailable"),
                                        );
                                    }
                                    return response_to_axum(prepared, None);
                                }
                                CircuitImpact::None => return response_to_axum(prepared, None),
                            }
                            if !policy.allow_provider_failover {
                                return response_to_axum(prepared, None);
                            }
                            only_payload_failures = false;
                            last_status = Some(status);
                            last_error = failure.message.clone();
                            first_failed_provider.get_or_insert_with(|| provider.id.clone());
                            first_failure_reason.get_or_insert_with(|| failure.message.clone());
                            provider_failed = true;
                            break 'transient_retry;
                        }

                        if let Some(capacity) = capacity {
                            let attempt_latency = started.elapsed().as_millis() as u64;
                            let wait_seconds = retry_after.unwrap_or(policy.retry_interval_sec);
                            let can_retry = retry_budget_allows(
                                retry_index,
                                policy.retry_count,
                                waited_seconds,
                                wait_seconds,
                                policy.max_retry_wait_sec,
                            );
                            let collected = collect_prepared_usage(
                                &prepared,
                                request_token_estimate.clone(),
                                model.clone(),
                                false,
                            );
                            persist_gateway_attempt(
                                &state,
                                gateway_usage_record(
                                    &request_id,
                                    usage_attempt_index,
                                    &provider.id,
                                    &provider.name,
                                    model.clone(),
                                    &request_path,
                                    Some(status),
                                    "capacity_rejected",
                                    !can_retry,
                                    client_origin,
                                    collected,
                                ),
                            );
                            capacity_attempts.push(RoutingAttempt {
                                provider_id: provider.id.clone(),
                                provider_name: provider.name.clone(),
                                status_code: Some(status.as_u16()),
                                error: Some(capacity.message.clone()),
                                latency_ms: Some(attempt_latency),
                                retry_count: retry_index,
                                switched: false,
                            });
                            last_capacity = Some(capacity.clone());
                            if can_retry {
                                waited_seconds += wait_seconds;
                                push_diagnostic_log(
                                    &state,
                                    "warning",
                                    format!(
                                        "模型或分组暂不可用，{wait_seconds} 秒后重试同一中转（{}/{})",
                                        retry_index + 1,
                                        policy.retry_count
                                    ),
                                    Some(&provider.id),
                                    model.as_deref(),
                                    Some("retry_scheduled"),
                                );
                                sleep(StdDuration::from_secs(wait_seconds)).await;
                                continue 'transient_retry;
                            }
                            record_model_temporarily_unavailable(
                                &state,
                                &provider.id,
                                &capacity.message,
                                attempt_latency,
                            );
                            push_model_retry_event(
                                &state,
                                &provider,
                                model.as_deref(),
                                &request_path,
                                body_bytes,
                                capacity_attempts.clone(),
                                &capacity,
                                false,
                                Some(status.as_u16()),
                            );
                            push_diagnostic_log(
                                &state,
                                "error",
                                format!(
                                    "同一中转重试已耗尽：{}（共 {} 次尝试）",
                                    capacity.message,
                                    retry_index + 1
                                ),
                                Some(&provider.id),
                                model.as_deref(),
                                Some("retry_exhausted"),
                            );
                            return response_to_axum(prepared, None);
                        }

                        if !capacity_attempts.is_empty() {
                            let capacity =
                                last_capacity.as_ref().expect("模型级重试必须保留错误原因");
                            if status.is_success() {
                                capacity_attempts.push(RoutingAttempt {
                                    provider_id: provider.id.clone(),
                                    provider_name: provider.name.clone(),
                                    status_code: Some(status.as_u16()),
                                    error: None,
                                    latency_ms: Some(latency),
                                    retry_count: retry_index,
                                    switched: false,
                                });
                                push_model_retry_event(
                                    &state,
                                    &provider,
                                    model.as_deref(),
                                    &request_path,
                                    body_bytes,
                                    capacity_attempts.clone(),
                                    capacity,
                                    true,
                                    Some(status.as_u16()),
                                );
                            } else {
                                let message = format!("模型级重试后上游返回 {status}");
                                capacity_attempts.push(RoutingAttempt {
                                    provider_id: provider.id.clone(),
                                    provider_name: provider.name.clone(),
                                    status_code: Some(status.as_u16()),
                                    error: Some(message.clone()),
                                    latency_ms: Some(latency),
                                    retry_count: retry_index,
                                    switched: false,
                                });
                                record_model_temporarily_unavailable(
                                    &state,
                                    &provider.id,
                                    &message,
                                    latency,
                                );
                                push_model_retry_event(
                                    &state,
                                    &provider,
                                    model.as_deref(),
                                    &request_path,
                                    body_bytes,
                                    capacity_attempts.clone(),
                                    capacity,
                                    false,
                                    Some(status.as_u16()),
                                );
                                let collected = collect_prepared_usage(
                                    &prepared,
                                    request_token_estimate.clone(),
                                    model.clone(),
                                    false,
                                );
                                persist_gateway_attempt(
                                    &state,
                                    gateway_usage_record(
                                        &request_id,
                                        usage_attempt_index,
                                        &provider.id,
                                        &provider.name,
                                        model.clone(),
                                        &request_path,
                                        Some(status),
                                        "capacity_retry_failed",
                                        true,
                                        client_origin,
                                        collected,
                                    ),
                                );
                                return response_to_axum(prepared, None);
                            }
                        }

                        if status == StatusCode::PAYLOAD_TOO_LARGE {
                            last_status = Some(status);
                            last_error = format!("上游返回 413，拒绝 {body_bytes} 字节请求体");
                            mark_payload_too_large(
                                &state,
                                &provider.id,
                                body_bytes,
                                latency,
                                &policy,
                            );
                            attempts.push(RoutingAttempt {
                                provider_id: provider.id.clone(),
                                provider_name: provider.name.clone(),
                                status_code: Some(status.as_u16()),
                                error: Some(last_error.clone()),
                                latency_ms: Some(latency),
                                retry_count: retry_index,
                                switched: policy.payload_too_large_action
                                    == PayloadTooLargeAction::Failover,
                            });
                            let collected = collect_prepared_usage(
                                &prepared,
                                request_token_estimate.clone(),
                                model.clone(),
                                false,
                            );
                            persist_gateway_attempt(
                                &state,
                                gateway_usage_record(
                                    &request_id,
                                    usage_attempt_index,
                                    &provider.id,
                                    &provider.name,
                                    model.clone(),
                                    &request_path,
                                    Some(status),
                                    "payload_rejected",
                                    !policy.allow_provider_failover
                                        || policy.payload_too_large_action
                                            == PayloadTooLargeAction::Return,
                                    client_origin,
                                    collected,
                                ),
                            );
                            push_log_with_model(
                                &state,
                                "warning",
                                last_error.clone(),
                                Some(&provider.id),
                                model.as_deref(),
                            );
                            if !policy.allow_provider_failover
                                || policy.payload_too_large_action == PayloadTooLargeAction::Return
                            {
                                return response_to_axum(prepared, None);
                            }
                            first_failed_provider.get_or_insert_with(|| provider.id.clone());
                            first_failure_reason.get_or_insert_with(|| last_error.clone());
                            provider_failed = true;
                            break 'transient_retry;
                        }

                        if should_retry_status(status) {
                            only_payload_failures = false;
                            last_status = Some(status);
                            let failure = http_failure.clone().unwrap_or(StreamFailure {
                                classification: ErrorClassification::Retryable,
                                code: Some(format!("http_{}", status.as_u16())),
                                error_type: Some("http_error".to_string()),
                                message: format!("上游返回 {status}"),
                                incomplete_reason: None,
                                last_sse_event: None,
                                terminal_event: None,
                                terminal_received: false,
                                bytes_received: 0,
                                parse_error: None,
                            });
                            last_error = failure.message.clone();
                            let wait_seconds = if status == StatusCode::TOO_MANY_REQUESTS {
                                retry_after.unwrap_or(policy.retry_interval_sec)
                            } else {
                                policy.retry_interval_sec
                            };
                            let can_retry = retry_budget_allows(
                                retry_index,
                                policy.retry_count,
                                waited_seconds,
                                wait_seconds,
                                policy.max_retry_wait_sec,
                            );
                            attempts.push(RoutingAttempt {
                                provider_id: provider.id.clone(),
                                provider_name: provider.name.clone(),
                                status_code: Some(status.as_u16()),
                                error: Some(last_error.clone()),
                                latency_ms: Some(latency),
                                retry_count: retry_index,
                                switched: !can_retry && policy.allow_provider_failover,
                            });
                            let collected = collect_prepared_usage(
                                &prepared,
                                request_token_estimate.clone(),
                                model.clone(),
                                false,
                            );
                            persist_gateway_attempt(
                                &state,
                                gateway_usage_record(
                                    &request_id,
                                    usage_attempt_index,
                                    &provider.id,
                                    &provider.name,
                                    model.clone(),
                                    &request_path,
                                    Some(status),
                                    if can_retry {
                                        "retryable_http"
                                    } else {
                                        "http_error"
                                    },
                                    !can_retry && !policy.allow_provider_failover,
                                    client_origin,
                                    collected,
                                ),
                            );
                            if can_retry {
                                waited_seconds += wait_seconds;
                                let detail = stream_diagnostic_detail(
                                    &request_id,
                                    &provider.id,
                                    model.as_deref(),
                                    &request_path,
                                    Some(status),
                                    upstream_id.clone(),
                                    &failure,
                                    latency,
                                    retry_index.saturating_add(1),
                                    &policy,
                                    Some(wait_seconds),
                                    "retry_current_provider",
                                    attempts.clone(),
                                    false,
                                );
                                push_diagnostic_detail_log(
                                    &state,
                                    "warning",
                                    format!(
                                        "{last_error}，{wait_seconds} 秒后重试当前中转（{}/{})",
                                        retry_index + 1,
                                        policy.retry_count
                                    ),
                                    "retry_scheduled",
                                    detail,
                                );
                                sleep(StdDuration::from_secs(wait_seconds)).await;
                                continue 'transient_retry;
                            }
                            let cooldown = (status == StatusCode::TOO_MANY_REQUESTS).then(|| {
                                Utc::now()
                                    + Duration::seconds(
                                        retry_after
                                            .unwrap_or(policy.circuit_cooldown_sec)
                                            .clamp(1, 3600)
                                            as i64,
                                    )
                            });
                            let (impact, issue_kind) = http_error_impact(status, &failure);
                            match impact {
                                CircuitImpact::ProviderCircuit => {
                                    mark_provider_failure(
                                        &state,
                                        &provider.id,
                                        last_error.clone(),
                                        issue_kind,
                                        &policy,
                                    );
                                }
                                CircuitImpact::DegradedOnly => {
                                    record_degraded_issue(
                                        &state,
                                        &provider.id,
                                        &last_error,
                                        latency,
                                        issue_kind,
                                        cooldown,
                                    );
                                }
                                CircuitImpact::None => {}
                            }
                            let detail = stream_diagnostic_detail(
                                &request_id,
                                &provider.id,
                                model.as_deref(),
                                &request_path,
                                Some(status),
                                upstream_id.clone(),
                                &failure,
                                latency,
                                retry_index.saturating_add(1),
                                &policy,
                                None,
                                if policy.allow_provider_failover {
                                    "failover_after_retry_exhausted"
                                } else {
                                    "forward_after_retry_exhausted"
                                },
                                attempts.clone(),
                                false,
                            );
                            push_diagnostic_detail_log(
                                &state,
                                "error",
                                format!(
                                    "同一中转重试已耗尽：{last_error}（共 {} 次尝试）",
                                    retry_index + 1
                                ),
                                "retry_exhausted",
                                detail,
                            );
                            if !policy.allow_provider_failover {
                                push_diagnostic_log(
                                    &state,
                                    "warning",
                                    "自动切换已关闭，原样返回当前中转的最后一次响应",
                                    Some(&provider.id),
                                    model.as_deref(),
                                    Some("provider_failover_blocked"),
                                );
                                return response_to_axum(prepared, None);
                            }
                            first_failed_provider.get_or_insert_with(|| provider.id.clone());
                            first_failure_reason.get_or_insert_with(|| last_error.clone());
                            provider_failed = true;
                            break 'transient_retry;
                        }

                        if status.is_success() {
                            mark_response_started(&state, &provider.id, latency);
                            attempts.push(RoutingAttempt {
                                provider_id: provider.id.clone(),
                                provider_name: provider.name.clone(),
                                status_code: Some(status.as_u16()),
                                error: None,
                                latency_ms: Some(latency),
                                retry_count: retry_index,
                                switched: false,
                            });
                            push_log_with_model(
                                &state,
                                "info",
                                format!("已建立上游响应，响应头延迟 {latency} ms"),
                                Some(&provider.id),
                                model.as_deref(),
                            );
                            if let Some(source_provider_id) = first_failed_provider.clone() {
                                let event = RoutingEvent {
                                    id: Uuid::new_v4().to_string(),
                                    timestamp: Utc::now(),
                                    event_type: "auto_switch".to_string(),
                                    model: model.clone(),
                                    request_path: request_path.clone(),
                                    request_body_bytes: body_bytes,
                                    source_provider_id: Some(source_provider_id),
                                    target_provider_id: Some(provider.id.clone()),
                                    status_code: Some(status.as_u16()),
                                    retry_count: attempts.iter().fold(0_u64, |total, item| {
                                        total.saturating_add(item.retry_count)
                                    }),
                                    attempts: attempts.clone(),
                                    reason: first_failure_reason
                                        .clone()
                                        .unwrap_or_else(|| "首选中转不可用".to_string()),
                                    final_result: "switched".to_string(),
                                };
                                push_routing_event(
                                    &state,
                                    format!("已自动切换到 {}", provider.name),
                                    event,
                                );
                            }
                        } else {
                            // 400/401/403/404 等请求或配置错误原样返回，避免对所有中转重复发送无效请求。
                            mark_upstream_rejection(&state, &provider.id, status, latency);
                            attempts.push(RoutingAttempt {
                                provider_id: provider.id.clone(),
                                provider_name: provider.name.clone(),
                                status_code: Some(status.as_u16()),
                                error: http_failure.as_ref().map(|failure| failure.message.clone()),
                                latency_ms: Some(latency),
                                retry_count: retry_index,
                                switched: false,
                            });
                            push_log_with_model(
                                &state,
                                "error",
                                format!("上游拒绝请求：{status}"),
                                Some(&provider.id),
                                model.as_deref(),
                            );
                            if let Some(failure) = http_failure.as_ref() {
                                let detail = stream_diagnostic_detail(
                                    &request_id,
                                    &provider.id,
                                    model.as_deref(),
                                    &request_path,
                                    Some(status),
                                    upstream_id.clone(),
                                    failure,
                                    latency,
                                    retry_index.saturating_add(1),
                                    &policy,
                                    None,
                                    "forward_permanent_http_error",
                                    attempts.clone(),
                                    false,
                                );
                                push_diagnostic_detail_log(
                                    &state,
                                    "error",
                                    format!("上游拒绝请求：{}", failure.message),
                                    "http_error_detected",
                                    detail,
                                );
                            }
                            let collected = collect_prepared_usage(
                                &prepared,
                                request_token_estimate.clone(),
                                model.clone(),
                                false,
                            );
                            persist_gateway_attempt(
                                &state,
                                gateway_usage_record(
                                    &request_id,
                                    usage_attempt_index,
                                    &provider.id,
                                    &provider.name,
                                    model.clone(),
                                    &request_path,
                                    Some(status),
                                    "rejected",
                                    true,
                                    client_origin,
                                    collected,
                                ),
                            );
                        }
                        let observation = status.is_success().then(|| ResponseStreamContext {
                            state: state.clone(),
                            request_id: request_id.clone(),
                            provider_id: provider.id.clone(),
                            provider_name: provider.name.clone(),
                            model: model.clone(),
                            request_path: request_path.clone(),
                            response_status: status,
                            policy: policy.clone(),
                            usage_kind: prepared.usage_response_kind(),
                            request_estimate: request_token_estimate.clone(),
                            client_origin,
                            validate_responses_terminal: request_path == "/v1/responses"
                                && prepared.is_sse(),
                            upstream_request_id: upstream_id.clone(),
                            attempt_index: retry_index.saturating_add(1),
                            usage_attempt_index,
                            attempts: attempts.clone(),
                            started_at: Instant::now(),
                        });
                        return response_to_axum(prepared, observation);
                    }
                    Err(error) => {
                        only_payload_failures = false;
                        last_error = match error {
                            UpstreamSendError::ResponseHeaderTimeout(timeout_ms) => {
                                format!("等待上游响应头超时（{timeout_ms} ms）")
                            }
                            UpstreamSendError::Request(error) if error.is_timeout() => {
                                "连接或发送上游请求超时".to_string()
                            }
                            UpstreamSendError::Request(error) => {
                                format!("连接或发送上游请求失败：{error}")
                            }
                        };
                        let wait_seconds = policy.retry_interval_sec;
                        let can_retry = retry_budget_allows(
                            retry_index,
                            policy.retry_count,
                            waited_seconds,
                            wait_seconds,
                            policy.max_retry_wait_sec,
                        );
                        attempts.push(RoutingAttempt {
                            provider_id: provider.id.clone(),
                            provider_name: provider.name.clone(),
                            status_code: None,
                            error: Some(last_error.clone()),
                            latency_ms: Some(latency),
                            retry_count: retry_index,
                            switched: !can_retry && policy.allow_provider_failover,
                        });
                        persist_gateway_attempt(
                            &state,
                            gateway_usage_record(
                                &request_id,
                                usage_attempt_index,
                                &provider.id,
                                &provider.name,
                                model.clone(),
                                &request_path,
                                None,
                                if can_retry {
                                    "connection_retry"
                                } else {
                                    "connection_failed"
                                },
                                !can_retry && !policy.allow_provider_failover,
                                client_origin,
                                CollectedUsage {
                                    usage: CapturedUsage::default(),
                                    token_source: "none",
                                    estimate_method: None,
                                },
                            ),
                        );
                        if can_retry {
                            waited_seconds += wait_seconds;
                            push_diagnostic_log(
                                &state,
                                "warning",
                                format!(
                                    "{last_error}，{wait_seconds} 秒后重试当前中转（{}/{})",
                                    retry_index + 1,
                                    policy.retry_count
                                ),
                                Some(&provider.id),
                                model.as_deref(),
                                Some("retry_scheduled"),
                            );
                            sleep(StdDuration::from_secs(wait_seconds)).await;
                            continue 'transient_retry;
                        }
                        if !capacity_attempts.is_empty() {
                            let capacity =
                                last_capacity.as_ref().expect("模型级重试必须保留错误原因");
                            capacity_attempts.push(RoutingAttempt {
                                provider_id: provider.id.clone(),
                                provider_name: provider.name.clone(),
                                status_code: None,
                                error: Some(last_error.clone()),
                                latency_ms: Some(latency),
                                retry_count: retry_index,
                                switched: false,
                            });
                            record_model_temporarily_unavailable(
                                &state,
                                &provider.id,
                                &last_error,
                                latency,
                            );
                            push_model_retry_event(
                                &state,
                                &provider,
                                model.as_deref(),
                                &request_path,
                                body_bytes,
                                capacity_attempts.clone(),
                                capacity,
                                false,
                                None,
                            );
                            return json_error(StatusCode::SERVICE_UNAVAILABLE, &last_error);
                        }
                        mark_provider_failure(
                            &state,
                            &provider.id,
                            last_error.clone(),
                            HealthIssueKind::Transport,
                            &policy,
                        );
                        push_diagnostic_log(
                            &state,
                            "error",
                            format!(
                                "同一中转重试已耗尽：{last_error}（共 {} 次尝试）",
                                retry_index + 1
                            ),
                            Some(&provider.id),
                            model.as_deref(),
                            Some("retry_exhausted"),
                        );
                        first_failed_provider.get_or_insert_with(|| provider.id.clone());
                        first_failure_reason.get_or_insert_with(|| last_error.clone());
                        provider_failed = true;
                        break 'transient_retry;
                    }
                }
            }
        }
        if provider_failed {
            if !policy.allow_provider_failover {
                push_diagnostic_log(
                    &state,
                    "warning",
                    "自动切换已关闭，保留当前中转错误".to_string(),
                    Some(&provider.id),
                    model.as_deref(),
                    Some("provider_failover_blocked"),
                );
                return json_error(StatusCode::SERVICE_UNAVAILABLE, &last_error);
            }
            push_log_with_model(
                &state,
                "warning",
                format!("{}，准备尝试备用中转", last_error),
                Some(&provider.id),
                model.as_deref(),
            );
        }
    }
    let final_status = if only_payload_failures
        && !attempts.is_empty()
        && last_status == Some(StatusCode::PAYLOAD_TOO_LARGE)
    {
        StatusCode::PAYLOAD_TOO_LARGE
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let summary = if attempts.is_empty() {
        last_error
    } else {
        format!("所有可用中转均失败：{last_error}")
    };
    json_error(final_status, &summary)
}

struct ResponseStreamContext {
    state: AppState,
    request_id: String,
    provider_id: String,
    provider_name: String,
    model: Option<String>,
    request_path: String,
    response_status: StatusCode,
    policy: RoutingPolicy,
    usage_kind: UsageResponseKind,
    request_estimate: Option<usage_estimation::RequestTokenEstimate>,
    client_origin: &'static str,
    validate_responses_terminal: bool,
    upstream_request_id: Option<String>,
    attempt_index: u64,
    usage_attempt_index: u64,
    attempts: Vec<RoutingAttempt>,
    started_at: Instant,
}

struct SseStreamState {
    buffer: Vec<u8>,
    semantic_tracking: bool,
    started_output: bool,
    terminal_event_received: bool,
    terminal_event_type: Option<String>,
    error_code: Option<String>,
    error_type: Option<String>,
    error_message: Option<String>,
    incomplete_reason: Option<String>,
    parse_error: Option<String>,
    response_id: Option<String>,
    last_event_type: Option<String>,
    bytes_received: u64,
}

impl Default for SseStreamState {
    fn default() -> Self {
        Self {
            buffer: Vec::new(),
            semantic_tracking: true,
            started_output: false,
            terminal_event_received: false,
            terminal_event_type: None,
            error_code: None,
            error_type: None,
            error_message: None,
            incomplete_reason: None,
            parse_error: None,
            response_id: None,
            last_event_type: None,
            bytes_received: 0,
        }
    }
}

impl SseStreamState {
    fn observe(&mut self, chunk: &[u8]) {
        self.bytes_received = self.bytes_received.saturating_add(chunk.len() as u64);
        if !self.semantic_tracking {
            return;
        }
        self.buffer.extend_from_slice(chunk);
        while let Some(end) = next_sse_frame_end(&self.buffer) {
            if end > MAX_SSE_EVENT_BYTES {
                // 超大事件继续逐字节透传，但停止终态语义判断，避免观察缓冲无界增长。
                self.buffer.clear();
                self.semantic_tracking = false;
                return;
            }
            let frame = self.buffer.drain(..end).collect::<Vec<_>>();
            let Some(data) = sse_frame_data(&frame) else {
                continue;
            };
            let value = match serde_json::from_slice::<serde_json::Value>(&data) {
                Ok(value) => value,
                Err(error) => {
                    self.parse_error = Some(sanitized_diagnostic_text(&error.to_string(), 300));
                    continue;
                }
            };
            let event_type = value.get("type").and_then(|item| item.as_str());
            if let Some(event_type) = event_type {
                self.last_event_type = Some(event_type.to_string());
                if matches!(
                    event_type,
                    "response.completed" | "response.failed" | "response.incomplete"
                ) {
                    self.terminal_event_received = true;
                    self.terminal_event_type = Some(event_type.to_string());
                    if event_type == "response.failed" {
                        let (code, error_type, message) = response_error_fields(&value);
                        self.error_code = code;
                        self.error_type = error_type;
                        self.error_message = message;
                    } else if event_type == "response.incomplete" {
                        self.incomplete_reason = value
                            .pointer("/response/incomplete_details/reason")
                            .and_then(|item| item.as_str())
                            .map(|reason| sanitized_diagnostic_text(reason, 180));
                    }
                } else if !matches!(event_type, "response.created" | "response.in_progress") {
                    // 文本、推理和工具事件一旦出现，就禁止网关重放当前尝试。
                    self.started_output = true;
                }
            }
            if self.response_id.is_none() {
                self.response_id = value
                    .pointer("/response/id")
                    .or_else(|| value.get("response_id"))
                    .and_then(|item| item.as_str())
                    .map(ToOwned::to_owned);
            }
        }
        if self.buffer.len() > MAX_SSE_EVENT_BYTES {
            // 没有完整分帧的异常流也必须受到相同上限约束；降级不改变下游收到的原始字节。
            self.buffer.clear();
            self.semantic_tracking = false;
        }
    }

    fn incomplete_failure_event(&self) -> Bytes {
        incomplete_sse_failure_event(self.response_id.as_deref())
    }
}

fn incomplete_sse_failure_event(response_id: Option<&str>) -> Bytes {
    let response = if let Some(id) = response_id {
        serde_json::json!({
            "id": id,
            "status": "failed",
            "error": {
                "code": "upstream_stream_incomplete",
                "message": "上游流在 response.completed 前结束"
            }
        })
    } else {
        // 没观察到真实 response id 时不伪造标识，只提供可被客户端识别的脱敏错误。
        serde_json::json!({
            "status": "failed",
            "error": {
                "code": "upstream_stream_incomplete",
                "message": "上游流在 response.completed 前结束"
            }
        })
    };
    let data = serde_json::json!({ "type": "response.failed", "response": response });
    Bytes::from(format!("event: response.failed\ndata: {data}\n\n"))
}

struct ObservedResponseStream {
    inner: GatewayByteStream,
    context: ResponseStreamContext,
    finished: bool,
    failed: bool,
    usage: UsageCollector,
    sse: SseStreamState,
}

impl ObservedResponseStream {
    fn new(stream: GatewayByteStream, context: ResponseStreamContext) -> Self {
        let usage = UsageCollector::new(
            context.usage_kind,
            context.request_estimate.clone(),
            context.model.clone(),
        );
        Self {
            inner: stream,
            context,
            finished: false,
            failed: false,
            usage,
            sse: SseStreamState::default(),
        }
    }

    fn persist_attempt(&mut self, outcome: &'static str, allow_estimate: bool) {
        let collected = self.usage.finish(allow_estimate);
        let record = gateway_usage_record(
            &self.context.request_id,
            self.context.usage_attempt_index,
            &self.context.provider_id,
            &self.context.provider_name,
            self.context.model.clone(),
            &self.context.request_path,
            Some(self.context.response_status),
            outcome,
            true,
            self.context.client_origin,
            collected,
        );
        persist_gateway_attempt(&self.context.state, record);
    }
}

impl Stream for ObservedResponseStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                self.usage.observe(&chunk);
                if self.context.validate_responses_terminal {
                    self.sse.observe(&chunk);
                }
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(error))) => {
                if !self.failed {
                    self.failed = true;
                    let detail: String = error.to_string().chars().take(180).collect();
                    let message = format!(
                        "流式响应中途断开（{}）：{}",
                        self.context.request_path, detail
                    );
                    // 响应头之后无法迁移当前请求，但这次传输失败必须影响后续请求的健康路由。
                    mark_provider_failure(
                        &self.context.state,
                        &self.context.provider_id,
                        message.clone(),
                        HealthIssueKind::Stream,
                        &self.context.policy,
                    );
                    let failure = StreamFailure {
                        classification: ErrorClassification::Retryable,
                        code: Some("upstream_stream_interrupted".to_string()),
                        error_type: Some("stream_read_error".to_string()),
                        message: message.clone(),
                        incomplete_reason: None,
                        last_sse_event: self.sse.last_event_type.clone(),
                        terminal_event: self.sse.terminal_event_type.clone(),
                        terminal_received: self.sse.terminal_event_received,
                        bytes_received: self.sse.bytes_received,
                        parse_error: Some(detail),
                    };
                    let diagnostic = stream_diagnostic_detail(
                        &self.context.request_id,
                        &self.context.provider_id,
                        self.context.model.as_deref(),
                        &self.context.request_path,
                        None,
                        self.context.upstream_request_id.clone(),
                        &failure,
                        self.context.started_at.elapsed().as_millis() as u64,
                        self.context.attempt_index,
                        &self.context.policy,
                        None,
                        "delegate_stream_recovery_to_client",
                        self.context.attempts.clone(),
                        self.sse.started_output,
                    );
                    push_diagnostic_detail_log(
                        &self.context.state,
                        "error",
                        message,
                        "stream_retry_delegated",
                        diagnostic,
                    );
                    let allow_estimate = self.usage.saw_output() || self.sse.started_output;
                    self.persist_attempt("stream_interrupted", allow_estimate);
                }
                Poll::Ready(Some(Err(std::io::Error::other("上游流式响应中途断开"))))
            }
            Poll::Ready(None) => {
                if self.failed {
                    return Poll::Ready(None);
                }
                if self.context.validate_responses_terminal
                    && self.sse.semantic_tracking
                    && !self.sse.terminal_event_received
                {
                    self.failed = true;
                    let detail = format!(
                        "上游流在 response.completed 前结束（最后事件：{}，已接收 {} 字节）",
                        self.sse.last_event_type.as_deref().unwrap_or("无"),
                        self.sse.bytes_received
                    );
                    let message =
                        format!("流式响应不完整（{}）：{detail}", self.context.request_path);
                    mark_provider_failure(
                        &self.context.state,
                        &self.context.provider_id,
                        message.clone(),
                        HealthIssueKind::Stream,
                        &self.context.policy,
                    );
                    let failure = StreamFailure {
                        classification: ErrorClassification::Retryable,
                        code: Some("upstream_stream_incomplete".to_string()),
                        error_type: Some("unexpected_eof".to_string()),
                        message: detail,
                        incomplete_reason: None,
                        last_sse_event: self.sse.last_event_type.clone(),
                        terminal_event: None,
                        terminal_received: false,
                        bytes_received: self.sse.bytes_received,
                        parse_error: self.sse.parse_error.clone(),
                    };
                    let diagnostic = stream_diagnostic_detail(
                        &self.context.request_id,
                        &self.context.provider_id,
                        self.context.model.as_deref(),
                        &self.context.request_path,
                        None,
                        self.context.upstream_request_id.clone(),
                        &failure,
                        self.context.started_at.elapsed().as_millis() as u64,
                        self.context.attempt_index,
                        &self.context.policy,
                        None,
                        "delegate_stream_recovery_to_client",
                        self.context.attempts.clone(),
                        self.sse.started_output,
                    );
                    push_diagnostic_detail_log(
                        &self.context.state,
                        "error",
                        message,
                        "stream_retry_delegated",
                        diagnostic,
                    );
                    let allow_estimate = self.usage.saw_output() || self.sse.started_output;
                    self.persist_attempt("stream_incomplete", allow_estimate);
                    return Poll::Ready(Some(Ok(self.sse.incomplete_failure_event())));
                }
                if self.context.validate_responses_terminal
                    && self.sse.semantic_tracking
                    && matches!(
                        self.sse.terminal_event_type.as_deref(),
                        Some("response.failed" | "response.incomplete")
                    )
                {
                    self.finished = true;
                    let terminal = self
                        .sse
                        .terminal_event_type
                        .as_deref()
                        .unwrap_or("response.failed")
                        .to_string();
                    let message = format!(
                        "上游返回 {terminal}（{}，已接收 {} 字节）",
                        self.context.request_path, self.sse.bytes_received
                    );
                    let classification = if terminal == "response.incomplete" {
                        classify_incomplete_reason(self.sse.incomplete_reason.as_deref())
                    } else {
                        classify_error_fields(
                            self.sse.error_code.as_deref(),
                            self.sse.error_type.as_deref(),
                            self.sse.error_message.as_deref(),
                        )
                    };
                    let failure = StreamFailure {
                        classification,
                        code: self.sse.error_code.clone(),
                        error_type: self.sse.error_type.clone(),
                        message: self
                            .sse
                            .error_message
                            .clone()
                            .unwrap_or_else(|| message.clone()),
                        incomplete_reason: self.sse.incomplete_reason.clone(),
                        last_sse_event: self.sse.last_event_type.clone(),
                        terminal_event: self.sse.terminal_event_type.clone(),
                        terminal_received: true,
                        bytes_received: self.sse.bytes_received,
                        parse_error: self.sse.parse_error.clone(),
                    };
                    let (impact, issue_kind) = stream_failure_impact(&failure);
                    match impact {
                        CircuitImpact::ProviderCircuit => {
                            mark_provider_failure(
                                &self.context.state,
                                &self.context.provider_id,
                                message.clone(),
                                issue_kind,
                                &self.context.policy,
                            );
                        }
                        CircuitImpact::DegradedOnly => {
                            record_degraded_issue(
                                &self.context.state,
                                &self.context.provider_id,
                                &failure.message,
                                self.context.started_at.elapsed().as_millis() as u64,
                                issue_kind,
                                None,
                            );
                            if issue_kind == HealthIssueKind::Model {
                                push_diagnostic_log(
                                    &self.context.state,
                                    "warning",
                                    "当前模型或分组暂不可用，未触发整个中转熔断",
                                    Some(&self.context.provider_id),
                                    self.context.model.as_deref(),
                                    Some("model_temporarily_unavailable"),
                                );
                            }
                        }
                        CircuitImpact::None => {
                            // max_output_tokens、内容过滤和普通业务失败均不降低线路健康度。
                            record_incomplete_business_response(
                                &self.context.state,
                                &self.context.provider_id,
                                self.context.started_at.elapsed().as_millis() as u64,
                            );
                        }
                    }
                    let diagnostic = stream_diagnostic_detail(
                        &self.context.request_id,
                        &self.context.provider_id,
                        self.context.model.as_deref(),
                        &self.context.request_path,
                        None,
                        self.context.upstream_request_id.clone(),
                        &failure,
                        self.context.started_at.elapsed().as_millis() as u64,
                        self.context.attempt_index,
                        &self.context.policy,
                        None,
                        if self.sse.started_output {
                            "delegate_stream_recovery_to_client"
                        } else {
                            "forward_terminal"
                        },
                        self.context.attempts.clone(),
                        self.sse.started_output,
                    );
                    push_diagnostic_detail_log(
                        &self.context.state,
                        if classification == ErrorClassification::IncompleteBusiness {
                            "info"
                        } else {
                            "error"
                        },
                        message,
                        if terminal == "response.incomplete" {
                            "sse_incomplete_detected"
                        } else {
                            "sse_failure_detected"
                        },
                        diagnostic,
                    );
                    let allow_estimate = self.usage.saw_output() || self.sse.started_output;
                    self.persist_attempt(
                        if terminal == "response.incomplete" {
                            "incomplete"
                        } else {
                            "failed"
                        },
                        allow_estimate,
                    );
                    return Poll::Ready(None);
                }
                if !self.finished {
                    self.finished = true;
                    mark_response_completed(&self.context.state, &self.context.provider_id);
                    self.persist_attempt("completed", true);
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for ObservedResponseStream {
    fn drop(&mut self) {
        if self.finished || self.failed {
            return;
        }
        // 下游不再轮询且上游没有报错，代表客户端主动取消；释放连接但不惩罚中转健康度。
        push_log_with_model(
            &self.context.state,
            "info",
            format!(
                "客户端取消请求，已释放上游连接（{}）",
                self.context.request_path
            ),
            Some(&self.context.provider_id),
            self.context.model.as_deref(),
        );
        let allow_estimate = self.usage.saw_output() || self.sse.started_output;
        self.persist_attempt("client_cancelled", allow_estimate);
    }
}

fn response_to_axum(
    response: PreparedUpstreamResponse,
    observation: Option<ResponseStreamContext>,
) -> Response<Body> {
    let status = response.status;
    let source_headers = response.headers.clone();
    let upstream_stream = response.into_stream();
    // 所有响应仍逐块透传；成功响应额外观察完整结束、上游断流和客户端取消三种终态。
    let stream: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>> =
        match observation {
            Some(context) => Box::pin(ObservedResponseStream::new(upstream_stream, context)),
            None => upstream_stream,
        };
    let mut builder = Response::builder().status(status);
    for (name, value) in &source_headers {
        if name != header::CONTENT_LENGTH && !is_hop_by_hop_header(name) {
            builder = builder.header(name, value);
        }
    }
    builder
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| json_error(StatusCode::BAD_GATEWAY, "无法构造上游响应"))
}
fn json_error(status: StatusCode, message: &str) -> Response<Body> {
    let body =
        serde_json::json!({ "error": { "message": message, "type": "local_gateway_error" } });
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

async fn health_probe(state: AppState, provider: ProviderConfig) {
    let policy = state.policy.read().unwrap().clone();
    let url = match normalize_url(&provider.base_url, "/v1/models", None) {
        Ok(url) => url,
        Err(error) => {
            let probe_state = record_degraded_issue(
                &state,
                &provider.id,
                &error,
                0,
                HealthIssueKind::Configuration,
                None,
            );
            record_health_probe(&state, &provider.id, probe_state, None, Some(error));
            return;
        }
    };
    let started = Instant::now();
    let result = state
        .client
        .get(url)
        .header(
            header::AUTHORIZATION,
            format!("Bearer {}", provider.api_key),
        )
        .timeout(StdDuration::from_millis(provider.timeout_ms.max(1000)))
        .send()
        .await;
    let latency = started.elapsed().as_millis() as u64;
    match result {
        Ok(response) if response.status().is_success() => {
            let probe_state = mark_probe_success(&state, &provider.id, latency);
            record_health_probe(&state, &provider.id, probe_state, Some(latency), None);
        }
        Ok(response) => {
            let status = response.status();
            let error = format!("探测返回 {status}");
            let probe_state = if status.is_server_error() {
                mark_provider_failure(
                    &state,
                    &provider.id,
                    error.clone(),
                    HealthIssueKind::Upstream,
                    &policy,
                )
            } else {
                // /models 的普通 4xx 只说明探测接口或鉴权异常，不能代表模型请求线路熔断。
                record_degraded_issue(
                    &state,
                    &provider.id,
                    &error,
                    latency,
                    HealthIssueKind::Configuration,
                    None,
                )
            };
            record_health_probe(
                &state,
                &provider.id,
                probe_state,
                Some(latency),
                Some(error),
            );
        }
        Err(error) => {
            let error = error.to_string();
            let probe_state = mark_provider_failure(
                &state,
                &provider.id,
                error.clone(),
                HealthIssueKind::Transport,
                &policy,
            );
            record_health_probe(
                &state,
                &provider.id,
                probe_state,
                Some(latency),
                Some(error),
            );
        }
    }
}
async fn health_loop(state: AppState) {
    loop {
        let interval = state.settings.read().unwrap().probe_interval_sec.max(5);
        sleep(StdDuration::from_secs(interval)).await;
        // 先复制配置快照再发起网络请求，避免健康探测期间阻塞桌面端配置更新。
        let providers = state.config.read().unwrap().clone();
        for provider in providers
            .into_iter()
            .filter(|item| item.enabled && !item.api_key.is_empty())
        {
            health_probe(state.clone(), provider).await;
        }
    }
}

async fn log_cleanup_loop(state: AppState) {
    loop {
        sleep(StdDuration::from_secs(60 * 60)).await;
        let retention_days = state.settings.read().unwrap().log_retention_days;
        if let Err(error) = prune_state_logs(&state, retention_days) {
            warn!(%error, "scheduled event log cleanup failed");
        }
        let usage_retention_days = state.settings.read().unwrap().usage_retention_days;
        if let Err(error) = prune_expired_usage(&state.store, usage_retention_days) {
            warn!(%error, "scheduled usage cleanup failed");
        }
    }
}

async fn codex_config_watch_loop(state: AppState) {
    loop {
        sleep(StdDuration::from_secs(3)).await;
        let fingerprint = match codex_integration::config_fingerprint() {
            Ok(value) => value,
            Err(error) => {
                warn!(%error, "Codex config fingerprint check failed");
                continue;
            }
        };
        let changed = {
            let mut observed = state.codex_config_fingerprint.lock().unwrap();
            if observed.as_ref() == Some(&fingerprint) {
                false
            } else {
                *observed = Some(fingerprint);
                true
            }
        };
        if changed {
            // 外部写入只触发重新识别，不自动覆盖 CC-Switch 或用户选择的 Provider。
            if let Some(app) = state.app_handle.as_ref() {
                let _ = app.emit("codex-config-changed", ());
            }
        }
    }
}

async fn codex_usage_sync_loop(state: AppState) {
    loop {
        let _guard = state.codex_usage_sync_lock.lock().await;
        state.codex_usage_status.lock().unwrap().state = "syncing".to_string();
        let store = state.store.clone();
        let retention_days = state.settings.read().unwrap().usage_retention_days;
        let result =
            tokio::task::spawn_blocking(move || codex_usage::sync_current(&store, retention_days))
                .await;
        match result {
            Ok(Ok(status))
                if status.rebuild_performed
                    || status.imported_count > 0
                    || status.matched_count > 0
                    || status.removed_stale_events > 0 =>
            {
                *state.codex_usage_status.lock().unwrap() = status.clone();
                if let Some(app) = state.app_handle.as_ref() {
                    let _ = app.emit("usage-updated", &status);
                }
            }
            Ok(Ok(status)) => *state.codex_usage_status.lock().unwrap() = status,
            Ok(Err(error)) => {
                *state.codex_usage_status.lock().unwrap() = codex_usage::CodexUsageSyncStatus {
                    state: "error".to_string(),
                    warning_messages: vec![error.clone()],
                    ..Default::default()
                };
                warn!(%error, "codex usage sync failed");
            }
            Err(error) => {
                state.codex_usage_status.lock().unwrap().state = "error".to_string();
                warn!(%error, "codex usage sync worker failed");
            }
        }
        sleep(StdDuration::from_secs(15)).await;
    }
}

#[tauri::command]
fn list_providers(state: TauriState<'_, AppState>) -> Result<Vec<ProviderDto>, String> {
    Ok(state.config.read().unwrap().iter().map(dto).collect())
}
#[tauri::command]
fn get_health_snapshots(state: TauriState<'_, AppState>) -> Result<Vec<HealthSnapshot>, String> {
    let (snapshots, half_open_ids) = {
        let mut health = state
            .health
            .lock()
            .map_err(|_| "健康状态不可用".to_string())?;
        let mut half_open_ids = Vec::new();
        let snapshots = health
            .iter_mut()
            .map(|(id, record)| {
                // 查询时同步刷新冷却过期状态，避免页面一直显示为已熔断。
                if refresh_circuit_state(record) {
                    half_open_ids.push(id.clone());
                }
                health_snapshot(id, record)
            })
            .collect();
        (snapshots, half_open_ids)
    };
    for id in half_open_ids {
        push_diagnostic_log(
            &state,
            "info",
            "中转冷却已结束，等待下一次真实请求进行半开试探",
            Some(&id),
            None,
            Some("circuit_half_open"),
        );
    }
    Ok(snapshots)
}
#[tauri::command]
fn get_router_settings(state: TauriState<'_, AppState>) -> Result<RouterSettings, String> {
    Ok(state.settings.read().unwrap().clone())
}

#[tauri::command]
fn get_routing_policy(state: TauriState<'_, AppState>) -> Result<RoutingPolicy, String> {
    Ok(state.policy.read().unwrap().clone())
}

fn configuration_snapshot(state: &AppState, autostart_enabled: bool) -> ConfigurationSnapshot {
    ConfigurationSnapshot {
        router_settings: state.settings.read().unwrap().clone(),
        routing_policy: state.policy.read().unwrap().clone(),
        autostart_enabled,
    }
}

fn gateway_codex_configuration(
    settings: &RouterSettings,
) -> codex_integration::GatewayCodexConfiguration {
    codex_integration::GatewayCodexConfiguration {
        port: settings.port,
        local_token: settings.local_token.clone(),
    }
}

fn observe_codex_config_fingerprint(state: &AppState, fingerprint: String) {
    *state.codex_config_fingerprint.lock().unwrap() = Some(fingerprint);
}

fn persist_codex_management_metadata(
    state: &AppState,
    update_switched_at: bool,
) -> Result<Option<String>, String> {
    let fingerprint = codex_integration::config_fingerprint()?;
    let switched_at =
        update_switched_at.then(|| Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true));
    let mut conn = state
        .store
        .connection
        .lock()
        .map_err(|_| "数据库锁已损坏".to_string())?;
    let transaction = conn.transaction().map_err(|error| error.to_string())?;
    transaction
        .execute(
            "INSERT INTO settings(key,value) VALUES('codex_managed_config_hash',?1)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![fingerprint],
        )
        .map_err(|error| error.to_string())?;
    if let Some(value) = switched_at.as_ref() {
        transaction
            .execute(
                "INSERT INTO settings(key,value) VALUES('codex_last_switched_at',?1)
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                params![value],
            )
            .map_err(|error| error.to_string())?;
    }
    transaction.commit().map_err(|error| error.to_string())?;
    observe_codex_config_fingerprint(state, fingerprint);
    Ok(switched_at)
}

#[tauri::command]
fn get_codex_integration_state(
    state: TauriState<'_, AppState>,
) -> codex_integration::CodexIntegrationState {
    let configuration = gateway_codex_configuration(&state.settings.read().unwrap());
    let environment = state.codex_environment_status.lock().unwrap().clone();
    codex_integration::integration_state(&configuration, environment)
}

#[tauri::command]
fn refresh_codex_login_status(
    state: TauriState<'_, AppState>,
) -> codex_integration::CodexIntegrationState {
    let configuration = gateway_codex_configuration(&state.settings.read().unwrap());
    let environment = codex_integration::environment_status();
    *state.codex_environment_status.lock().unwrap() = environment.clone();
    codex_integration::integration_state(&configuration, environment)
}

#[tauri::command]
fn preview_codex_history_migration(
    state: TauriState<'_, AppState>,
) -> Result<codex_integration::CodexHistoryMigrationPreview, String> {
    let snapshot = codex_integration::scan_history()?;
    let preview = snapshot.preview.clone();
    *state.codex_history_preview.lock().unwrap() = Some(snapshot);
    Ok(preview)
}

#[tauri::command]
fn apply_codex_history_migration(
    preview_id: String,
    state: TauriState<'_, AppState>,
) -> Result<codex_integration::CodexHistoryMigrationResult, String> {
    let snapshot = state
        .codex_history_preview
        .lock()
        .unwrap()
        .take()
        .ok_or_else(|| "迁移预览已失效，请重新扫描会话".to_string())?;
    if snapshot.preview.preview_id != preview_id {
        return Err("迁移预览不匹配，请重新扫描会话".to_string());
    }
    let result = codex_integration::apply_history_migration(&snapshot)?;
    save_setting(
        &state.store,
        "codex_last_history_migration_at",
        &Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
    )?;
    push_log(
        &state,
        "info",
        "Codex 本地会话 Provider 已统一为 custom",
        None,
    );
    Ok(result)
}

#[tauri::command]
fn restore_codex_history_migration(
    state: TauriState<'_, AppState>,
) -> Result<codex_integration::CodexHistoryRestoreResult, String> {
    let result = codex_integration::restore_latest_history_migration()?;
    *state.codex_history_preview.lock().unwrap() = None;
    push_log(&state, "info", "已恢复最近一次 Codex 会话迁移", None);
    Ok(result)
}

#[tauri::command]
fn switch_codex_connection_mode(
    mode: codex_integration::CodexConnectionMode,
    state: TauriState<'_, AppState>,
) -> Result<codex_integration::SwitchCodexModeResult, String> {
    let configuration = gateway_codex_configuration(&state.settings.read().unwrap());
    let mut result = codex_integration::switch_mode(mode, &configuration)?;
    if result.changed {
        if let Err(error) = persist_codex_management_metadata(&state, true) {
            // config.toml 已完成可靠替换时不谎报切换失败；元数据可在后续修复时重建。
            result.warning_messages.push(format!(
                "Codex 模式已切换，但本地管理元数据保存失败：{error}"
            ));
        }
        push_log(
            &state,
            "info",
            match result.current_mode {
                codex_integration::CodexConnectionMode::Official => {
                    "Codex 已切换为 OpenAI 官方直连模式"
                }
                codex_integration::CodexConnectionMode::Gateway => {
                    "Codex 已切换为 AI Gateway 第三方模式"
                }
                codex_integration::CodexConnectionMode::External => unreachable!(),
            },
            None,
        );
        let mode = match result.current_mode {
            codex_integration::CodexConnectionMode::Official => "official",
            codex_integration::CodexConnectionMode::Gateway => "gateway",
            codex_integration::CodexConnectionMode::External => unreachable!(),
        };
        if let Err(error) = codex_usage::record_mode(&state.store, mode) {
            result
                .warning_messages
                .push(format!("模式已切换，但用量归属时间线保存失败：{error}"));
        }
        if let Some(app) = state.app_handle.as_ref() {
            let _ = app.emit("codex-config-changed", ());
        }
    }
    Ok(result)
}

#[tauri::command]
fn repair_codex_gateway_configuration(
    state: TauriState<'_, AppState>,
) -> Result<codex_integration::SwitchCodexModeResult, String> {
    let configuration = gateway_codex_configuration(&state.settings.read().unwrap());
    let mut result = codex_integration::repair_gateway_configuration(&configuration)?;
    if result.changed {
        if let Err(error) = persist_codex_management_metadata(&state, true) {
            result.warning_messages.push(format!(
                "Codex 配置已修复，但本地管理元数据保存失败：{error}"
            ));
        }
        push_log(&state, "info", "Codex 的 AI Gateway 接入配置已修复", None);
        if let Some(app) = state.app_handle.as_ref() {
            let _ = app.emit("codex-config-changed", ());
        }
    }
    Ok(result)
}

fn save_configuration_state(state: &AppState, input: &ConfigurationInput) -> Result<(), String> {
    validate_port(input.port)?;
    validate_probe_interval(input.probe_interval_sec)?;
    validate_log_retention_days(input.log_retention_days)?;
    validate_usage_retention_days(input.usage_retention_days)?;
    let policy = normalized_routing_policy(&input.routing_policy);
    validate_routing_policy(&policy)?;
    persist_configuration(
        &state.store,
        input.port,
        input.probe_interval_sec,
        input.log_retention_days,
        input.usage_retention_days,
        &policy,
    )?;

    // 同时持有两把写锁后再替换快照，确保事务成功前运行时永远看不到新配置。
    let mut settings = state.settings.write().unwrap();
    let mut policy = state.policy.write().unwrap();
    settings.port = input.port;
    settings.probe_interval_sec = input.probe_interval_sec;
    settings.log_retention_days = input.log_retention_days;
    settings.usage_retention_days = input.usage_retention_days;
    *policy = normalized_routing_policy(&input.routing_policy);
    drop(policy);
    drop(settings);
    // 缩短保留期限时立即清理；清理失败不回滚已提交配置，后台任务会在下一小时重试。
    if let Err(error) = prune_state_logs(state, input.log_retention_days) {
        warn!(%error, "event log cleanup after configuration save failed");
    }
    if let Err(error) = prune_expired_usage(&state.store, input.usage_retention_days) {
        warn!(%error, "usage cleanup after configuration save failed");
    }
    Ok(())
}

#[tauri::command]
fn get_configuration(
    app: tauri::AppHandle,
    state: TauriState<'_, AppState>,
) -> Result<ConfigurationSnapshot, String> {
    let autostart_enabled = app
        .autolaunch()
        .is_enabled()
        .map_err(|error| format!("无法读取开机启动状态：{error}"))?;
    Ok(configuration_snapshot(&state, autostart_enabled))
}

async fn apply_configuration(
    input: ConfigurationInput,
    app: &tauri::AppHandle,
    state: &AppState,
) -> Result<ConfigurationSnapshot, String> {
    validate_port(input.port)?;
    validate_probe_interval(input.probe_interval_sec)?;
    validate_log_retention_days(input.log_retention_days)?;
    validate_usage_retention_days(input.usage_retention_days)?;
    validate_routing_policy(&input.routing_policy)?;

    // 新端口必须在任何持久化之前完成预绑定；失败时旧监听和全部配置保持不变。
    let listener = prepare_gateway_listener(state, input.port).await?;
    let previous_settings = state.settings.read().unwrap().clone();
    let previous_port = previous_settings.port;
    let previous_codex = gateway_codex_configuration(&previous_settings);
    let next_codex = codex_integration::GatewayCodexConfiguration {
        port: input.port,
        local_token: previous_settings.local_token.clone(),
    };
    let autolaunch = app.autolaunch();
    let current_autostart = autolaunch
        .is_enabled()
        .map_err(|error| format!("无法读取开机启动状态：{error}"))?;
    let set_autostart = |enabled: bool| {
        if enabled {
            autolaunch.enable()
        } else {
            autolaunch.disable()
        }
    };
    if current_autostart != input.autostart_enabled {
        set_autostart(input.autostart_enabled)
            .map_err(|error| format!("开机启动更新失败，未保存其他配置：{error}"))?;
    }
    let actual_autostart = autolaunch
        .is_enabled()
        .map_err(|error| format!("无法确认开机启动状态，未保存其他配置：{error}"))?;
    if actual_autostart != input.autostart_enabled {
        let _ = set_autostart(current_autostart);
        return Err("开机启动状态未按预期生效，未保存其他配置".to_string());
    }

    // 第三方接管时，Codex Provider 与监听端口是一份配置。所有外部状态都先保留旧值，
    // 后续任一步失败都会补偿恢复，确保旧监听、SQLite 与 Codex 配置仍指向同一端口。
    let codex_configuration_changed = if previous_codex.port != next_codex.port {
        match codex_integration::update_managed_gateway_configuration(&previous_codex, &next_codex)
        {
            Ok(result) => result.is_some(),
            Err(error) => {
                if current_autostart != input.autostart_enabled {
                    let _ = set_autostart(current_autostart);
                }
                return Err(error);
            }
        }
    } else {
        false
    };
    if let Err(error) = save_configuration_state(state, &input) {
        if codex_configuration_changed {
            let _ = codex_integration::update_managed_gateway_configuration(
                &next_codex,
                &previous_codex,
            );
        }
        if current_autostart != input.autostart_enabled {
            let _ = set_autostart(current_autostart);
        }
        return Err(error);
    }
    if codex_configuration_changed {
        // SQLite 主配置提交成功后再记录受管指纹，避免失败事务被误认为当前有效配置。
        if let Err(error) = persist_codex_management_metadata(state, false) {
            warn!(%error, "Codex managed configuration metadata update failed");
        }
    }
    if let Some(listener) = listener {
        activate_gateway_listener(state.clone(), listener, input.port);
    }

    if previous_port != input.port {
        push_log(
            state,
            "info",
            format!(
                "本地网关已从 {DEFAULT_BIND}:{previous_port} 切换到 {DEFAULT_BIND}:{}",
                input.port
            ),
            None,
        );
    } else {
        push_log(state, "info", "配置中心设置已更新", None);
    }
    Ok(configuration_snapshot(state, actual_autostart))
}

#[tauri::command]
async fn save_configuration(
    input: ConfigurationInput,
    app: tauri::AppHandle,
    state: TauriState<'_, AppState>,
) -> Result<ConfigurationSnapshot, String> {
    apply_configuration(input, &app, &state).await
}

#[tauri::command]
fn save_routing_policy(
    policy: RoutingPolicy,
    state: TauriState<'_, AppState>,
) -> Result<RoutingPolicy, String> {
    let policy = normalized_routing_policy(&policy);
    validate_routing_policy(&policy)?;
    persist_routing_policy(&state.store, &policy)?;
    *state.policy.write().unwrap() = policy.clone();
    push_log(&state, "info", "路由错误处理规则已更新", None);
    Ok(policy)
}

#[tauri::command]
fn list_route_groups(state: TauriState<'_, AppState>) -> Result<Vec<RouteGroupDto>, String> {
    let provider_ids = state
        .config
        .read()
        .unwrap()
        .iter()
        .map(|provider| provider.id.clone())
        .collect();
    Ok(vec![RouteGroupDto {
        id: "default".to_string(),
        name: "默认路由".to_string(),
        provider_ids,
        locked_provider_id: state.locked_provider_id.read().unwrap().clone(),
    }])
}

#[tauri::command]
fn update_route_group(
    input: RouteGroupInput,
    state: TauriState<'_, AppState>,
) -> Result<RouteGroupDto, String> {
    {
        let mut providers = state.config.write().unwrap();
        for (index, id) in input.provider_ids.iter().enumerate() {
            if let Some(provider) = providers.iter_mut().find(|provider| &provider.id == id) {
                provider.priority = index as i64 + 1;
            }
        }
        providers.sort_by_key(|provider| provider.priority);
        let conn = state
            .store
            .connection
            .lock()
            .map_err(|_| "数据库锁已损坏".to_string())?;
        for provider in providers.iter() {
            conn.execute(
                "UPDATE providers SET priority=?1 WHERE id=?2",
                params![provider.priority, provider.id],
            )
            .map_err(|error| error.to_string())?;
        }
    }
    if let Some(id) = input.locked_provider_id.as_ref() {
        if !state
            .config
            .read()
            .unwrap()
            .iter()
            .any(|provider| &provider.id == id)
        {
            return Err("固定的中转不存在".to_string());
        }
    }
    let persisted_lock = input.locked_provider_id.clone().unwrap_or_default();
    save_setting(&state.store, "locked_provider_id", &persisted_lock)?;
    *state.locked_provider_id.write().unwrap() = input.locked_provider_id.clone();
    push_log(
        &state,
        "info",
        input
            .locked_provider_id
            .as_ref()
            .map(|_| "已更新首选中转")
            .unwrap_or("已恢复自动选择"),
        input.locked_provider_id.as_deref(),
    );
    Ok(RouteGroupDto {
        id: "default".to_string(),
        name: "默认路由".to_string(),
        provider_ids: state
            .config
            .read()
            .unwrap()
            .iter()
            .map(|provider| provider.id.clone())
            .collect(),
        locked_provider_id: input.locked_provider_id,
    })
}

#[tauri::command]
fn get_runtime_stats(state: TauriState<'_, AppState>) -> Result<RuntimeStats, String> {
    let health = state
        .health
        .lock()
        .map_err(|_| "健康状态不可用".to_string())?;
    let total_requests = health.values().map(|record| record.total_requests).sum();
    let successful_requests = health
        .values()
        .map(|record| record.successful_requests)
        .sum();
    let latencies: Vec<u64> = health
        .values()
        .filter_map(|record| record.latency_ms)
        .collect();
    let average_latency_ms =
        (!latencies.is_empty()).then(|| latencies.iter().sum::<u64>() / latencies.len() as u64);
    let active_provider_id = state
        .locked_provider_id
        .read()
        .unwrap()
        .clone()
        .or_else(|| {
            state
                .config
                .read()
                .unwrap()
                .iter()
                .find(|provider| provider.enabled)
                .map(|provider| provider.id.clone())
        });
    Ok(RuntimeStats {
        total_requests,
        successful_requests,
        average_latency_ms,
        active_provider_id,
    })
}

#[tauri::command]
fn get_usage_statistics(
    query: UsageQuery,
    state: TauriState<'_, AppState>,
) -> Result<UsageStatistics, String> {
    query_usage_statistics(&state, &query)
}

#[tauri::command]
async fn sync_codex_usage(
    app: tauri::AppHandle,
    state: TauriState<'_, AppState>,
) -> Result<codex_usage::CodexUsageSyncStatus, String> {
    let _guard = state.codex_usage_sync_lock.lock().await;
    state.codex_usage_status.lock().unwrap().state = "syncing".to_string();
    let store = state.store.clone();
    let retention_days = state.settings.read().unwrap().usage_retention_days;
    let status =
        tokio::task::spawn_blocking(move || codex_usage::sync_current(&store, retention_days))
            .await
            .map_err(|error| format!("Codex 用量同步任务异常结束：{error}"))??;
    let _ = app.emit("usage-updated", &status);
    *state.codex_usage_status.lock().unwrap() = status.clone();
    Ok(status)
}

#[tauri::command]
async fn rebuild_codex_usage(
    app: tauri::AppHandle,
    state: TauriState<'_, AppState>,
) -> Result<codex_usage::CodexUsageSyncStatus, String> {
    let _guard = state.codex_usage_sync_lock.lock().await;
    state.codex_usage_status.lock().unwrap().state = "syncing".to_string();
    let store = state.store.clone();
    let retention_days = state.settings.read().unwrap().usage_retention_days;
    let status = tokio::task::spawn_blocking(move || codex_usage::rebuild(&store, retention_days))
        .await
        .map_err(|error| format!("Codex 用量重建任务异常结束：{error}"))??;
    let _ = app.emit("usage-updated", &status);
    *state.codex_usage_status.lock().unwrap() = status.clone();
    Ok(status)
}

#[tauri::command]
fn get_recent_logs(state: TauriState<'_, AppState>) -> Result<Vec<EventLog>, String> {
    Ok(state
        .logs
        .lock()
        .map_err(|_| "运行日志不可用".to_string())?
        .iter()
        .rev()
        .cloned()
        .collect())
}

#[tauri::command]
fn reload_config(state: TauriState<'_, AppState>) -> Result<(), String> {
    let providers = load_providers(&state.store, state.secrets.as_ref())?;
    let policy = load_routing_policy(&state.store)?;
    *state.config.write().unwrap() = providers;
    *state.policy.write().unwrap() = policy;
    let locked = setting(&state.store, "locked_provider_id")?.filter(|value| !value.is_empty());
    *state.locked_provider_id.write().unwrap() = locked;
    push_log(&state, "info", "配置已从本地数据库重新加载", None);
    Ok(())
}
#[tauri::command]
async fn save_router_settings(
    port: u16,
    probe_interval_sec: u64,
    app: tauri::AppHandle,
    state: TauriState<'_, AppState>,
) -> Result<RouterSettings, String> {
    let current = state.settings.read().unwrap().clone();
    let routing_policy = state.policy.read().unwrap().clone();
    let autostart_enabled = app
        .autolaunch()
        .is_enabled()
        .map_err(|error| format!("无法读取开机启动状态：{error}"))?;
    let snapshot = apply_configuration(
        ConfigurationInput {
            port,
            probe_interval_sec,
            log_retention_days: current.log_retention_days,
            usage_retention_days: current.usage_retention_days,
            routing_policy,
            autostart_enabled,
        },
        &app,
        &state,
    )
    .await?;
    Ok(snapshot.router_settings)
}
#[tauri::command]
fn save_provider(
    input: ProviderInput,
    state: TauriState<'_, AppState>,
) -> Result<ProviderDto, String> {
    save_provider_inner(input, &state)
}

fn save_provider_inner(input: ProviderInput, state: &AppState) -> Result<ProviderDto, String> {
    let id = input.id.unwrap_or_else(|| Uuid::new_v4().to_string());
    let mut providers = state.config.write().unwrap();
    let old = providers.iter().find(|item| item.id == id).cloned();
    let name = input.name.trim().to_string();
    if name.is_empty() {
        return Err("中转名称不能为空".to_string());
    }
    let base_url = sanitize_base_url(&input.base_url)?;
    let timeout_ms = input.timeout_ms.unwrap_or(120000);
    if !(1000..=600000).contains(&timeout_ms) {
        return Err("请求超时必须在 1000 到 600000 毫秒之间".to_string());
    }
    let previous_secret = state
        .secrets
        .get(&id)
        .map_err(|error| format!("无法读取现有 Windows 凭据，未保存任何修改：{error}"))?;
    let submitted_secret = input
        .api_key
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.trim().to_string());
    let runtime_secret = old
        .as_ref()
        .map(|item| item.api_key.clone())
        .unwrap_or_default();

    // 留空时优先保留系统凭据；凭据缺失但旧请求仍持有内存密钥时主动修复。
    // 只有真正写过凭据才需要在数据库失败时执行补偿。
    let api_key = submitted_secret
        .clone()
        .or_else(|| previous_secret.clone().filter(|value| !value.is_empty()))
        .unwrap_or(runtime_secret);
    if api_key.is_empty() {
        return Err(old
            .and_then(|item| item.api_key_error)
            .unwrap_or_else(|| "API Key 不能为空；旧版已丢失的密钥需要重新填写一次".to_string()));
    }
    let should_write_secret = submitted_secret.is_some()
        || previous_secret
            .as_ref()
            .map(|value| value.is_empty())
            .unwrap_or(true);
    if should_write_secret {
        if let Err(error) = save_and_verify_provider_secret(state.secrets.as_ref(), &id, &api_key) {
            return match restore_provider_secret(
                state.secrets.as_ref(),
                &id,
                previous_secret.as_deref(),
            ) {
                Ok(()) => Err(format!(
                    "API Key 无法保存到 Windows 凭据管理器，原密钥已恢复：{error}"
                )),
                Err(rollback_error) => Err(format!(
                    "API Key 保存和原密钥恢复均失败：{error}；{rollback_error}"
                )),
            };
        }
    }
    let provider = ProviderConfig {
        id: id.clone(),
        name,
        base_url,
        models: input
            .models
            .into_iter()
            .map(|model| model.trim().to_string())
            .filter(|model| !model.is_empty())
            .collect(),
        priority: input.priority.unwrap_or_else(|| providers.len() as i64 + 1),
        enabled: input.enabled.unwrap_or(true),
        timeout_ms,
        api_key,
        api_key_error: None,
    };
    let models_json = serde_json::to_string(&provider.models).map_err(|error| error.to_string())?;
    let database_result = state
        .store
        .connection
        .lock()
        .map_err(|_| "数据库锁已损坏".to_string())
        .and_then(|conn| {
            conn.execute("INSERT INTO providers(id,name,base_url,models_json,priority,enabled,timeout_ms,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,datetime('now')) ON CONFLICT(id) DO UPDATE SET name=excluded.name,base_url=excluded.base_url,models_json=excluded.models_json,priority=excluded.priority,enabled=excluded.enabled,timeout_ms=excluded.timeout_ms", params![provider.id, provider.name, provider.base_url, models_json, provider.priority, provider.enabled as i64, provider.timeout_ms as i64])
                .map(|_| ())
                .map_err(|error| error.to_string())
        });
    if let Err(error) = database_result {
        if should_write_secret {
            return match restore_provider_secret(
                state.secrets.as_ref(),
                &id,
                previous_secret.as_deref(),
            ) {
                Ok(()) => Err(format!("中转配置保存失败，API Key 已恢复：{error}")),
                Err(rollback_error) => Err(format!(
                    "中转配置保存失败，且旧 API Key 无法自动恢复：{error}；{rollback_error}"
                )),
            };
        }
        return Err(format!("中转配置保存失败：{error}"));
    }
    if let Some(existing) = providers.iter_mut().find(|item| item.id == id) {
        *existing = provider.clone();
    } else {
        providers.push(provider.clone());
    }
    providers.sort_by_key(|item| item.priority);
    Ok(dto(&provider))
}
#[tauri::command]
fn delete_provider(id: String, state: TauriState<'_, AppState>) -> Result<(), String> {
    let mut providers = state.config.write().unwrap();
    providers.retain(|provider| provider.id != id);
    let conn = state
        .store
        .connection
        .lock()
        .map_err(|_| "数据库锁已损坏".to_string())?;
    conn.execute("DELETE FROM providers WHERE id=?1", params![id])
        .map_err(|e| e.to_string())?;
    drop(conn);
    // 数据库记录删除后再清理密钥；密钥删除失败只会留下不可达凭据，不会破坏路由配置。
    let _ = state.secrets.delete(&id);
    if state.locked_provider_id.read().unwrap().as_ref() == Some(&id) {
        *state.locked_provider_id.write().unwrap() = None;
        save_setting(&state.store, "locked_provider_id", "")?;
    }
    Ok(())
}
#[tauri::command]
fn set_provider_enabled(
    id: String,
    enabled: bool,
    state: TauriState<'_, AppState>,
) -> Result<(), String> {
    let mut providers = state.config.write().unwrap();
    let provider = providers
        .iter_mut()
        .find(|item| item.id == id)
        .ok_or_else(|| "中转不存在".to_string())?;
    provider.enabled = enabled;
    let conn = state
        .store
        .connection
        .lock()
        .map_err(|_| "数据库锁已损坏".to_string())?;
    conn.execute(
        "UPDATE providers SET enabled=?1 WHERE id=?2",
        params![enabled as i64, id],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}
#[tauri::command]
fn reorder_providers(ids: Vec<String>, state: TauriState<'_, AppState>) -> Result<(), String> {
    let mut providers = state.config.write().unwrap();
    for (index, id) in ids.iter().enumerate() {
        if let Some(provider) = providers.iter_mut().find(|item| &item.id == id) {
            provider.priority = index as i64 + 1;
        }
    }
    providers.sort_by_key(|item| item.priority);
    let conn = state
        .store
        .connection
        .lock()
        .map_err(|_| "数据库锁已损坏".to_string())?;
    for provider in providers.iter() {
        conn.execute(
            "UPDATE providers SET priority=?1 WHERE id=?2",
            params![provider.priority, provider.id],
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}
#[tauri::command]
async fn test_provider(
    id: String,
    state: TauriState<'_, AppState>,
) -> Result<serde_json::Value, String> {
    let provider = state
        .config
        .read()
        .unwrap()
        .iter()
        .find(|item| item.id == id)
        .cloned()
        .ok_or_else(|| "中转不存在".to_string())?;
    let url = normalize_url(&provider.base_url, "/v1/models", None)?;
    let started = Instant::now();
    let result = state
        .client
        .get(url)
        .header(
            header::AUTHORIZATION,
            format!("Bearer {}", provider.api_key),
        )
        .timeout(StdDuration::from_millis(provider.timeout_ms.max(1000)))
        .send()
        .await;
    match result {
        Ok(response) if response.status().is_success() => {
            let latency = started.elapsed().as_millis() as u64;
            mark_probe_success(&state, &id, latency);
            Ok(serde_json::json!({"ok": true, "message": "基础连接正常", "latency_ms": latency}))
        }
        Ok(response) => {
            let latency = started.elapsed().as_millis() as u64;
            let status = response.status();
            let message = format!("上游返回 {status}");
            let policy = state.policy.read().unwrap().clone();
            if status.is_server_error() {
                mark_provider_failure(
                    &state,
                    &id,
                    message.clone(),
                    HealthIssueKind::Upstream,
                    &policy,
                );
            } else {
                record_degraded_issue(
                    &state,
                    &id,
                    &message,
                    latency,
                    HealthIssueKind::Configuration,
                    None,
                );
            }
            Ok(serde_json::json!({"ok": false, "message": message}))
        }
        Err(error) => {
            let policy = state.policy.read().unwrap().clone();
            mark_provider_failure(
                &state,
                &id,
                error.to_string(),
                HealthIssueKind::Transport,
                &policy,
            );
            Ok(serde_json::json!({"ok": false, "message": error.to_string()}))
        }
    }
}

fn gateway_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/*path", any(proxy_request))
        .with_state(state)
}

async fn prepare_gateway_listener(
    state: &AppState,
    port: u16,
) -> Result<Option<tokio::net::TcpListener>, String> {
    validate_port(port)?;
    if state.gateway.runtime.lock().unwrap().port == Some(port) {
        return Ok(None);
    }
    let address = format!("{DEFAULT_BIND}:{port}");
    tokio::net::TcpListener::bind(&address)
        .await
        .map(Some)
        .map_err(|error| format!("本地端口 {address} 无法监听，可能已被占用或无权使用：{error}"))
}

fn record_gateway_bind_failure(state: &AppState, port: u16, message: String) {
    state.running.store(false, Ordering::Relaxed);
    let mut settings = state.settings.write().unwrap();
    settings.port = port;
    settings.gateway_running = false;
    settings.gateway_listening = false;
    settings.gateway_error = Some(message.clone());
    drop(settings);
    push_log(state, "error", message, None);
}

fn activate_gateway_listener(state: AppState, listener: tokio::net::TcpListener, port: u16) {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (previous_shutdown, generation) = {
        let mut runtime = state.gateway.runtime.lock().unwrap();
        runtime.generation = runtime.generation.wrapping_add(1);
        let generation = runtime.generation;
        let previous_shutdown = runtime.shutdown.replace(shutdown_tx);
        runtime.port = Some(port);
        (previous_shutdown, generation)
    };
    {
        let mut settings = state.settings.write().unwrap();
        // 端口占用导致的启动失败在用户选到可用端口后自动恢复；手动暂停状态则保持不变。
        if !settings.gateway_listening && settings.gateway_error.is_some() {
            state.running.store(true, Ordering::Relaxed);
            settings.gateway_running = true;
        }
        settings.port = port;
        settings.gateway_listening = true;
        settings.gateway_error = None;
    }

    let server_state = state.clone();
    tokio::spawn(async move {
        let result = axum::serve(listener, gateway_router(server_state.clone()))
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await;
        let is_current = {
            let mut runtime = server_state.gateway.runtime.lock().unwrap();
            if runtime.generation == generation {
                runtime.port = None;
                runtime.shutdown = None;
                true
            } else {
                false
            }
        };
        if !is_current {
            return;
        }
        let message = result.err().map(|error| format!("本地网关已停止：{error}"));
        let mut settings = server_state.settings.write().unwrap();
        settings.gateway_listening = false;
        settings.gateway_error = message.clone();
        drop(settings);
        if let Some(message) = message {
            push_log(&server_state, "error", message.clone(), None);
            error!(%message, "gateway stopped");
        }
    });

    // 新端口已经绑定并进入服务任务后才关闭旧监听；旧连接由 Axum 优雅结束。
    if let Some(shutdown) = previous_shutdown {
        let _ = shutdown.send(());
    }
    info!(address = %format!("{DEFAULT_BIND}:{port}"), "local gateway started");
}

#[tauri::command]
async fn test_local_gateway(state: TauriState<'_, AppState>) -> Result<GatewayTestResult, String> {
    let settings = state.settings.read().unwrap().clone();
    let providers = state.config.read().unwrap().clone();
    let provider_count = providers.len();
    let enabled_provider_count = providers
        .iter()
        .filter(|provider| provider.enabled && !provider.api_key.is_empty())
        .count();
    if !settings.gateway_listening {
        return Ok(GatewayTestResult {
            ok: false,
            status_code: None,
            message: settings
                .gateway_error
                .unwrap_or_else(|| "本地端口尚未开始监听".to_string()),
            provider_count,
            enabled_provider_count,
        });
    }
    if !settings.gateway_running {
        return Ok(GatewayTestResult {
            ok: false,
            status_code: Some(StatusCode::SERVICE_UNAVAILABLE.as_u16()),
            message: "本地网关已暂停".to_string(),
            provider_count,
            enabled_provider_count,
        });
    }
    if enabled_provider_count == 0 {
        return Ok(GatewayTestResult {
            ok: false,
            status_code: Some(StatusCode::SERVICE_UNAVAILABLE.as_u16()),
            message: "没有已启用且保存了 API Key 的中转".to_string(),
            provider_count,
            enabled_provider_count,
        });
    }

    // 从应用内部访问与 cc-switch 完全相同的本地入口，能够同时验证监听、令牌、路由和上游连接。
    let url = format!(
        "http://{}:{}/v1/models",
        settings.bind_address, settings.port
    );
    let response = state
        .client
        .get(url)
        .header(
            header::AUTHORIZATION,
            format!("Bearer {}", settings.local_token),
        )
        .timeout(StdDuration::from_secs(30))
        .send()
        .await
        .map_err(|error| format!("无法连接本地网关：{error}"))?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if status.is_success() {
        let model_count = serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|json| json.get("data")?.as_array().map(Vec::len))
            .unwrap_or_default();
        return Ok(GatewayTestResult {
            ok: true,
            status_code: Some(status.as_u16()),
            message: format!("本地转发正常，上游返回 {model_count} 个模型"),
            provider_count,
            enabled_provider_count,
        });
    }
    let message = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|json| {
            json.pointer("/error/message")
                .and_then(|value| value.as_str())
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| format!("本地网关返回 {status}"));
    Ok(GatewayTestResult {
        ok: false,
        status_code: Some(status.as_u16()),
        message,
        provider_count,
        enabled_provider_count,
    })
}

#[tauri::command]
fn toggle_gateway(running: bool, state: TauriState<'_, AppState>) -> Result<(), String> {
    state.running.store(running, Ordering::Relaxed);
    state.settings.write().unwrap().gateway_running = running;
    push_log(
        &state,
        "info",
        if running {
            "网关已启用"
        } else {
            "网关已暂停"
        },
        None,
    );
    Ok(())
}

pub fn run() {
    // 开发构建保留终端日志，Release 构建使用应用内的脱敏事件日志，不依赖标准输出。
    #[cfg(debug_assertions)]
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .init();

    let start_minimized = std::env::args().any(|argument| argument == "--minimized");
    tauri::Builder::default()
        // 开机启动明确传入后台参数；用户手动启动时仍会正常显示管理窗口。
        .plugin(
            tauri_plugin_autostart::Builder::new()
                .arg("--minimized")
                .build(),
        )
        .plugin(tauri_plugin_dialog::init())
        .on_window_event(|window, event| {
            if let tauri::WindowEvent::CloseRequested { api, .. } = event {
                api.prevent_close();
                let _ = window.hide();
            }
        })
        .setup(move |app| {
            let state = AppState::new(&app.handle())?;
            let gateway_state = state.clone();
            app.manage(state);
            let monitor_state = gateway_state.clone();
            let log_cleanup_state = gateway_state.clone();
            let codex_watch_state = gateway_state.clone();
            let codex_usage_state = gateway_state.clone();
            tauri::async_runtime::spawn(async move {
                health_loop(monitor_state).await;
            });
            tauri::async_runtime::spawn(async move {
                log_cleanup_loop(log_cleanup_state).await;
            });
            tauri::async_runtime::spawn(async move {
                codex_config_watch_loop(codex_watch_state).await;
            });
            // 首轮立即回填保留期内历史，后续只按游标读取变化文件；阻塞 I/O 不占用网关任务线程。
            tauri::async_runtime::spawn(async move {
                codex_usage_sync_loop(codex_usage_state).await;
            });
            tauri::async_runtime::spawn(async move {
                let port = gateway_state.settings.read().unwrap().port;
                match prepare_gateway_listener(&gateway_state, port).await {
                    Ok(Some(listener)) => {
                        activate_gateway_listener(gateway_state.clone(), listener, port)
                    }
                    Ok(None) => {}
                    Err(message) => {
                        record_gateway_bind_failure(&gateway_state, port, message.clone());
                        error!(%message, "gateway bind failed");
                    }
                }
            });
            let show = MenuItem::with_id(app, "show", "显示控制台", true, None::<&str>)?;
            let toggle = MenuItem::with_id(app, "toggle", "启用或暂停网关", true, None::<&str>)?;
            let quit = MenuItem::with_id(app, "quit", "退出", true, None::<&str>)?;
            let menu = Menu::with_items(app, &[&show, &toggle, &quit])?;
            let tray_icon = app
                .default_window_icon()
                .cloned()
                .ok_or("应用图标未加载，无法创建系统托盘")?;
            // 托盘只在这里创建一次：它同时负责菜单和动态切换提示，避免配置层再生成一个空白图标。
            TrayIconBuilder::with_id("main-tray")
                .icon(tray_icon)
                .menu(&menu)
                .tooltip("AI Gateway")
                .on_menu_event(|app, event| match event.id().as_ref() {
                    "show" => {
                        if let Some(window) = app.get_webview_window("main") {
                            let _ = window.show();
                            let _ = window.set_focus();
                        }
                    }
                    "toggle" => {
                        let state = app.state::<AppState>();
                        let running = !state.running.load(Ordering::Relaxed);
                        state.running.store(running, Ordering::Relaxed);
                        state.settings.write().unwrap().gateway_running = running;
                    }
                    "quit" => app.exit(0),
                    _ => {}
                })
                .build(app)?;

            if !start_minimized {
                // 窗口在配置中默认隐藏，初始化完成后再显示，避免开机后台启动时短暂闪现。
                if let Some(window) = app.get_webview_window("main") {
                    window.show()?;
                    window.set_focus()?;
                }
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            list_providers,
            get_health_snapshots,
            get_router_settings,
            get_routing_policy,
            get_configuration,
            save_configuration,
            get_codex_integration_state,
            switch_codex_connection_mode,
            refresh_codex_login_status,
            repair_codex_gateway_configuration,
            preview_codex_history_migration,
            apply_codex_history_migration,
            restore_codex_history_migration,
            save_routing_policy,
            list_route_groups,
            update_route_group,
            get_runtime_stats,
            get_usage_statistics,
            sync_codex_usage,
            rebuild_codex_usage,
            get_recent_logs,
            reload_config,
            save_router_settings,
            save_provider,
            delete_provider,
            set_provider_enabled,
            reorder_providers,
            test_provider,
            test_local_gateway,
            toggle_gateway,
            backup::export_configuration_backup,
            backup::inspect_configuration_backup,
            backup::import_configuration_backup
        ])
        .run(tauri::generate_context!())
        .expect("error while running AI Gateway");
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;
    use futures_util::stream;
    use http_body_util::BodyExt;
    use std::convert::Infallible;
    use std::sync::atomic::AtomicUsize;
    use tower::ServiceExt;

    struct VolatileProviderSecretStore {
        value: Mutex<Option<String>>,
    }

    impl ProviderSecretStore for VolatileProviderSecretStore {
        fn get(&self, _id: &str) -> Result<Option<String>, String> {
            // 模拟旧版 Mock 后端：写入对象销毁后，新读取对象看不到任何凭据。
            Ok(None)
        }

        fn set(&self, _id: &str, value: &str) -> Result<(), String> {
            *self.value.lock().unwrap() = Some(value.to_string());
            Ok(())
        }

        fn delete(&self, _id: &str) -> Result<(), String> {
            *self.value.lock().unwrap() = None;
            Ok(())
        }
    }

    struct OneTimeUnreadableSecretStore {
        value: Mutex<Option<String>>,
        rejected: AtomicBool,
    }

    impl ProviderSecretStore for OneTimeUnreadableSecretStore {
        fn get(&self, _id: &str) -> Result<Option<String>, String> {
            let value = self.value.lock().unwrap().clone();
            if value.as_deref() == Some("new-secret") && !self.rejected.swap(true, Ordering::SeqCst)
            {
                return Ok(None);
            }
            Ok(value)
        }

        fn set(&self, _id: &str, value: &str) -> Result<(), String> {
            *self.value.lock().unwrap() = Some(value.to_string());
            Ok(())
        }

        fn delete(&self, _id: &str) -> Result<(), String> {
            *self.value.lock().unwrap() = None;
            Ok(())
        }
    }

    fn test_provider(id: &str, base_url: String, priority: i64) -> ProviderConfig {
        ProviderConfig {
            id: id.to_string(),
            name: id.to_string(),
            base_url,
            models: Vec::new(),
            priority,
            enabled: true,
            timeout_ms: 2_000,
            api_key: "test-key".to_string(),
            api_key_error: None,
        }
    }

    fn test_state(providers: Vec<ProviderConfig>) -> AppState {
        let connection = Connection::open_in_memory().unwrap();
        AppState {
            config: Arc::new(RwLock::new(providers)),
            health: Arc::new(Mutex::new(HashMap::new())),
            settings: Arc::new(RwLock::new(RouterSettings {
                bind_address: DEFAULT_BIND.to_string(),
                port: DEFAULT_PORT,
                local_token: "local-test-token".to_string(),
                probe_interval_sec: 30,
                log_retention_days: DEFAULT_LOG_RETENTION_DAYS,
                usage_retention_days: DEFAULT_USAGE_RETENTION_DAYS,
                gateway_running: true,
                gateway_listening: true,
                gateway_error: None,
            })),
            store: Arc::new(Store {
                connection: Mutex::new(connection),
            }),
            client: Client::new(),
            running: Arc::new(AtomicBool::new(true)),
            locked_provider_id: Arc::new(RwLock::new(None)),
            logs: Arc::new(Mutex::new(VecDeque::new())),
            policy: Arc::new(RwLock::new(RoutingPolicy::default())),
            secrets: Arc::new(MemoryProviderSecretStore::default()),
            gateway: Arc::new(GatewayController::default()),
            codex_config_fingerprint: Arc::new(Mutex::new(None)),
            codex_history_preview: Arc::new(Mutex::new(None)),
            codex_environment_status: Arc::new(Mutex::new(
                codex_integration::CodexEnvironmentStatus {
                    desktop_status: codex_integration::CodexApplicationStatus::NotInstalled,
                    cli_status: codex_integration::CodexApplicationStatus::NotInstalled,
                    login_status: codex_integration::CodexLoginStatus::Unknown,
                },
            )),
            codex_usage_sync_lock: Arc::new(tokio::sync::Mutex::new(())),
            codex_usage_status: Arc::new(Mutex::new(Default::default())),
            app_handle: None,
        }
    }

    fn initialize_test_settings(state: &AppState) {
        state
            .store
            .connection
            .lock()
            .unwrap()
            .execute_batch(
                "CREATE TABLE settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE event_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL,
                    provider_id TEXT,
                    model TEXT,
                    event_type TEXT,
                    routing_event_json TEXT,
                    diagnostic_json TEXT
                );
                CREATE TABLE usage_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    completed_at TEXT NOT NULL,
                    provider_id TEXT NOT NULL,
                    provider_name TEXT NOT NULL,
                    model TEXT,
                    request_path TEXT NOT NULL,
                    input_tokens INTEGER,
                    output_tokens INTEGER,
                    cached_tokens INTEGER,
                    cache_write_tokens INTEGER,
                    reasoning_tokens INTEGER,
                    total_tokens INTEGER,
                    usage_reported INTEGER NOT NULL,
                    data_source TEXT NOT NULL DEFAULT 'gateway_response',
                    usage_origin TEXT NOT NULL DEFAULT 'gateway',
                    client_origin TEXT NOT NULL DEFAULT 'unknown',
                    codex_event_key TEXT,
                    codex_usage_supplemented INTEGER NOT NULL DEFAULT 0,
                    gateway_request_id TEXT,
                    attempt_index INTEGER NOT NULL DEFAULT 1,
                    is_final_attempt INTEGER NOT NULL DEFAULT 1,
                    http_status INTEGER,
                    outcome TEXT NOT NULL DEFAULT 'completed',
                    token_source TEXT NOT NULL DEFAULT 'none',
                    estimate_method TEXT,
                    codex_previous_usage_json TEXT
                );
                CREATE UNIQUE INDEX idx_usage_records_gateway_attempt
                    ON usage_records(gateway_request_id, provider_id, attempt_index)
                    WHERE gateway_request_id IS NOT NULL;",
            )
            .unwrap();
    }

    #[test]
    fn configured_keyring_backend_is_persistent() {
        assert!(matches!(
            default::default_credential_builder().persistence(),
            CredentialPersistence::UntilDelete
        ));
        assert!(validate_system_secret_store().is_ok());
    }

    #[test]
    fn secret_verification_rejects_non_persistent_store() {
        let secrets = VolatileProviderSecretStore {
            value: Mutex::new(None),
        };

        let error = save_and_verify_provider_secret(&secrets, "provider", "secret")
            .expect_err("回读不到刚写入的密钥时必须拒绝保存");

        assert!(error.contains("写入后未找到"));
    }

    #[test]
    fn restoring_provider_secret_reinstates_previous_value_or_deletes_new_entry() {
        let secrets = MemoryProviderSecretStore::default();
        secrets.set("provider", "new-secret").unwrap();
        restore_provider_secret(&secrets, "provider", Some("old-secret")).unwrap();
        assert_eq!(
            secrets.get("provider").unwrap().as_deref(),
            Some("old-secret")
        );

        restore_provider_secret(&secrets, "provider", None).unwrap();
        assert!(secrets.get("provider").unwrap().is_none());
    }

    #[test]
    fn provider_verification_failure_restores_previous_secret() {
        let provider = test_provider("provider", "https://example.com/v1".to_string(), 1);
        let mut state = test_state(vec![provider]);
        let secrets = Arc::new(OneTimeUnreadableSecretStore {
            value: Mutex::new(Some("old-secret".to_string())),
            rejected: AtomicBool::new(false),
        });
        state.secrets = secrets.clone();

        let error = save_provider_inner(
            ProviderInput {
                id: Some("provider".to_string()),
                name: "更新后的中转".to_string(),
                base_url: "https://example.com/v1".to_string(),
                models: Vec::new(),
                priority: Some(1),
                enabled: Some(true),
                timeout_ms: Some(120_000),
                api_key: Some("new-secret".to_string()),
            },
            &state,
        )
        .expect_err("凭据回读失败时必须取消保存");

        assert!(error.contains("原密钥已恢复"));
        assert_eq!(
            secrets.get("provider").unwrap().as_deref(),
            Some("old-secret")
        );
        assert_eq!(state.config.read().unwrap()[0].name, "provider");
    }

    #[test]
    fn provider_database_failure_restores_previous_secret() {
        let provider = test_provider("provider", "https://example.com/v1".to_string(), 1);
        let mut state = test_state(vec![provider]);
        let secrets = Arc::new(MemoryProviderSecretStore::default());
        secrets.set("provider", "old-secret").unwrap();
        state.secrets = secrets.clone();

        let error = save_provider_inner(
            ProviderInput {
                id: Some("provider".to_string()),
                name: "更新后的中转".to_string(),
                base_url: "https://example.com/v1".to_string(),
                models: Vec::new(),
                priority: Some(1),
                enabled: Some(true),
                timeout_ms: Some(120_000),
                api_key: Some("new-secret".to_string()),
            },
            &state,
        )
        .expect_err("未创建 providers 表时数据库写入必须失败");

        assert!(error.contains("API Key 已恢复"));
        assert_eq!(
            secrets.get("provider").unwrap().as_deref(),
            Some("old-secret")
        );
        assert_eq!(state.config.read().unwrap()[0].name, "provider");
    }

    #[test]
    fn new_provider_database_failure_removes_orphan_secret() {
        let mut state = test_state(Vec::new());
        let secrets = Arc::new(MemoryProviderSecretStore::default());
        state.secrets = secrets.clone();

        let error = save_provider_inner(
            ProviderInput {
                id: Some("new-provider".to_string()),
                name: "新中转".to_string(),
                base_url: "https://example.com/v1".to_string(),
                models: Vec::new(),
                priority: Some(1),
                enabled: Some(true),
                timeout_ms: Some(120_000),
                api_key: Some("new-secret".to_string()),
            },
            &state,
        )
        .expect_err("未创建 providers 表时数据库写入必须失败");

        assert!(error.contains("API Key 已恢复"));
        assert!(secrets.get("new-provider").unwrap().is_none());
        assert!(state.config.read().unwrap().is_empty());
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn windows_credential_manager_round_trips_across_new_entries() {
        let id = format!("ai-gateway-test-{}", Uuid::new_v4());
        let secret = format!("test-secret-{}", Uuid::new_v4());
        let store = SystemProviderSecretStore;

        save_and_verify_provider_secret(&store, &id, &secret)
            .expect("真实 Windows 凭据应能由新 Entry 回读");
        assert_eq!(store.get(&id).unwrap().as_deref(), Some(secret.as_str()));
        store.delete(&id).expect("测试凭据应被清理");
        assert!(store.get(&id).unwrap().is_none());
    }

    async fn mock_upstream(status: StatusCode, body: &'static str) -> String {
        mock_upstream_with_content_type(status, body, "application/json").await
    }

    async fn mock_upstream_with_content_type(
        status: StatusCode,
        body: &'static str,
        content_type: &'static str,
    ) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().fallback(any(move || async move {
            Response::builder()
                .status(status)
                .header(header::CONTENT_TYPE, content_type)
                .body(Body::from(body))
                .unwrap()
        }));
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}/v1")
    }

    async fn mock_counted_sequence(
        responses: Vec<(StatusCode, &'static str, &'static str)>,
    ) -> (String, Arc<AtomicUsize>) {
        assert!(!responses.is_empty());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let response_sequence = Arc::new(responses);
        let app = Router::new().fallback(any({
            let calls = calls.clone();
            move || {
                let calls = calls.clone();
                let responses = response_sequence.clone();
                async move {
                    let index = calls.fetch_add(1, Ordering::SeqCst);
                    // 序列耗尽后重复最后一个响应，便于验证重试上限而不让 Mock 自身报错。
                    let (status, body, content_type) = responses
                        .get(index)
                        .or_else(|| responses.last())
                        .copied()
                        .unwrap();
                    Response::builder()
                        .status(status)
                        .header(header::CONTENT_TYPE, content_type)
                        .body(Body::from(body))
                        .unwrap()
                }
            }
        }));
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{address}/v1"), calls)
    }

    async fn mock_exact_request_body(
        expected: Bytes,
    ) -> (String, Arc<AtomicUsize>, Arc<AtomicBool>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let body_matches = Arc::new(AtomicBool::new(false));
        let expected = Arc::new(expected);
        let app = Router::new().fallback(any({
            let calls = calls.clone();
            let body_matches = body_matches.clone();
            move |request: Request<Body>| {
                let calls = calls.clone();
                let body_matches = body_matches.clone();
                let expected = expected.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let received = request.into_body().collect().await.unwrap().to_bytes();
                    body_matches.store(received == *expected, Ordering::SeqCst);
                    Response::builder()
                        .status(StatusCode::OK)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(r#"{"ok":true}"#))
                        .unwrap()
                }
            }
        }));
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{address}/v1"), calls, body_matches)
    }

    async fn mock_delayed_stream(delay: StdDuration) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().fallback(any(move || async move {
            // 响应头立即建立，数据块晚于中转超时到达，用于证明超时不再覆盖响应体。
            let body = Body::from_stream(stream::once(async move {
                sleep(delay).await;
                Ok::<Bytes, Infallible>(Bytes::from_static(b"data: done\n\n"))
            }));
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/event-stream")
                .body(body)
                .unwrap()
        }));
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}/v1")
    }

    async fn mock_delayed_headers(delay: StdDuration) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().fallback(any(move || async move {
            sleep(delay).await;
            Response::builder()
                .status(StatusCode::OK)
                .body(Body::from(r#"{"provider":"slow"}"#))
                .unwrap()
        }));
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}/v1")
    }

    async fn mock_broken_stream() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().fallback(any(|| async move {
            let chunks = vec![
                Ok(Bytes::from_static(b"data: first\n\n")),
                Err(std::io::Error::other("mock upstream disconnected")),
            ];
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/event-stream")
                .body(Body::from_stream(stream::iter(chunks)))
                .unwrap()
        }));
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}/v1")
    }

    async fn mock_request_header_echo() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new().fallback(any(|headers: HeaderMap| async move {
            let encoding = headers
                .get(header::ACCEPT_ENCODING)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("missing");
            let connection = headers
                .get(header::CONNECTION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("missing");
            format!("{encoding}|{connection}")
        }));
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}/v1")
    }

    async fn call_test_gateway_request(
        state: AppState,
        body: Body,
        authorization: &str,
        content_length: Option<u64>,
        extra_headers: &[(&str, &str)],
    ) -> Response<Body> {
        let mut request = Request::builder()
            .method(Method::POST)
            .uri("/v1/responses")
            .header(header::AUTHORIZATION, authorization)
            .header(header::CONTENT_TYPE, "application/json");
        if let Some(content_length) = content_length {
            request = request.header(header::CONTENT_LENGTH, content_length);
        }
        for (name, value) in extra_headers {
            request = request.header(*name, *value);
        }
        gateway_router(state)
            .oneshot(request.body(body).unwrap())
            .await
            .unwrap()
    }

    async fn call_test_gateway_path(state: AppState, path: &str) -> Response<Body> {
        gateway_router(state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(path)
                    .header(header::AUTHORIZATION, "Bearer local-test-token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":"gpt-test","messages":[]}"#))
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    async fn call_test_gateway(state: AppState) -> Response<Body> {
        call_test_gateway_request(
            state,
            Body::from(r#"{"model":"gpt-test","input":"hello"}"#),
            "Bearer local-test-token",
            None,
            &[],
        )
        .await
    }

    async fn call_test_gateway_with_headers(
        state: AppState,
        extra_headers: &[(&str, &str)],
    ) -> Response<Body> {
        call_test_gateway_request(
            state,
            Body::from(r#"{"model":"gpt-test","input":"hello"}"#),
            "Bearer local-test-token",
            None,
            extra_headers,
        )
        .await
    }
    #[test]
    fn normalizes_v1_url_without_duplicate_path() {
        assert_eq!(
            normalize_url("https://example.test/v1", "/v1/models", None)
                .unwrap()
                .as_str(),
            "https://example.test/v1/models"
        );
    }
    #[test]
    fn sanitizes_quotes_around_provider_base_url() {
        assert_eq!(
            sanitize_base_url("  “https://api.example.test/v1”  ").unwrap(),
            "https://api.example.test/v1"
        );
    }
    #[test]
    fn rejects_non_http_provider_base_url() {
        assert!(sanitize_base_url("file:///c:/secret").is_err());
    }
    #[test]
    fn retries_only_transient_statuses() {
        assert!(should_retry_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(should_retry_status(StatusCode::BAD_GATEWAY));
        assert!(!should_retry_status(StatusCode::UNAUTHORIZED));
        assert!(!should_retry_status(StatusCode::BAD_REQUEST));
    }

    #[test]
    fn recognizes_account_concurrency_as_retryable_capacity_error() {
        let value = serde_json::json!({
            "error": {
                "code": "concurrency_limit_exceeded",
                "message": "Concurrency limit exceeded for account, please retry later"
            }
        });
        let error = model_temporary_error_from_json(&value).expect("应识别账号并发限制");
        assert_eq!(error.code.as_deref(), Some("concurrency_limit_exceeded"));
    }

    #[test]
    fn extracts_model_without_materializing_other_request_fields() {
        let body = Bytes::from(format!(
            r#"{{"model":"gpt-large","input":"{}"}}"#,
            "x".repeat(3 * 1024 * 1024)
        ));

        assert_eq!(request_model(&body).as_deref(), Some("gpt-large"));
    }

    #[tokio::test]
    async fn request_larger_than_axum_default_limit_reaches_upstream_unchanged() {
        let expected = Bytes::from(format!(
            r#"{{"model":"gpt-test","input":"{}"}}"#,
            "x".repeat(3 * 1024 * 1024)
        ));
        let (upstream, calls, body_matches) = mock_exact_request_body(expected.clone()).await;
        let state = test_state(vec![test_provider("primary", upstream, 1)]);

        let response = call_test_gateway_request(
            state,
            Body::from(expected.clone()),
            "Bearer local-test-token",
            Some(expected.len() as u64),
            &[],
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let _ = response.into_body().collect().await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(body_matches.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn declared_body_over_local_limit_is_rejected_without_routing() {
        let (upstream, calls) =
            mock_counted_sequence(vec![(StatusCode::OK, r#"{"ok":true}"#, "application/json")])
                .await;
        let state = test_state(vec![test_provider("primary", upstream, 1)]);
        initialize_test_settings(&state);

        let response = call_test_gateway_request(
            state.clone(),
            Body::from(r#"{"model":"gpt-test"}"#),
            "Bearer local-test-token",
            Some(MAX_LOCAL_REQUEST_BODY_BYTES as u64 + 1),
            &[],
        )
        .await;
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let error: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            error
                .pointer("/error/type")
                .and_then(|value| value.as_str()),
            Some("local_request_body_too_large")
        );
        assert_eq!(
            error
                .pointer("/error/limit_bytes")
                .and_then(|value| value.as_u64()),
            Some(MAX_LOCAL_REQUEST_BODY_BYTES as u64)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(state.health.lock().unwrap().is_empty());
        let logs = state.logs.lock().unwrap();
        let entry = logs.back().expect("本地 413 应记录诊断日志");
        assert!(entry.message.contains("本地请求体被拒绝"));
        assert!(entry.provider_id.is_none());
        assert!(entry.routing_event.is_none());
    }

    #[tokio::test]
    async fn streamed_body_over_limit_is_rejected_without_content_length() {
        let state = test_state(Vec::new());
        initialize_test_settings(&state);
        let body = Body::from_stream(stream::iter(vec![
            Ok::<_, Infallible>(Bytes::from_static(b"12345")),
            Ok::<_, Infallible>(Bytes::from_static(b"67890")),
        ]));

        let response = read_local_request_body(&state, "/v1/responses", &HeaderMap::new(), body, 8)
            .await
            .expect_err("实际正文超过读取上限时必须拒绝");
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let error: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            error
                .pointer("/error/limit_bytes")
                .and_then(|value| value.as_u64()),
            Some(8)
        );
    }

    #[tokio::test]
    async fn invalid_token_is_rejected_before_large_body_is_read() {
        let (upstream, calls) =
            mock_counted_sequence(vec![(StatusCode::OK, r#"{"ok":true}"#, "application/json")])
                .await;
        let state = test_state(vec![test_provider("primary", upstream, 1)]);

        let response = call_test_gateway_request(
            state.clone(),
            Body::from(r#"{"model":"gpt-test"}"#),
            "Bearer wrong-token",
            Some(MAX_LOCAL_REQUEST_BODY_BYTES as u64 + 1),
            &[],
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(state.logs.lock().unwrap().is_empty());
    }

    #[test]
    fn identifies_structured_and_plain_model_temporary_errors() {
        let exact = serde_json::json!({
            "error": {
                "code": "model_at_capacity",
                "message": "Selected model is at capacity. Please try a different model."
            }
        });
        let responses_event = serde_json::json!({
            "type": "response.failed",
            "response": {
                "error": {
                    "type": "server_error",
                    "message": "The selected model is at capacity"
                }
            }
        });
        let unrelated = serde_json::json!({
            "error": { "message": "Please try a different model configuration" }
        });
        let capacity_type = serde_json::json!({
            "error": {
                "code": "server_error",
                "type": "model_at_capacity",
                "message": "Please retry later"
            }
        });
        let unavailable_channel = serde_json::json!({
            "error": {
                "code": "no_available_channel",
                "message": "No available channel for model gpt-test under group default"
            }
        });

        assert!(model_temporary_error_from_json(&exact).is_some());
        assert!(model_temporary_error_from_json(&responses_event).is_some());
        assert!(model_temporary_error_from_json(&capacity_type).is_some());
        assert!(model_temporary_error_from_json(&unavailable_channel).is_some());
        assert!(model_temporary_message_matches(
            "No channel available for model gpt-test"
        ));
        assert!(model_temporary_error_from_json(&unrelated).is_none());
    }

    #[test]
    fn old_routing_policy_json_uses_unified_values_for_legacy_capacity_fields() {
        let old = serde_json::json!({
            "retry_count": 1,
            "retry_interval_sec": 1,
            "max_retry_wait_sec": 10,
            "circuit_failure_threshold": 3,
            "circuit_cooldown_sec": 60,
            "payload_too_large_skip_sec": 600,
            "payload_too_large_action": "failover",
            "notify_switches": true
        });
        let mut policy: RoutingPolicy = serde_json::from_value(old).unwrap();
        synchronize_legacy_retry_fields(&mut policy);
        assert_eq!(policy.capacity_retry_count, policy.retry_count);
        assert!(policy.retry_unknown_transient_errors);
        assert_eq!(
            policy.capacity_retry_interval_sec,
            policy.retry_interval_sec
        );
        assert_eq!(
            policy.capacity_max_retry_wait_sec,
            policy.max_retry_wait_sec
        );
    }
    #[test]
    fn filters_connection_specific_headers() {
        for name in [
            header::CONNECTION,
            header::TRANSFER_ENCODING,
            header::UPGRADE,
            HeaderName::from_static("keep-alive"),
            HeaderName::from_static("proxy-connection"),
            header::TE,
            header::TRAILER,
        ] {
            assert!(is_hop_by_hop_header(&name));
        }
        assert!(!is_hop_by_hop_header(&header::CONTENT_TYPE));
    }
    #[test]
    fn model_patterns_allow_prefix_wildcard() {
        let provider = ProviderConfig {
            id: "1".into(),
            name: "x".into(),
            base_url: "https://x".into(),
            models: vec!["gpt-*".into()],
            priority: 1,
            enabled: true,
            timeout_ms: 1000,
            api_key: "key".into(),
            api_key_error: None,
        };
        assert!(matches_model(&provider, Some("gpt-5")));
        assert!(!matches_model(&provider, Some("claude")));
    }

    #[tokio::test]
    async fn scheduled_probe_records_success_and_failure_results() {
        let success_upstream = mock_upstream(StatusCode::OK, r#"{"data":[]}"#).await;
        let success_provider = test_provider("probe-success", success_upstream, 1);
        let success_state = test_state(vec![success_provider.clone()]);
        health_probe(success_state.clone(), success_provider).await;

        let failure_upstream =
            mock_upstream(StatusCode::INTERNAL_SERVER_ERROR, r#"{"error":"failed"}"#).await;
        let failure_provider = test_provider("probe-failure", failure_upstream, 1);
        let failure_state = test_state(vec![failure_provider.clone()]);
        health_probe(failure_state.clone(), failure_provider).await;

        let success_health = success_state.health.lock().unwrap();
        let success = success_health.get("probe-success").unwrap();
        assert_eq!(success.recent_probes.len(), 1);
        assert_eq!(success.recent_probes[0].state, HealthState::Healthy);
        assert!(success.recent_probes[0].error.is_none());
        drop(success_health);

        let failure_health = failure_state.health.lock().unwrap();
        let failure = failure_health.get("probe-failure").unwrap();
        assert_eq!(failure.recent_probes.len(), 1);
        assert_eq!(failure.recent_probes[0].state, HealthState::Degraded);
        assert!(failure.recent_probes[0]
            .error
            .as_deref()
            .is_some_and(|error| error.contains("500")));
    }

    #[test]
    fn probe_history_is_bounded_and_ignores_regular_requests() {
        let state = test_state(Vec::new());
        mark_response_started(&state, "provider", 20);
        mark_response_completed(&state, "provider");
        assert!(state
            .health
            .lock()
            .unwrap()
            .get("provider")
            .unwrap()
            .recent_probes
            .is_empty());

        for index in 0..(MAX_HEALTH_PROBES + 3) {
            record_health_probe(
                &state,
                "provider",
                HealthState::Healthy,
                Some(index as u64),
                None,
            );
        }

        let health = state.health.lock().unwrap();
        let probes = &health.get("provider").unwrap().recent_probes;
        assert_eq!(probes.len(), MAX_HEALTH_PROBES);
        assert_eq!(probes.front().unwrap().sequence, 4);
        assert_eq!(probes.back().unwrap().sequence, 27);
    }

    #[test]
    fn payload_rejection_only_skips_same_or_larger_requests() {
        let mut record = HealthRecord {
            payload_rejected_size: Some(100),
            payload_rejected_until: Some(Utc::now() + Duration::seconds(60)),
            ..HealthRecord::default()
        };
        assert!(payload_size_allowed(&mut record, 99));
        assert!(!payload_size_allowed(&mut record, 100));
        assert!(!payload_size_allowed(&mut record, 101));
    }

    #[test]
    fn routing_policy_accepts_large_retry_count_and_unlimited_wait() {
        let policy = RoutingPolicy {
            retry_count: 1_000,
            max_retry_wait_sec: 0,
            capacity_retry_count: 1_000,
            capacity_max_retry_wait_sec: 0,
            ..RoutingPolicy::default()
        };
        assert!(validate_routing_policy(&policy).is_ok());
    }

    #[test]
    fn retry_budget_zero_wait_limit_only_uses_retry_count() {
        assert!(retry_budget_allows(999, 1_000, 86_400, 60, 0));
        assert!(!retry_budget_allows(1_000, 1_000, 86_400, 60, 0));
    }

    #[test]
    fn retry_budget_positive_wait_limit_stops_early() {
        assert!(retry_budget_allows(2, 1_000, 20, 10, 30));
        assert!(!retry_budget_allows(3, 1_000, 30, 10, 30));
    }

    #[test]
    fn capacity_event_preserves_retry_counts_above_u8_range() {
        let state = test_state(Vec::new());
        initialize_test_settings(&state);
        let provider = test_provider("primary", "http://127.0.0.1:1".to_string(), 1);
        let attempts = (0..=300)
            .map(|retry_count| RoutingAttempt {
                provider_id: provider.id.clone(),
                provider_name: provider.name.clone(),
                status_code: Some(StatusCode::TOO_MANY_REQUESTS.as_u16()),
                error: Some("capacity".to_string()),
                latency_ms: Some(1),
                retry_count,
                switched: false,
            })
            .collect();
        let capacity = ModelTemporaryError {
            code: Some("model_at_capacity".to_string()),
            message: "Selected model is at capacity".to_string(),
        };

        push_model_retry_event(
            &state,
            &provider,
            Some("gpt-test"),
            "/v1/responses",
            128,
            attempts,
            &capacity,
            false,
            Some(StatusCode::TOO_MANY_REQUESTS.as_u16()),
        );

        let logs = state.logs.lock().unwrap();
        let event = logs
            .back()
            .and_then(|entry| entry.routing_event.as_ref())
            .expect("容量事件应写入路由日志");
        assert_eq!(event.retry_count, 300);
        assert_eq!(event.attempts.last().unwrap().retry_count, 300);
    }

    #[test]
    fn configuration_rejects_invalid_probe_interval() {
        assert!(validate_probe_interval(4).is_err());
        assert!(validate_probe_interval(3601).is_err());
        assert!(validate_probe_interval(45).is_ok());
    }

    #[test]
    fn configuration_rejects_invalid_log_retention_days() {
        assert!(validate_log_retention_days(0).is_err());
        assert!(validate_log_retention_days(366).is_err());
        assert!(validate_log_retention_days(7).is_ok());
    }

    #[test]
    fn event_logs_are_restored_from_sqlite_after_memory_is_cleared() {
        let state = test_state(Vec::new());
        initialize_test_settings(&state);
        push_log_with_model(
            &state,
            "warning",
            "模型容量不足，准备重试",
            Some("primary"),
            Some("gpt-test"),
        );
        state.logs.lock().unwrap().clear();

        let restored = load_recent_logs(&state.store).unwrap();

        assert_eq!(restored.len(), 1);
        let entry = restored.back().unwrap();
        assert_eq!(entry.message, "模型容量不足，准备重试");
        assert_eq!(entry.provider_id.as_deref(), Some("primary"));
        assert_eq!(entry.model.as_deref(), Some("gpt-test"));
    }

    #[test]
    fn diagnostic_detail_is_restored_without_sensitive_body_fields() {
        let state = test_state(Vec::new());
        initialize_test_settings(&state);
        let failure = StreamFailure {
            classification: ErrorClassification::Retryable,
            code: Some("server_error".to_string()),
            error_type: Some("upstream_error".to_string()),
            message: "temporary failure".to_string(),
            incomplete_reason: None,
            last_sse_event: Some("response.failed".to_string()),
            terminal_event: Some("response.failed".to_string()),
            terminal_received: true,
            bytes_received: 128,
            parse_error: None,
        };
        let detail = stream_diagnostic_detail(
            "request-id",
            "primary",
            Some("gpt-test"),
            "/v1/responses",
            Some(StatusCode::OK),
            Some("upstream-id".to_string()),
            &failure,
            42,
            1,
            &RoutingPolicy::default(),
            Some(1),
            "retry_current_provider",
            Vec::new(),
            false,
        );
        push_diagnostic_detail_log(
            &state,
            "warning",
            "检测到临时错误",
            "sse_failure_detected",
            detail,
        );

        let restored = load_recent_logs(&state.store).unwrap();
        let detail = restored.back().unwrap().diagnostic_detail.as_ref().unwrap();
        assert_eq!(detail.error_code.as_deref(), Some("server_error"));
        assert_eq!(detail.upstream_request_id.as_deref(), Some("upstream-id"));
        let json = serde_json::to_string(detail).unwrap();
        assert!(!json.contains("authorization"));
        assert!(!json.contains("api_key"));
        assert!(!json.contains("prompt"));
    }

    #[test]
    fn expired_event_logs_are_deleted_by_retention_days() {
        let state = test_state(Vec::new());
        initialize_test_settings(&state);
        let old = EventLog {
            timestamp: Utc::now() - Duration::days(8),
            level: "info".to_string(),
            message: "过期日志".to_string(),
            provider_id: None,
            model: None,
            event_type: None,
            routing_event: None,
            diagnostic_detail: None,
        };
        let recent = EventLog {
            timestamp: Utc::now() - Duration::days(6),
            message: "保留日志".to_string(),
            ..old.clone()
        };
        persist_event_log(&state.store, &old).unwrap();
        persist_event_log(&state.store, &recent).unwrap();

        let removed = prune_expired_logs(&state.store, 7).unwrap();
        let restored = load_recent_logs(&state.store).unwrap();

        assert_eq!(removed, 1);
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].message, "保留日志");
    }

    #[test]
    fn unified_configuration_save_updates_database_and_runtime_snapshots() {
        let state = test_state(Vec::new());
        initialize_test_settings(&state);
        let input = ConfigurationInput {
            port: 9876,
            probe_interval_sec: 45,
            log_retention_days: 14,
            usage_retention_days: 365,
            routing_policy: RoutingPolicy {
                retry_count: 2,
                ..RoutingPolicy::default()
            },
            autostart_enabled: false,
        };

        save_configuration_state(&state, &input).unwrap();

        assert_eq!(state.settings.read().unwrap().probe_interval_sec, 45);
        assert_eq!(state.settings.read().unwrap().port, 9876);
        assert_eq!(state.settings.read().unwrap().log_retention_days, 14);
        assert_eq!(state.policy.read().unwrap().retry_count, 2);
        assert_eq!(
            setting(&state.store, "port").unwrap().as_deref(),
            Some("9876")
        );
        assert_eq!(
            setting(&state.store, "probe_interval_sec")
                .unwrap()
                .as_deref(),
            Some("45")
        );
        assert_eq!(
            setting(&state.store, "retry_count").unwrap().as_deref(),
            Some("2")
        );
        assert_eq!(
            setting(&state.store, "log_retention_days")
                .unwrap()
                .as_deref(),
            Some("14")
        );
    }

    #[test]
    fn configurable_port_validation_and_legacy_default_are_stable() {
        let state = test_state(Vec::new());
        initialize_test_settings(&state);

        assert_eq!(load_port(&state.store).unwrap(), DEFAULT_PORT);
        assert!(validate_port(MIN_CONFIGURABLE_PORT - 1).is_err());
        assert!(validate_port(MIN_CONFIGURABLE_PORT).is_ok());
        assert!(validate_port(u16::MAX).is_ok());

        save_setting(&state.store, "port", "9988").unwrap();
        assert_eq!(load_port(&state.store).unwrap(), 9988);
    }

    #[test]
    fn official_identity_headers_are_never_forwarded_to_third_party() {
        let provider = test_provider("primary", "https://example.com/v1".to_string(), 0);
        let state = test_state(vec![provider.clone()]);
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            "Bearer official-token".parse().unwrap(),
        );
        headers.insert(header::COOKIE, "session=official".parse().unwrap());
        headers.insert("chatgpt-account-id", "account-1".parse().unwrap());
        headers.insert("openai-organization", "org-1".parse().unwrap());
        headers.insert("openai-project", "project-1".parse().unwrap());
        headers.insert("x-openai-internal", "secret".parse().unwrap());
        headers.insert("x-chatgpt-session", "secret".parse().unwrap());
        headers.insert("x-business-header", "keep".parse().unwrap());

        let request = upstream_request(
            &state,
            &provider,
            &Method::POST,
            Url::parse("https://example.com/v1/responses").unwrap(),
            &headers,
            &Bytes::new(),
        )
        .build()
        .unwrap();

        assert_eq!(request.headers()[header::AUTHORIZATION], "Bearer test-key");
        assert!(!request.headers().contains_key(header::COOKIE));
        assert!(!request.headers().contains_key("chatgpt-account-id"));
        assert!(!request.headers().contains_key("openai-organization"));
        assert!(!request.headers().contains_key("openai-project"));
        assert!(!request.headers().contains_key("x-openai-internal"));
        assert!(!request.headers().contains_key("x-chatgpt-session"));
        assert_eq!(request.headers()["x-business-header"], "keep");
    }

    #[test]
    fn gateway_token_header_authenticates_without_bearer_token() {
        let mut headers = HeaderMap::new();
        headers.insert("x-ai-gateway-token", "local-test-token".parse().unwrap());
        assert!(auth_valid(&headers, "local-test-token"));
        assert!(!auth_valid(&headers, "different-token"));
    }

    #[test]
    fn gateway_token_header_is_never_forwarded_upstream() {
        let state = test_state(Vec::new());
        let provider = test_provider("provider-a", "http://127.0.0.1:9999".to_string(), 1);
        let mut headers = HeaderMap::new();
        headers.insert("x-ai-gateway-token", "local-test-token".parse().unwrap());
        headers.insert("x-request-id", "request-1".parse().unwrap());
        let request = upstream_request(
            &state,
            &provider,
            &Method::POST,
            Url::parse("http://127.0.0.1:9999/v1/responses").unwrap(),
            &headers,
            &Bytes::from_static(b"{}"),
        )
        .build()
        .unwrap();
        assert!(request.headers().get("x-ai-gateway-token").is_none());
        assert_eq!(request.headers().get("x-request-id").unwrap(), "request-1");
    }

    #[tokio::test]
    async fn occupied_port_is_rejected_without_changing_runtime_settings() {
        let state = test_state(Vec::new());
        let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let occupied_port = occupied.local_addr().unwrap().port();
        let previous = state.settings.read().unwrap().clone();

        let result = prepare_gateway_listener(&state, occupied_port).await;

        assert!(result.is_err());
        assert_eq!(state.settings.read().unwrap().port, previous.port);
        assert_eq!(
            state.settings.read().unwrap().gateway_listening,
            previous.gateway_listening
        );
    }

    #[tokio::test]
    async fn gateway_listener_switches_to_prebound_port() {
        let state = test_state(Vec::new());
        state.settings.write().unwrap().gateway_listening = false;
        let first = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_port = first.local_addr().unwrap().port();
        activate_gateway_listener(state.clone(), first, first_port);
        tokio::time::sleep(StdDuration::from_millis(30)).await;
        assert_eq!(
            state
                .client
                .get(format!("http://127.0.0.1:{first_port}/healthz"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );

        let second = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_port = second.local_addr().unwrap().port();
        activate_gateway_listener(state.clone(), second, second_port);
        tokio::time::sleep(StdDuration::from_millis(30)).await;

        assert_eq!(state.settings.read().unwrap().port, second_port);
        assert_eq!(
            state
                .client
                .get(format!("http://127.0.0.1:{second_port}/healthz"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        let mut old_port_closed = false;
        for _ in 0..20 {
            if tokio::net::TcpStream::connect(("127.0.0.1", first_port))
                .await
                .is_err()
            {
                old_port_closed = true;
                break;
            }
            tokio::time::sleep(StdDuration::from_millis(10)).await;
        }
        assert!(old_port_closed);

        let shutdown = { state.gateway.runtime.lock().unwrap().shutdown.take() };
        if let Some(shutdown) = shutdown {
            let _ = shutdown.send(());
        }
    }

    #[test]
    fn configuration_transaction_rolls_back_all_values_on_failure() {
        let state = test_state(Vec::new());
        initialize_test_settings(&state);
        save_setting(&state.store, "probe_interval_sec", "30").unwrap();
        state
            .store
            .connection
            .lock()
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER reject_retry_count
                 BEFORE INSERT ON settings
                 WHEN NEW.key = 'retry_count'
                 BEGIN
                   SELECT RAISE(ABORT, 'blocked');
                 END;",
            )
            .unwrap();

        let result = persist_configuration(
            &state.store,
            DEFAULT_PORT,
            45,
            DEFAULT_LOG_RETENTION_DAYS,
            DEFAULT_USAGE_RETENTION_DAYS,
            &RoutingPolicy::default(),
        );

        assert!(result.is_err());
        assert_eq!(
            setting(&state.store, "probe_interval_sec")
                .unwrap()
                .as_deref(),
            Some("30")
        );
        assert!(setting(&state.store, "retry_count").unwrap().is_none());
    }

    #[test]
    fn missing_configuration_keys_use_routing_defaults() {
        let state = test_state(Vec::new());
        initialize_test_settings(&state);

        let policy = load_routing_policy(&state.store).unwrap();

        assert_eq!(policy.retry_count, RoutingPolicy::default().retry_count);
        assert_eq!(
            policy.circuit_cooldown_sec,
            RoutingPolicy::default().circuit_cooldown_sec
        );
    }

    #[test]
    fn unified_retry_migration_preserves_unrelated_routing_preferences() {
        let state = test_state(Vec::new());
        initialize_test_settings(&state);
        save_setting(&state.store, "retry_count", "1").unwrap();
        save_setting(&state.store, "circuit_failure_threshold", "7").unwrap();
        save_setting(&state.store, "payload_too_large_action", "return").unwrap();
        save_setting(&state.store, "notify_switches", "false").unwrap();

        migrate_unified_retry_policy(&state.store).unwrap();

        assert_eq!(
            setting(&state.store, "retry_count").unwrap().as_deref(),
            Some("5")
        );
        assert_eq!(
            setting(&state.store, "allow_provider_failover")
                .unwrap()
                .as_deref(),
            Some("false")
        );
        assert_eq!(
            setting(&state.store, "circuit_failure_threshold")
                .unwrap()
                .as_deref(),
            Some("7")
        );
        assert_eq!(
            setting(&state.store, "payload_too_large_action")
                .unwrap()
                .as_deref(),
            Some("return")
        );
        assert_eq!(
            setting(&state.store, "notify_switches").unwrap().as_deref(),
            Some("false")
        );
    }

    #[tokio::test]
    async fn retries_backup_provider_after_server_error() {
        let primary =
            mock_upstream(StatusCode::INTERNAL_SERVER_ERROR, r#"{"error":"failed"}"#).await;
        let backup = mock_upstream(StatusCode::OK, r#"{"provider":"backup"}"#).await;
        let state = test_state(vec![
            test_provider("primary", primary, 1),
            test_provider("backup", backup, 2),
        ]);
        state.policy.write().unwrap().allow_provider_failover = true;
        let response = call_test_gateway(state).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, Bytes::from_static(br#"{"provider":"backup"}"#));
    }

    #[tokio::test]
    async fn default_policy_never_calls_backup_provider() {
        let (primary, primary_calls) = mock_counted_sequence(vec![(
            StatusCode::SERVICE_UNAVAILABLE,
            r#"{"error":"failed"}"#,
            "application/json",
        )])
        .await;
        let (backup, backup_calls) = mock_counted_sequence(vec![(
            StatusCode::OK,
            r#"{"provider":"backup"}"#,
            "application/json",
        )])
        .await;
        let state = test_state(vec![
            test_provider("primary", primary, 1),
            test_provider("backup", backup, 2),
        ]);
        state.policy.write().unwrap().retry_count = 1;

        let response = call_test_gateway(state).await;

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(primary_calls.load(Ordering::SeqCst), 2);
        assert_eq!(backup_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn capacity_error_retries_only_original_provider_and_recovers() {
        let capacity_body = r#"{"error":{"code":"model_at_capacity","message":"Selected model is at capacity. Please try a different model."}}"#;
        let (primary, primary_calls) = mock_counted_sequence(vec![
            (
                StatusCode::SERVICE_UNAVAILABLE,
                capacity_body,
                "application/json",
            ),
            (
                StatusCode::OK,
                r#"{"provider":"primary"}"#,
                "application/json",
            ),
        ])
        .await;
        let (backup, backup_calls) = mock_counted_sequence(vec![(
            StatusCode::OK,
            r#"{"provider":"backup"}"#,
            "application/json",
        )])
        .await;
        let state = test_state(vec![
            test_provider("primary", primary, 1),
            test_provider("backup", backup, 2),
        ]);
        {
            let mut policy = state.policy.write().unwrap();
            policy.retry_count = 1;
            policy.retry_interval_sec = 1;
        }

        let response = call_test_gateway(state.clone()).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, Bytes::from_static(br#"{"provider":"primary"}"#));
        assert_eq!(primary_calls.load(Ordering::SeqCst), 2);
        assert_eq!(backup_calls.load(Ordering::SeqCst), 0);
        assert!(state
            .logs
            .lock()
            .unwrap()
            .iter()
            .any(|entry| { entry.event_type.as_deref() == Some("model_availability_recovered") }));
    }

    #[tokio::test]
    async fn exhausted_capacity_retry_preserves_response_without_failover() {
        let capacity_body = r#"{"error":{"code":"model_at_capacity","message":"Selected model is at capacity. Please try a different model."}}"#;
        let (primary, primary_calls) = mock_counted_sequence(vec![(
            StatusCode::BAD_REQUEST,
            capacity_body,
            "application/json",
        )])
        .await;
        let (backup, backup_calls) = mock_counted_sequence(vec![(
            StatusCode::OK,
            r#"{"provider":"backup"}"#,
            "application/json",
        )])
        .await;
        let state = test_state(vec![
            test_provider("primary", primary, 1),
            test_provider("backup", backup, 2),
        ]);
        {
            let mut policy = state.policy.write().unwrap();
            policy.retry_count = 1;
            policy.retry_interval_sec = 1;
        }

        let response = call_test_gateway(state.clone()).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, Bytes::from_static(capacity_body.as_bytes()));
        assert_eq!(primary_calls.load(Ordering::SeqCst), 2);
        assert_eq!(backup_calls.load(Ordering::SeqCst), 0);
        let health = state.health.lock().unwrap();
        let record = health.get("primary").unwrap();
        assert_eq!(record.consecutive_failures, 0);
        assert_ne!(record.state, HealthState::Open);
    }

    #[tokio::test]
    async fn account_concurrency_uses_global_retry_without_failover() {
        let concurrency_body = r#"{"error":{"code":"account_concurrency_limit","message":"Concurrency limit exceeded for account, please retry later"}}"#;
        let (primary, primary_calls) = mock_counted_sequence(vec![
            (
                StatusCode::TOO_MANY_REQUESTS,
                concurrency_body,
                "application/json",
            ),
            (
                StatusCode::OK,
                r#"{"provider":"primary"}"#,
                "application/json",
            ),
        ])
        .await;
        let (backup, backup_calls) = mock_counted_sequence(vec![(
            StatusCode::OK,
            r#"{"provider":"backup"}"#,
            "application/json",
        )])
        .await;
        let state = test_state(vec![
            test_provider("primary", primary, 1),
            test_provider("backup", backup, 2),
        ]);
        {
            let mut policy = state.policy.write().unwrap();
            policy.retry_count = 1;
            policy.retry_interval_sec = 1;
            policy.allow_provider_failover = true;
        }

        let response = call_test_gateway(state).await;
        let body = response.into_body().collect().await.unwrap().to_bytes();

        assert_eq!(body, Bytes::from_static(br#"{"provider":"primary"}"#));
        assert_eq!(primary_calls.load(Ordering::SeqCst), 2);
        assert_eq!(backup_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn sse_capacity_before_output_retries_without_mixing_response_ids() {
        let first = concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"failed-id\"}}\r\n\r\n",
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"model_at_capacity\",\"message\":\"Selected model is at capacity\"}}}\r\n\r\n"
        );
        let second = concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"success-id\"}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"success-id\"}}\n\n"
        );
        let (primary, calls) = mock_counted_sequence(vec![
            (StatusCode::OK, first, "text/event-stream"),
            (StatusCode::OK, second, "text/event-stream"),
        ])
        .await;
        let state = test_state(vec![test_provider("primary", primary, 1)]);
        {
            let mut policy = state.policy.write().unwrap();
            policy.retry_count = 1;
            policy.retry_interval_sec = 1;
        }

        let response = call_test_gateway(state).await;
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(!text.contains("failed-id"));
        assert!(text.contains("success-id"));
        assert!(text.contains("response.output_text.delta"));
    }

    #[tokio::test]
    async fn retryable_response_failed_before_output_retries_current_provider() {
        let failed = concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"failed-id\"}}\n\n",
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_error\",\"message\":\"processing error\"}}}\n\n"
        );
        let completed =
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"success-id\"}}\n\n";
        let (upstream, calls) = mock_counted_sequence(vec![
            (StatusCode::OK, failed, "text/event-stream"),
            (StatusCode::OK, completed, "text/event-stream"),
        ])
        .await;
        let state = test_state(vec![test_provider("primary", upstream, 1)]);
        state.policy.write().unwrap().retry_count = 1;

        let response = call_test_gateway(state.clone()).await;
        let body = response.into_body().collect().await.unwrap().to_bytes();

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(!String::from_utf8_lossy(&body).contains("failed-id"));
        assert!(String::from_utf8_lossy(&body).contains("success-id"));
        assert!(state.logs.lock().unwrap().iter().any(|entry| {
            entry.event_type.as_deref() == Some("sse_failure_detected")
                && entry.diagnostic_detail.is_some()
        }));
    }

    #[tokio::test]
    async fn unknown_response_failed_respects_fallback_retry_switch() {
        let failed = "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"future_error\",\"message\":\"new failure\"}}}\n\n";
        let completed =
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"should-not-run\"}}\n\n";
        let (upstream, calls) = mock_counted_sequence(vec![
            (StatusCode::OK, failed, "text/event-stream"),
            (StatusCode::OK, completed, "text/event-stream"),
        ])
        .await;
        let state = test_state(vec![test_provider("primary", upstream, 1)]);
        {
            let mut policy = state.policy.write().unwrap();
            policy.retry_count = 1;
            policy.retry_unknown_transient_errors = false;
        }

        let response = call_test_gateway(state).await;
        let body = response.into_body().collect().await.unwrap().to_bytes();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(String::from_utf8_lossy(&body).contains("future_error"));
        assert!(!String::from_utf8_lossy(&body).contains("should-not-run"));
    }

    #[tokio::test]
    async fn business_incomplete_is_forwarded_without_retry() {
        let incomplete = "data: {\"type\":\"response.incomplete\",\"response\":{\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}\n\n";
        let (upstream, calls) =
            mock_counted_sequence(vec![(StatusCode::OK, incomplete, "text/event-stream")]).await;
        let state = test_state(vec![test_provider("primary", upstream, 1)]);
        state.policy.write().unwrap().retry_count = 1;

        let response = call_test_gateway(state.clone()).await;
        let body = response.into_body().collect().await.unwrap().to_bytes();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(String::from_utf8_lossy(&body).contains("max_output_tokens"));
        let health = state.health.lock().unwrap();
        let record = health.get("primary").unwrap();
        assert_eq!(record.consecutive_failures, 0);
        assert_eq!(record.successful_requests, 0);
    }

    #[tokio::test]
    async fn capacity_after_visible_sse_output_is_not_replayed() {
        let stream = concat!(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n",
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"Selected model is at capacity\"}}}\n\n"
        );
        let (primary, calls) =
            mock_counted_sequence(vec![(StatusCode::OK, stream, "text/event-stream")]).await;
        let state = test_state(vec![test_provider("primary", primary, 1)]);

        let response = call_test_gateway(state).await;
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(String::from_utf8_lossy(&body).contains("partial"));
    }

    #[tokio::test]
    async fn responses_sse_with_terminal_event_completes_without_synthetic_failure() {
        let stream = concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-ok\"}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"done\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-ok\"}}\n\n"
        );
        let upstream =
            mock_upstream_with_content_type(StatusCode::OK, stream, "text/event-stream").await;
        let state = test_state(vec![test_provider("stream", upstream, 1)]);

        let response = call_test_gateway(state.clone()).await;
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let text = String::from_utf8_lossy(&body);

        assert!(text.contains("response.completed"));
        assert!(!text.contains("upstream_stream_incomplete"));
        assert_eq!(
            state
                .health
                .lock()
                .unwrap()
                .get("stream")
                .unwrap()
                .consecutive_failures,
            0
        );
    }

    #[tokio::test]
    async fn responses_sse_completed_without_text_or_usage_is_successful() {
        let stream = concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-empty\"}}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-empty\"}}\n\n"
        );
        let (upstream, calls) =
            mock_counted_sequence(vec![(StatusCode::OK, stream, "text/event-stream")]).await;
        let state = test_state(vec![test_provider("stream", upstream, 1)]);

        let response = call_test_gateway(state.clone()).await;
        let body = response.into_body().collect().await.unwrap().to_bytes();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(String::from_utf8_lossy(&body).contains("response.completed"));
        assert_eq!(
            state
                .health
                .lock()
                .unwrap()
                .get("stream")
                .unwrap()
                .successful_requests,
            1
        );
    }

    #[test]
    fn classifies_stream_error_fields_without_retrying_permanent_errors() {
        assert_eq!(
            classify_error_fields(Some("server_error"), None, None),
            ErrorClassification::Retryable
        );
        assert_eq!(
            classify_error_fields(None, None, Some("Our servers are currently overloaded")),
            ErrorClassification::Retryable
        );
        assert_eq!(
            classify_error_fields(Some("model_not_found"), None, None),
            ErrorClassification::Permanent
        );
        assert_eq!(
            classify_error_fields(Some("context_length_exceeded"), None, None),
            ErrorClassification::Permanent
        );
        assert_eq!(
            classify_error_fields(Some("new_unrecognized_error"), None, None),
            ErrorClassification::Unknown
        );
    }

    #[test]
    fn classifies_incomplete_business_and_transient_reasons() {
        assert_eq!(
            classify_incomplete_reason(Some("max_output_tokens")),
            ErrorClassification::IncompleteBusiness
        );
        assert_eq!(
            classify_incomplete_reason(Some("upstream_timeout")),
            ErrorClassification::Retryable
        );
        assert_eq!(
            classify_incomplete_reason(Some("future_reason")),
            ErrorClassification::Unknown
        );
    }

    #[tokio::test]
    async fn responses_sse_without_terminal_event_returns_structured_failure() {
        let stream = concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-cut\"}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n"
        );
        let (upstream, calls) =
            mock_counted_sequence(vec![(StatusCode::OK, stream, "text/event-stream")]).await;
        let state = test_state(vec![test_provider("stream", upstream, 1)]);

        let response = call_test_gateway(state.clone()).await;
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let text = String::from_utf8_lossy(&body);

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(text.contains("partial"));
        assert!(text.contains("upstream_stream_incomplete"));
        assert!(text.contains("resp-cut"));
        assert_eq!(
            state
                .logs
                .lock()
                .unwrap()
                .iter()
                .filter(|entry| entry.event_type.as_deref() == Some("stream_retry_delegated"))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn chat_completions_done_marker_is_not_treated_as_missing_responses_terminal() {
        let stream = concat!(
            "data: {\"id\":\"chat-1\",\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n",
            "data: [DONE]\n\n"
        );
        let upstream =
            mock_upstream_with_content_type(StatusCode::OK, stream, "text/event-stream").await;
        let state = test_state(vec![test_provider("chat", upstream, 1)]);

        let response = call_test_gateway_path(state.clone(), "/v1/chat/completions").await;
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let text = String::from_utf8_lossy(&body);

        assert!(text.contains("[DONE]"));
        assert!(!text.contains("upstream_stream_incomplete"));
        assert_eq!(
            state
                .health
                .lock()
                .unwrap()
                .get("chat")
                .unwrap()
                .consecutive_failures,
            0
        );
    }

    #[tokio::test]
    async fn fails_over_immediately_after_payload_too_large() {
        let primary =
            mock_upstream(StatusCode::PAYLOAD_TOO_LARGE, r#"{"error":"too large"}"#).await;
        let backup = mock_upstream(StatusCode::OK, r#"{"provider":"backup"}"#).await;
        let state = test_state(vec![
            test_provider("primary", primary, 1),
            test_provider("backup", backup, 2),
        ]);
        state.policy.write().unwrap().allow_provider_failover = true;
        let response = call_test_gateway(state.clone()).await;
        assert_eq!(response.status(), StatusCode::OK);
        let logs = state.logs.lock().unwrap();
        let routing = logs.back().and_then(|entry| entry.routing_event.as_ref());
        assert_eq!(
            routing.and_then(|event| event.source_provider_id.as_deref()),
            Some("primary")
        );
        assert_eq!(
            routing.and_then(|event| event.target_provider_id.as_deref()),
            Some("backup")
        );
        assert_eq!(
            routing.and_then(|event| event.attempts.first()?.status_code),
            Some(413)
        );
    }

    #[tokio::test]
    async fn payload_too_large_can_be_configured_to_return() {
        let primary =
            mock_upstream(StatusCode::PAYLOAD_TOO_LARGE, r#"{"error":"too large"}"#).await;
        let backup = mock_upstream(StatusCode::OK, r#"{"provider":"backup"}"#).await;
        let state = test_state(vec![
            test_provider("primary", primary, 1),
            test_provider("backup", backup, 2),
        ]);
        state.policy.write().unwrap().payload_too_large_action = PayloadTooLargeAction::Return;
        let response = call_test_gateway(state).await;
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn payload_too_large_never_calls_backup_when_failover_is_disabled() {
        let (primary, primary_calls) = mock_counted_sequence(vec![(
            StatusCode::PAYLOAD_TOO_LARGE,
            r#"{"error":"too large"}"#,
            "application/json",
        )])
        .await;
        let (backup, backup_calls) = mock_counted_sequence(vec![(
            StatusCode::OK,
            r#"{"provider":"backup"}"#,
            "application/json",
        )])
        .await;
        let state = test_state(vec![
            test_provider("primary", primary, 1),
            test_provider("backup", backup, 2),
        ]);

        let response = call_test_gateway(state).await;

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(primary_calls.load(Ordering::SeqCst), 1);
        assert_eq!(backup_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn open_primary_circuit_is_bypassed_when_failover_is_disabled() {
        let (primary, primary_calls) = mock_counted_sequence(vec![(
            StatusCode::OK,
            r#"{"provider":"primary"}"#,
            "application/json",
        )])
        .await;
        let (backup, backup_calls) = mock_counted_sequence(vec![(
            StatusCode::OK,
            r#"{"provider":"backup"}"#,
            "application/json",
        )])
        .await;
        let state = test_state(vec![
            test_provider("primary", primary, 1),
            test_provider("backup", backup, 2),
        ]);
        state.health.lock().unwrap().insert(
            "primary".to_string(),
            HealthRecord {
                state: HealthState::Open,
                cooldown_until: Some(Utc::now() + Duration::minutes(5)),
                ..HealthRecord::default()
            },
        );

        let response = call_test_gateway(state.clone()).await;
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, Bytes::from_static(br#"{"provider":"primary"}"#));
        assert_eq!(primary_calls.load(Ordering::SeqCst), 1);
        assert_eq!(backup_calls.load(Ordering::SeqCst), 0);
        assert!(state.logs.lock().unwrap().iter().any(|entry| {
            entry.event_type.as_deref() == Some("circuit_bypassed_failover_disabled")
        }));
    }

    #[tokio::test]
    async fn open_primary_circuit_is_skipped_when_failover_is_enabled() {
        let (primary, primary_calls) = mock_counted_sequence(vec![(
            StatusCode::OK,
            r#"{"provider":"primary"}"#,
            "application/json",
        )])
        .await;
        let (backup, backup_calls) = mock_counted_sequence(vec![(
            StatusCode::OK,
            r#"{"provider":"backup"}"#,
            "application/json",
        )])
        .await;
        let state = test_state(vec![
            test_provider("primary", primary, 1),
            test_provider("backup", backup, 2),
        ]);
        state.policy.write().unwrap().allow_provider_failover = true;
        state.health.lock().unwrap().insert(
            "primary".to_string(),
            HealthRecord {
                state: HealthState::Open,
                cooldown_until: Some(Utc::now() + Duration::minutes(5)),
                last_issue_kind: HealthIssueKind::Transport,
                ..HealthRecord::default()
            },
        );

        let response = call_test_gateway(state).await;
        let body = response.into_body().collect().await.unwrap().to_bytes();

        assert_eq!(body, Bytes::from_static(br#"{"provider":"backup"}"#));
        assert_eq!(primary_calls.load(Ordering::SeqCst), 0);
        assert_eq!(backup_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn unavailable_model_channel_retries_without_opening_provider_circuit() {
        let body = r#"{"error":{"code":"no_available_channel","message":"No available channel for model gpt-test under group default"}}"#;
        let (primary, primary_calls) = mock_counted_sequence(vec![(
            StatusCode::SERVICE_UNAVAILABLE,
            body,
            "application/json",
        )])
        .await;
        let (backup, backup_calls) = mock_counted_sequence(vec![(
            StatusCode::OK,
            r#"{"provider":"backup"}"#,
            "application/json",
        )])
        .await;
        let state = test_state(vec![
            test_provider("primary", primary, 1),
            test_provider("backup", backup, 2),
        ]);
        {
            let mut policy = state.policy.write().unwrap();
            policy.retry_count = 1;
            policy.allow_provider_failover = true;
            policy.circuit_failure_threshold = 1;
        }

        let response = call_test_gateway(state.clone()).await;
        let response_body = response.into_body().collect().await.unwrap().to_bytes();

        assert_eq!(response_body, Bytes::from_static(body.as_bytes()));
        assert_eq!(primary_calls.load(Ordering::SeqCst), 2);
        assert_eq!(backup_calls.load(Ordering::SeqCst), 0);
        let health = state.health.lock().unwrap();
        let record = health.get("primary").unwrap();
        assert_eq!(record.state, HealthState::Degraded);
        assert_eq!(record.last_issue_kind, HealthIssueKind::Model);
        assert_eq!(record.consecutive_failures, 0);
    }

    #[tokio::test]
    async fn rate_limit_does_not_open_provider_circuit() {
        let upstream = mock_upstream(
            StatusCode::TOO_MANY_REQUESTS,
            r#"{"error":{"code":"rate_limit_exceeded","message":"slow down"}}"#,
        )
        .await;
        let state = test_state(vec![test_provider("primary", upstream, 1)]);
        {
            let mut policy = state.policy.write().unwrap();
            policy.retry_count = 0;
            policy.circuit_failure_threshold = 1;
        }

        let response = call_test_gateway(state.clone()).await;

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        let health = state.health.lock().unwrap();
        let record = health.get("primary").unwrap();
        assert_eq!(record.state, HealthState::Degraded);
        assert_eq!(record.last_issue_kind, HealthIssueKind::RateLimit);
        assert_eq!(record.consecutive_failures, 0);
    }

    #[tokio::test]
    async fn repeated_upstream_attempts_count_as_one_circuit_failure() {
        let (upstream, calls) = mock_counted_sequence(vec![(
            StatusCode::SERVICE_UNAVAILABLE,
            r#"{"error":{"code":"service_unavailable","message":"temporary outage"}}"#,
            "application/json",
        )])
        .await;
        let state = test_state(vec![test_provider("primary", upstream, 1)]);
        {
            let mut policy = state.policy.write().unwrap();
            policy.retry_count = 2;
            policy.circuit_failure_threshold = 2;
        }

        let response = call_test_gateway(state.clone()).await;

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        let health = state.health.lock().unwrap();
        let record = health.get("primary").unwrap();
        assert_eq!(record.consecutive_failures, 1);
        assert_eq!(record.state, HealthState::Degraded);
        assert_eq!(record.last_issue_kind, HealthIssueKind::Upstream);
    }

    #[test]
    fn probe_success_preserves_model_issue_and_expired_circuit_becomes_half_open() {
        let state = test_state(Vec::new());
        record_degraded_issue(
            &state,
            "provider",
            "model unavailable",
            10,
            HealthIssueKind::Model,
            None,
        );

        let probe_state = mark_probe_success(&state, "provider", 12);
        assert_eq!(probe_state, HealthState::Degraded);
        {
            let health = state.health.lock().unwrap();
            let record = health.get("provider").unwrap();
            assert_eq!(record.last_issue_kind, HealthIssueKind::Model);
        }

        {
            let mut health = state.health.lock().unwrap();
            let record = health.get_mut("provider").unwrap();
            record.state = HealthState::Open;
            record.cooldown_until = Some(Utc::now() - Duration::seconds(1));
            assert!(can_use(record));
            assert_eq!(record.state, HealthState::HalfOpen);
        }
    }

    #[tokio::test]
    async fn does_not_retry_backup_provider_after_unauthorized() {
        let primary = mock_upstream(StatusCode::UNAUTHORIZED, r#"{"error":"bad key"}"#).await;
        let backup = mock_upstream(StatusCode::OK, r#"{"provider":"backup"}"#).await;
        let response = call_test_gateway(test_state(vec![
            test_provider("primary", primary, 1),
            test_provider("backup", backup, 2),
        ]))
        .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, Bytes::from_static(br#"{"error":"bad key"}"#));
    }

    #[tokio::test]
    async fn records_requested_model_in_event_log() {
        let upstream = mock_upstream(StatusCode::OK, r#"{"ok":true}"#).await;
        let state = test_state(vec![test_provider("primary", upstream, 1)]);

        let response = call_test_gateway(state.clone()).await;

        assert_eq!(response.status(), StatusCode::OK);
        let logs = state.logs.lock().unwrap();
        assert_eq!(
            logs.back().and_then(|entry| entry.model.as_deref()),
            Some("gpt-test")
        );
    }

    #[tokio::test]
    async fn response_body_can_outlive_response_header_timeout() {
        let upstream = mock_delayed_stream(StdDuration::from_millis(1_200)).await;
        let mut provider = test_provider("slow-stream", upstream, 1);
        provider.timeout_ms = 1_000;
        let state = test_state(vec![provider]);

        let response = call_test_gateway(state.clone()).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();

        assert_eq!(body, Bytes::from_static(b"data: done\n\n"));
        let health = state.health.lock().unwrap();
        let record = health.get("slow-stream").unwrap();
        assert_eq!(record.successful_requests, 1);
        assert_eq!(record.consecutive_failures, 0);
    }

    #[tokio::test]
    async fn response_header_timeout_fails_over_before_streaming_starts() {
        let primary = mock_delayed_headers(StdDuration::from_millis(1_200)).await;
        let backup = mock_upstream(StatusCode::OK, r#"{"provider":"backup"}"#).await;
        let mut primary_provider = test_provider("slow-primary", primary, 1);
        primary_provider.timeout_ms = 1_000;
        let state = test_state(vec![primary_provider, test_provider("backup", backup, 2)]);
        {
            let mut policy = state.policy.write().unwrap();
            policy.retry_count = 0;
            policy.allow_provider_failover = true;
        }

        let response = call_test_gateway(state.clone()).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, Bytes::from_static(br#"{"provider":"backup"}"#));
        assert!(state.logs.lock().unwrap().iter().any(|entry| entry
            .routing_event
            .as_ref()
            .is_some_and(|event| event.reason.contains("响应头超时"))));
    }

    #[tokio::test]
    async fn upstream_stream_error_is_recorded_once_without_replay() {
        let upstream = mock_broken_stream().await;
        let state = test_state(vec![test_provider("broken", upstream, 1)]);

        let response = call_test_gateway(state.clone()).await;
        assert!(response.into_body().collect().await.is_err());

        let matching_logs = state
            .logs
            .lock()
            .unwrap()
            .iter()
            .filter(|entry| entry.message.contains("流式响应中途断开"))
            .count();
        assert_eq!(matching_logs, 1);
        let health = state.health.lock().unwrap();
        let record = health.get("broken").unwrap();
        assert_eq!(record.consecutive_failures, 1);
        assert_eq!(record.successful_requests, 0);
    }

    #[tokio::test]
    async fn client_cancellation_releases_stream_without_health_failure() {
        let upstream = mock_delayed_stream(StdDuration::from_secs(5)).await;
        let state = test_state(vec![test_provider("cancelled", upstream, 1)]);

        let response = call_test_gateway(state.clone()).await;
        drop(response);

        assert!(state
            .logs
            .lock()
            .unwrap()
            .iter()
            .any(|entry| entry.message.contains("客户端取消请求")));
        let health = state.health.lock().unwrap();
        let record = health.get("cancelled").unwrap();
        assert_eq!(record.consecutive_failures, 0);
        assert_eq!(record.successful_requests, 0);
    }

    #[tokio::test]
    async fn requests_identity_encoding_and_strips_connection_header() {
        let upstream = mock_request_header_echo().await;
        let response = call_test_gateway_with_headers(
            test_state(vec![test_provider("headers", upstream, 1)]),
            &[("accept-encoding", "gzip, br"), ("connection", "close")],
        )
        .await;
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, Bytes::from_static(b"identity|missing"));
    }

    #[tokio::test]
    async fn preserves_server_sent_event_response() {
        let upstream = mock_upstream_with_content_type(
            StatusCode::OK,
            concat!(
                "data: {\"type\":\"response.output_text.delta\"}\n\n",
                "data: {\"type\":\"response.completed\",\"response\":{}}\n\n"
            ),
            "text/event-stream",
        )
        .await;
        let response =
            call_test_gateway(test_state(vec![test_provider("stream", upstream, 1)])).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/event-stream"
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert!(body.starts_with(b"data: "));
    }

    #[test]
    fn parses_responses_and_chat_usage_shapes() {
        let responses = serde_json::json!({
            "usage": {
                "input_tokens": 120,
                "input_tokens_details": { "cached_tokens": 40 },
                "output_tokens": 30,
                "output_tokens_details": { "reasoning_tokens": 12 },
                "total_tokens": 150
            }
        });
        let chat = serde_json::json!({
            "usage": {
                "prompt_tokens": 80,
                "prompt_tokens_details": { "cached_tokens": 20 },
                "completion_tokens": 25,
                "completion_tokens_details": { "reasoning_tokens": 5 }
            }
        });

        let responses_usage = usage_from_response_json(&responses).unwrap();
        let chat_usage = usage_from_response_json(&chat).unwrap();

        assert_eq!(responses_usage.input_tokens, Some(120));
        assert_eq!(responses_usage.cache_read_tokens, Some(40));
        assert_eq!(responses_usage.reasoning_tokens, Some(12));
        assert_eq!(responses_usage.total_tokens, Some(150));
        assert_eq!(chat_usage.input_tokens, Some(80));
        assert_eq!(chat_usage.output_tokens, Some(25));
        assert_eq!(chat_usage.total_tokens, Some(105));
    }

    #[test]
    fn parses_chunked_sse_usage_without_modifying_bytes() {
        let first = Bytes::from_static(
            b"data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":18,",
        );
        let second = Bytes::from_static(
            b"\"output_tokens\":7,\"total_tokens\":25}}}\r\n\r\ndata: [DONE]\r\n\r\n",
        );
        let mut collector = UsageCollector::new(UsageResponseKind::Sse, None, None);

        collector.observe(&first);
        collector.observe(&second);
        let usage = collector.finish(false).usage;

        assert_eq!(usage.input_tokens, Some(18));
        assert_eq!(usage.output_tokens, Some(7));
        assert_eq!(usage.total_tokens, Some(25));
    }

    #[tokio::test]
    async fn completed_response_persists_reported_and_unreported_usage() {
        let upstream = mock_counted_sequence(vec![
            (
                StatusCode::OK,
                r#"{"usage":{"input_tokens":100,"output_tokens":25,"total_tokens":125}}"#,
                "application/json",
            ),
            (StatusCode::OK, r#"{"ok":true}"#, "application/json"),
        ])
        .await
        .0;
        let state = test_state(vec![test_provider("primary", upstream, 1)]);
        initialize_test_settings(&state);

        let first = call_test_gateway(state.clone()).await;
        first.into_body().collect().await.unwrap();
        let second = call_test_gateway(state.clone()).await;
        second.into_body().collect().await.unwrap();

        let statistics = query_usage_statistics(
            &state,
            &UsageQuery {
                mode: UsageQueryMode::Today,
                start_date: None,
                end_date: None,
                month: None,
                source: UsageSource::All,
            },
        )
        .unwrap();
        assert_eq!(statistics.completed_requests, 2);
        assert_eq!(statistics.reported_requests, 2);
        assert_eq!(statistics.coverage_percent, 100.0);
        assert!(statistics.totals.total_tokens.value > 125);
        assert_eq!(statistics.data_quality, "mixed");
        assert_eq!(
            statistics.providers[0].provider_id,
            codex_usage::OFFICIAL_PROVIDER_ID
        );
        let primary = statistics
            .providers
            .iter()
            .find(|provider| provider.provider_id == "primary")
            .unwrap();
        assert_eq!(primary.data_quality, "mixed");
        assert!(primary.metrics.total_tokens.estimated_value > 0);
    }

    #[tokio::test]
    async fn failover_usage_is_attributed_only_to_final_provider() {
        let primary =
            mock_upstream(StatusCode::INTERNAL_SERVER_ERROR, r#"{"error":"failed"}"#).await;
        let backup = mock_upstream(
            StatusCode::OK,
            r#"{"usage":{"input_tokens":40,"output_tokens":10,"total_tokens":50}}"#,
        )
        .await;
        let state = test_state(vec![
            test_provider("primary", primary, 1),
            test_provider("backup", backup, 2),
        ]);
        initialize_test_settings(&state);

        let response = call_test_gateway(state.clone()).await;
        response.into_body().collect().await.unwrap();
        let statistics = query_usage_statistics(
            &state,
            &UsageQuery {
                mode: UsageQueryMode::Today,
                start_date: None,
                end_date: None,
                month: None,
                source: UsageSource::All,
            },
        )
        .unwrap();

        let primary = statistics
            .providers
            .iter()
            .find(|provider| provider.provider_id == "primary")
            .unwrap();
        let backup = statistics
            .providers
            .iter()
            .find(|provider| provider.provider_id == "backup")
            .unwrap();
        assert_eq!(primary.completed_requests, 1);
        assert_eq!(primary.metrics.total_tokens.value, 0);
        assert_eq!(primary.data_quality, "unknown");
        assert_eq!(backup.completed_requests, 1);
        assert_eq!(backup.metrics.total_tokens.value, 50);
    }

    #[tokio::test]
    async fn retry_records_each_real_upstream_attempt() {
        let upstream = mock_counted_sequence(vec![
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                r#"{"error":{"message":"temporary"}}"#,
                "application/json",
            ),
            (
                StatusCode::OK,
                r#"{"usage":{"input_tokens":30,"output_tokens":8,"total_tokens":38}}"#,
                "application/json",
            ),
        ])
        .await
        .0;
        let state = test_state(vec![test_provider("primary", upstream, 1)]);
        initialize_test_settings(&state);
        {
            let mut policy = state.policy.write().unwrap();
            policy.retry_count = 1;
            policy.retry_interval_sec = 1;
        }

        call_test_gateway(state.clone())
            .await
            .into_body()
            .collect()
            .await
            .unwrap();
        let statistics = query_usage_statistics(
            &state,
            &UsageQuery {
                mode: UsageQueryMode::Today,
                start_date: None,
                end_date: None,
                month: None,
                source: UsageSource::Gateway,
            },
        )
        .unwrap();
        let primary = statistics
            .providers
            .iter()
            .find(|provider| provider.provider_id == "primary")
            .unwrap();
        assert_eq!(primary.completed_requests, 2);
        assert_eq!(primary.reported_requests, 1);
        assert_eq!(primary.metrics.total_tokens.value, 38);
    }

    #[test]
    fn usage_query_enforces_range_and_full_month_rules() {
        let accepted = UsageQuery {
            mode: UsageQueryMode::Range,
            start_date: Some("2024-01-01".to_string()),
            end_date: Some("2024-01-30".to_string()),
            month: None,
            source: UsageSource::All,
        };
        let rejected = UsageQuery {
            mode: UsageQueryMode::Range,
            start_date: Some("2024-01-01".to_string()),
            end_date: Some("2024-01-31".to_string()),
            month: None,
            source: UsageSource::All,
        };
        let full_month = UsageQuery {
            mode: UsageQueryMode::Month,
            start_date: None,
            end_date: None,
            month: Some("2024-01".to_string()),
            source: UsageSource::All,
        };

        assert!(usage_period_bounds(&accepted).is_ok());
        assert!(usage_period_bounds(&rejected).is_err());
        let (_, start, end, _, _) = usage_period_bounds(&full_month).unwrap();
        assert_eq!((end - start).num_days() + 1, 31);
    }

    #[test]
    fn usage_retention_prunes_only_expired_records() {
        let state = test_state(Vec::new());
        initialize_test_settings(&state);
        let insert = |completed_at: DateTime<Utc>| {
            persist_usage_record(
                &state.store,
                &UsageRecord {
                    completed_at,
                    provider_id: "provider".to_string(),
                    provider_name: "Provider".to_string(),
                    model: Some("gpt-test".to_string()),
                    request_path: "/v1/responses".to_string(),
                    usage: CapturedUsage {
                        input_tokens: Some(1),
                        output_tokens: Some(1),
                        total_tokens: Some(2),
                        ..CapturedUsage::default()
                    },
                    data_source: "gateway_response",
                    usage_origin: "gateway",
                    client_origin: "other",
                    codex_event_key: None,
                    gateway_request_id: None,
                    attempt_index: 1,
                    is_final_attempt: true,
                    http_status: Some(200),
                    outcome: "completed",
                    token_source: "upstream",
                    estimate_method: None,
                },
            )
            .unwrap();
        };
        insert(Utc::now() - Duration::days(31));
        insert(Utc::now() - Duration::days(1));

        let removed = prune_expired_usage(&state.store, 30).unwrap();
        let remaining: i64 = state
            .store
            .connection
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM usage_records", [], |row| row.get(0))
            .unwrap();

        assert_eq!(removed, 1);
        assert_eq!(remaining, 1);
    }
}
