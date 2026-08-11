mod backup;

use axum::{
    body::Body,
    extract::State,
    http::{header, HeaderMap, HeaderName, Method, StatusCode, Uri},
    response::Response,
    routing::{any, get},
    Router,
};
use bytes::Bytes;
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use futures_util::{Stream, StreamExt};
use keyring::Entry;
use reqwest::Client;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
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
use tokio::time::{sleep, timeout};
use tracing::{error, info, warn};
use url::Url;
use uuid::Uuid;

const DEFAULT_BIND: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 8765;
const KEYRING_SERVICE: &str = "ai-gateway-provider";
const MAX_EVENT_LOGS: usize = 2_000;
const DEFAULT_LOG_RETENTION_DAYS: u16 = 7;
const MAX_LOG_RETENTION_DAYS: u16 = 365;
const MAX_HEALTH_PROBES: usize = 24;
const MAX_ERROR_BODY_BYTES: usize = 1024 * 1024;
const MAX_SSE_EVENT_BYTES: usize = 256 * 1024;
const MAX_SSE_PREFLIGHT_BYTES: usize = 1024 * 1024;

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
}

#[derive(Clone, Serialize)]
pub struct ProviderDto {
    pub id: String,
    pub name: String,
    pub base_url: String,
    pub models: Vec<String>,
    pub priority: i64,
    pub enabled: bool,
    pub timeout_ms: u64,
    pub has_api_key: bool,
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
    pub probe_interval_sec: u64,
    pub log_retention_days: u16,
    pub routing_policy: RoutingPolicy,
    pub autostart_enabled: bool,
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
    pub retry_count: u8,
    pub retry_interval_sec: u64,
    pub max_retry_wait_sec: u64,
    pub capacity_retry_count: u8,
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
            retry_count: 1,
            retry_interval_sec: 1,
            max_retry_wait_sec: 10,
            capacity_retry_count: 3,
            capacity_retry_interval_sec: 5,
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
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RoutingAttempt {
    pub provider_id: String,
    pub provider_name: String,
    pub status_code: Option<u16>,
    pub error: Option<String>,
    pub latency_ms: Option<u64>,
    pub retry_count: u8,
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
    pub retry_count: u8,
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
                    routing_event_json TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_event_logs_timestamp ON event_logs(timestamp);",
            )
            .map_err(|e| e.to_string())?;
        let store = Arc::new(Store {
            connection: Mutex::new(connection),
        });
        let secrets: Arc<dyn ProviderSecretStore> = Arc::new(SystemProviderSecretStore);
        let token =
            setting(&store, "local_token")?.unwrap_or_else(|| Uuid::new_v4().simple().to_string());
        save_setting(&store, "local_token", &token)?;
        let probe_interval_sec = setting(&store, "probe_interval_sec")?
            .and_then(|v| v.parse().ok())
            .unwrap_or(30);
        let log_retention_days = setting(&store, "log_retention_days")?
            .and_then(|value| value.parse().ok())
            .filter(|days| (1..=MAX_LOG_RETENTION_DAYS).contains(days))
            .unwrap_or(DEFAULT_LOG_RETENTION_DAYS);
        // 启动时先清理过期记录，再恢复内存缓存，确保页面不会短暂看到已过保留期的日志。
        prune_expired_logs(&store, log_retention_days)?;
        let logs = load_recent_logs(&store)?;
        let policy = load_routing_policy(&store)?;
        let providers = load_providers(&store, secrets.as_ref())?;
        let locked_provider_id =
            setting(&store, "locked_provider_id")?.filter(|value| !value.is_empty());
        let settings = RouterSettings {
            bind_address: DEFAULT_BIND.to_string(),
            port: DEFAULT_PORT,
            local_token: token,
            probe_interval_sec,
            log_retention_days,
            gateway_running: true,
            gateway_listening: false,
            gateway_error: None,
        };
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
            app_handle: Some(app.clone()),
        })
    }
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

fn load_routing_policy(store: &Store) -> Result<RoutingPolicy, String> {
    let defaults = RoutingPolicy::default();
    let value = |key: &str| setting(store, key);
    let action = match value("payload_too_large_action")?.as_deref() {
        Some("return") => PayloadTooLargeAction::Return,
        _ => PayloadTooLargeAction::Failover,
    };
    let policy = RoutingPolicy {
        retry_count: value("retry_count")?
            .and_then(|item| item.parse().ok())
            .unwrap_or(defaults.retry_count),
        retry_interval_sec: value("retry_interval_sec")?
            .and_then(|item| item.parse().ok())
            .unwrap_or(defaults.retry_interval_sec),
        max_retry_wait_sec: value("max_retry_wait_sec")?
            .and_then(|item| item.parse().ok())
            .unwrap_or(defaults.max_retry_wait_sec),
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
    validate_routing_policy(&policy)?;
    Ok(policy)
}

fn validate_routing_policy(policy: &RoutingPolicy) -> Result<(), String> {
    if policy.retry_count > 3 {
        return Err("重试次数必须在 0 到 3 之间".to_string());
    }
    if policy.retry_interval_sec > 60 {
        return Err("重试间隔必须在 0 到 60 秒之间".to_string());
    }
    if !(1..=300).contains(&policy.max_retry_wait_sec) {
        return Err("单次请求最大重试等待必须在 1 到 300 秒之间".to_string());
    }
    if policy.capacity_retry_count > 5 {
        return Err("模型容量重试次数必须在 0 到 5 之间".to_string());
    }
    if !(1..=60).contains(&policy.capacity_retry_interval_sec) {
        return Err("模型容量重试间隔必须在 1 到 60 秒之间".to_string());
    }
    if !(1..=600).contains(&policy.capacity_max_retry_wait_sec) {
        return Err("模型容量最大等待必须在 1 到 600 秒之间".to_string());
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

fn routing_policy_values(policy: &RoutingPolicy) -> Vec<(&'static str, String)> {
    vec![
        ("retry_count", policy.retry_count.to_string()),
        ("retry_interval_sec", policy.retry_interval_sec.to_string()),
        ("max_retry_wait_sec", policy.max_retry_wait_sec.to_string()),
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
    let mut conn = store
        .connection
        .lock()
        .map_err(|_| "数据库锁已损坏".to_string())?;
    let transaction = conn.transaction().map_err(|error| error.to_string())?;
    // 所有规则在同一事务中更新，避免新请求读取到一半是旧值、一半是新值的策略。
    for (key, value) in routing_policy_values(policy) {
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
    probe_interval_sec: u64,
    log_retention_days: u16,
    policy: &RoutingPolicy,
) -> Result<(), String> {
    let mut conn = store
        .connection
        .lock()
        .map_err(|_| "数据库锁已损坏".to_string())?;
    let transaction = conn.transaction().map_err(|error| error.to_string())?;
    let mut values = vec![
        ("probe_interval_sec", probe_interval_sec.to_string()),
        ("log_retention_days", log_retention_days.to_string()),
    ];
    values.extend(routing_policy_values(policy));

    // 探测间隔和路由规则属于同一份运行配置，必须一起提交，避免保存成功一半。
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
            })
        })
        .map_err(|e| e.to_string())?;
    let mut result = Vec::new();
    for item in rows {
        let mut provider = item.map_err(|e| e.to_string())?;
        // 系统密钥库临时不可用时仍允许桌面端启动；路由会把缺少密钥的中转视为不可用，
        // 导出和编辑操作则继续返回明确错误，避免静默生成不完整备份。
        provider.api_key = secrets
            .get(&provider.id)
            .unwrap_or_default()
            .unwrap_or_default();
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
    }
}

fn health_snapshot(id: &str, record: &HealthRecord) -> HealthSnapshot {
    HealthSnapshot {
        provider_id: id.to_string(),
        state: record.state.clone(),
        latency_ms: record.latency_ms,
        consecutive_failures: record.consecutive_failures,
        last_error: record.last_error.clone(),
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
    let conn = store
        .connection
        .lock()
        .map_err(|_| "数据库锁已损坏".to_string())?;
    conn.execute(
        "INSERT INTO event_logs(timestamp,level,message,provider_id,model,event_type,routing_event_json)
         VALUES(?1,?2,?3,?4,?5,?6,?7)",
        params![
            log_timestamp(entry.timestamp),
            entry.level,
            entry.message,
            entry.provider_id,
            entry.model,
            entry.event_type,
            routing_event_json
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
            "SELECT timestamp,level,message,provider_id,model,event_type,routing_event_json
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
            ))
        })
        .map_err(|error| error.to_string())?;
    let mut newest_first = Vec::new();
    for row in rows {
        let (timestamp, level, message, provider_id, model, event_type, routing_event_json) =
            row.map_err(|error| error.to_string())?;
        let timestamp = DateTime::parse_from_rfc3339(&timestamp)
            .map_err(|error| format!("运行日志时间格式损坏：{error}"))?
            .with_timezone(&Utc);
        // 单条旧版详情损坏时仍保留基础日志，避免一行 JSON 阻止应用启动。
        let routing_event = routing_event_json
            .as_deref()
            .and_then(|json| serde_json::from_str::<RoutingEvent>(json).ok());
        newest_first.push(EventLog {
            timestamp,
            level,
            message,
            provider_id,
            model,
            event_type,
            routing_event,
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
    append_event_log(
        state,
        EventLog {
            timestamp: Utc::now(),
            level: level.to_string(),
            message: message.into(),
            provider_id: provider_id.map(ToOwned::to_owned),
            // 模型名属于路由元数据，允许展示但限制长度；prompt 和响应正文始终不会进入日志。
            model: model.map(|value| value.chars().take(120).collect()),
            event_type: None,
            routing_event: None,
        },
    );
}

fn push_routing_event(state: &AppState, message: String, event: RoutingEvent) {
    append_event_log(
        state,
        EventLog {
            timestamp: event.timestamp,
            level: match event.event_type.as_str() {
                "capacity_recovered" => "info",
                "capacity_exhausted" => "error",
                _ => "warning",
            }
            .to_string(),
            message,
            provider_id: event.target_provider_id.clone(),
            model: event.model.clone(),
            event_type: Some(event.event_type.clone()),
            routing_event: Some(event.clone()),
        },
    );

    let notify = state.policy.read().unwrap().notify_switches;
    let Some(app) = state.app_handle.as_ref() else {
        return;
    };
    let _ = app.emit("routing-event", &event);
    // 容量重试只进入日志和详情，不复用自动切换的 Toast 与托盘提示。
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

fn push_capacity_event(
    state: &AppState,
    provider: &ProviderConfig,
    model: Option<&str>,
    request_path: &str,
    request_body_bytes: usize,
    attempts: Vec<RoutingAttempt>,
    capacity: &CapacityError,
    recovered: bool,
    status_code: Option<u16>,
) {
    let retry_count = attempts.len().saturating_sub(1).min(usize::from(u8::MAX)) as u8;
    let event_type = if recovered {
        "capacity_recovered"
    } else {
        "capacity_exhausted"
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
        reason: capacity.message.clone(),
        final_result: event_type.to_string(),
    };
    let code = capacity.code.as_deref().unwrap_or("model_capacity");
    let message = if recovered {
        format!("模型容量恢复，已在 {} 完成重试（{code}）", provider.name)
    } else {
        format!("模型容量重试已耗尽：{}（{code}）", provider.name)
    };
    push_routing_event(state, message, event);
}
fn can_use(record: &mut HealthRecord) -> bool {
    if record.state != HealthState::Open {
        return true;
    }
    // 冷却到期只放行一次半开探测，后续成功或失败会重新确定熔断状态。
    if record.cooldown_until.is_some_and(|at| at <= Utc::now()) {
        record.state = HealthState::HalfOpen;
        return true;
    }
    false
}
fn mark_success(state: &AppState, id: &str, latency: u64) -> HealthState {
    let mut health = state.health.lock().unwrap();
    let record = health.entry(id.to_string()).or_default();
    record.state = HealthState::Healthy;
    record.latency_ms = Some(latency);
    record.consecutive_failures = 0;
    record.last_error = None;
    record.cooldown_until = None;
    record.total_requests += 1;
    record.successful_requests += 1;
    record.state.clone()
}

fn mark_response_started(state: &AppState, id: &str, latency: u64) {
    let mut health = state.health.lock().unwrap();
    let record = health.entry(id.to_string()).or_default();
    // 收到响应头只说明上游已经建立响应，完整成功必须等响应体正常结束后再计数。
    record.state = HealthState::Healthy;
    record.latency_ms = Some(latency);
    record.consecutive_failures = 0;
    record.last_error = None;
    record.cooldown_until = None;
}

fn mark_response_completed(state: &AppState, id: &str) {
    let mut health = state.health.lock().unwrap();
    let record = health.entry(id.to_string()).or_default();
    record.total_requests += 1;
    record.successful_requests += 1;
}

fn record_capacity_exhausted(state: &AppState, id: &str, message: &str, latency: u64) {
    let mut health = state.health.lock().unwrap();
    let record = health.entry(id.to_string()).or_default();
    // 模型容量是模型级瞬时状态，只计入请求结果，不累计中转失败或触发熔断。
    record.total_requests += 1;
    record.latency_ms = Some(latency);
    record.last_error = Some(message.chars().take(180).collect());
}

fn mark_upstream_rejection(state: &AppState, id: &str, status: StatusCode, latency: u64) {
    let mut health = state.health.lock().unwrap();
    let record = health.entry(id.to_string()).or_default();
    record.state = HealthState::Degraded;
    record.latency_ms = Some(latency);
    record.consecutive_failures += 1;
    record.last_error = Some(format!("上游返回 {status}"));
    record.total_requests += 1;
}

fn mark_failure(
    state: &AppState,
    id: &str,
    message: String,
    cooldown_until: Option<DateTime<Utc>>,
    policy: &RoutingPolicy,
) -> HealthState {
    let mut health = state.health.lock().unwrap();
    let record = health.entry(id.to_string()).or_default();
    record.total_requests += 1;
    record.consecutive_failures += 1;
    record.last_error = Some(message.chars().take(180).collect());
    // 429 按 Retry-After 直接冷却；其余瞬时错误按用户策略累计后熔断。
    if let Some(until) = cooldown_until {
        record.cooldown_until = Some(until);
        record.state = HealthState::Open;
    } else if record.consecutive_failures >= u32::from(policy.circuit_failure_threshold) {
        record.cooldown_until =
            Some(Utc::now() + Duration::seconds(policy.circuit_cooldown_sec as i64));
        record.state = HealthState::Open;
    } else {
        record.state = HealthState::Degraded;
    }
    record.state.clone()
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
    record.last_error = Some(format!("上游拒绝了 {body_bytes} 字节的请求体（413）"));
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
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|value| value == format!("Bearer {token}") || value == token)
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
fn request_model(body: &Bytes) -> Option<String> {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|json| {
            json.get("model")
                .and_then(|v| v.as_str())
                .map(ToOwned::to_owned)
        })
}

#[derive(Clone, Debug)]
struct CapacityError {
    code: Option<String>,
    message: String,
}

fn capacity_message_matches(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("model")
        && (normalized.contains("at capacity")
            || normalized.contains("capacity exceeded")
            || normalized.contains("capacity limit"))
}

fn capacity_error_from_json(value: &serde_json::Value) -> Option<CapacityError> {
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
        .any(|identifier| identifier.contains("capacity") && identifier.contains("model"));
    if !identifier_matches && !capacity_message_matches(message) {
        return None;
    }
    Some(CapacityError {
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
            && name != header::HOST
            && name != header::CONTENT_LENGTH
            && name != header::ACCEPT_ENCODING
            && !is_hop_by_hop_header(name)
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
) -> (PreparedUpstreamResponse, Option<CapacityError>) {
    let mut body = Vec::new();
    while let Some(chunk) = response.stream.next().await {
        match chunk {
            Ok(chunk) => {
                body.extend_from_slice(&chunk);
                response.prefix.push(chunk);
                if body.len() > MAX_ERROR_BODY_BYTES {
                    return (response, None);
                }
            }
            Err(error) => {
                response.preserve_stream_error(error);
                return (response, None);
            }
        }
    }
    let capacity = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .as_ref()
        .and_then(capacity_error_from_json);
    (response, capacity)
}

enum SsePreflightResult {
    Forward(PreparedUpstreamResponse),
    Capacity(PreparedUpstreamResponse, CapacityError),
}

async fn inspect_sse_before_output(mut response: PreparedUpstreamResponse) -> SsePreflightResult {
    let mut pending_event = Vec::new();
    let mut total_buffered = 0_usize;
    while let Some(chunk) = response.stream.next().await {
        match chunk {
            Ok(chunk) => {
                total_buffered = total_buffered.saturating_add(chunk.len());
                pending_event.extend_from_slice(&chunk);
                response.prefix.push(chunk);
            }
            Err(error) => {
                response.preserve_stream_error(error);
                return SsePreflightResult::Forward(response);
            }
        }

        if total_buffered > MAX_SSE_PREFLIGHT_BYTES {
            return SsePreflightResult::Forward(response);
        }
        loop {
            let Some(frame_end) = next_sse_frame_end(&pending_event) else {
                if pending_event.len() > MAX_SSE_EVENT_BYTES {
                    return SsePreflightResult::Forward(response);
                }
                break;
            };
            if frame_end > MAX_SSE_EVENT_BYTES {
                // 完整事件可能在单个网络块内到达，同样必须执行上限检查后立刻降级为原始透传。
                return SsePreflightResult::Forward(response);
            }
            let frame = pending_event.drain(..frame_end).collect::<Vec<_>>();
            let Some(data) = sse_frame_data(&frame) else {
                // SSE 注释和空事件不携带业务状态，可以继续等待首个真正输出。
                continue;
            };
            if data == b"[DONE]" {
                return SsePreflightResult::Forward(response);
            }
            let Ok(value) = serde_json::from_slice::<serde_json::Value>(&data) else {
                return SsePreflightResult::Forward(response);
            };
            if let Some(capacity) = capacity_error_from_json(&value) {
                return SsePreflightResult::Capacity(response, capacity);
            }
            let event_type = value.get("type").and_then(|item| item.as_str());
            if matches!(
                event_type,
                Some("response.created" | "response.in_progress")
            ) {
                continue;
            }
            // 第一个非生命周期事件可能包含文本、推理或工具调用；此后重放将不再安全。
            return SsePreflightResult::Forward(response);
        }
    }
    SsePreflightResult::Forward(response)
}

async fn proxy_request(
    State(state): State<AppState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    if !state.running.load(Ordering::Relaxed) {
        return json_error(StatusCode::SERVICE_UNAVAILABLE, "本地网关已停止");
    }
    let token = state.settings.read().unwrap().local_token.clone();
    if !auth_valid(&headers, &token) {
        return json_error(StatusCode::UNAUTHORIZED, "本地访问令牌无效");
    }
    let model = request_model(&body);
    let body_bytes = body.len();
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
        // 手动固定只改变首选顺序，首选故障时仍允许自动切换，避免固定配置把 Codex 完全阻断。
        candidates.sort_by_key(|provider| {
            if &provider.id == locked_id {
                0
            } else {
                provider.priority + 1
            }
        });
    }
    let mut last_error = "没有可用的中转".to_string();
    let mut last_status = None;
    let mut only_payload_failures = true;
    let mut first_failed_provider = None;
    let mut first_failure_reason = None;
    let mut attempts = Vec::new();
    let request_path = uri.path().to_string();

    for provider in candidates {
        let (available, payload_allowed) = {
            let mut health = state.health.lock().unwrap();
            let record = health.entry(provider.id.clone()).or_default();
            let available = can_use(record);
            let payload_allowed = payload_size_allowed(record, body_bytes);
            (available, payload_allowed)
        };
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
                continue;
            }
        };
        let mut waited_seconds = 0_u64;
        let mut provider_failed = false;
        let mut capacity_retry_index = 0_u8;
        let mut capacity_waited_seconds = 0_u64;
        let mut capacity_attempts = Vec::new();
        let mut last_capacity = None;
        'transient_retry: for retry_index in 0..=policy.retry_count {
            loop {
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
                        let (prepared, capacity) = if !status.is_success() {
                            inspect_json_error_response(prepared).await
                        } else if prepared.is_sse() {
                            match inspect_sse_before_output(prepared).await {
                                SsePreflightResult::Forward(response) => (response, None),
                                SsePreflightResult::Capacity(response, capacity) => {
                                    (response, Some(capacity))
                                }
                            }
                        } else {
                            (prepared, None)
                        };

                        if let Some(capacity) = capacity {
                            let attempt_latency = started.elapsed().as_millis() as u64;
                            let wait_seconds =
                                retry_after.unwrap_or(policy.capacity_retry_interval_sec);
                            let can_retry = capacity_retry_index < policy.capacity_retry_count
                                && capacity_waited_seconds.saturating_add(wait_seconds)
                                    <= policy.capacity_max_retry_wait_sec;
                            capacity_attempts.push(RoutingAttempt {
                                provider_id: provider.id.clone(),
                                provider_name: provider.name.clone(),
                                status_code: Some(status.as_u16()),
                                error: Some(capacity.message.clone()),
                                latency_ms: Some(attempt_latency),
                                retry_count: capacity_retry_index,
                                switched: false,
                            });
                            last_capacity = Some(capacity.clone());
                            if can_retry {
                                capacity_retry_index += 1;
                                capacity_waited_seconds += wait_seconds;
                                push_log_with_model(
                                    &state,
                                    "warning",
                                    format!(
                                        "模型容量不足，{wait_seconds} 秒后重试同一中转（{}/{})",
                                        capacity_retry_index, policy.capacity_retry_count
                                    ),
                                    Some(&provider.id),
                                    model.as_deref(),
                                );
                                sleep(StdDuration::from_secs(wait_seconds)).await;
                                continue;
                            }
                            record_capacity_exhausted(
                                &state,
                                &provider.id,
                                &capacity.message,
                                attempt_latency,
                            );
                            push_capacity_event(
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
                            return response_to_axum(prepared, None);
                        }

                        if !capacity_attempts.is_empty() {
                            let capacity =
                                last_capacity.as_ref().expect("容量重试必须保留错误原因");
                            if status.is_success() {
                                capacity_attempts.push(RoutingAttempt {
                                    provider_id: provider.id.clone(),
                                    provider_name: provider.name.clone(),
                                    status_code: Some(status.as_u16()),
                                    error: None,
                                    latency_ms: Some(latency),
                                    retry_count: capacity_retry_index,
                                    switched: false,
                                });
                                push_capacity_event(
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
                                let message = format!("容量重试后上游返回 {status}");
                                capacity_attempts.push(RoutingAttempt {
                                    provider_id: provider.id.clone(),
                                    provider_name: provider.name.clone(),
                                    status_code: Some(status.as_u16()),
                                    error: Some(message.clone()),
                                    latency_ms: Some(latency),
                                    retry_count: capacity_retry_index,
                                    switched: false,
                                });
                                record_capacity_exhausted(&state, &provider.id, &message, latency);
                                push_capacity_event(
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
                            push_log_with_model(
                                &state,
                                "warning",
                                last_error.clone(),
                                Some(&provider.id),
                                model.as_deref(),
                            );
                            if policy.payload_too_large_action == PayloadTooLargeAction::Return {
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
                            last_error = format!("上游返回 {status}");
                            let wait_seconds = if status == StatusCode::TOO_MANY_REQUESTS {
                                retry_after.unwrap_or(policy.retry_interval_sec)
                            } else {
                                policy.retry_interval_sec
                            };
                            let can_retry = retry_index < policy.retry_count
                                && waited_seconds.saturating_add(wait_seconds)
                                    <= policy.max_retry_wait_sec;
                            attempts.push(RoutingAttempt {
                                provider_id: provider.id.clone(),
                                provider_name: provider.name.clone(),
                                status_code: Some(status.as_u16()),
                                error: Some(last_error.clone()),
                                latency_ms: Some(latency),
                                retry_count: retry_index,
                                switched: !can_retry,
                            });
                            if can_retry {
                                waited_seconds += wait_seconds;
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
                            mark_failure(
                                &state,
                                &provider.id,
                                last_error.clone(),
                                cooldown,
                                &policy,
                            );
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
                                    retry_count: attempts
                                        .iter()
                                        .map(|item| u32::from(item.retry_count))
                                        .sum::<u32>()
                                        .min(u32::from(u8::MAX))
                                        as u8,
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
                            push_log_with_model(
                                &state,
                                "error",
                                format!("上游拒绝请求：{status}"),
                                Some(&provider.id),
                                model.as_deref(),
                            );
                        }
                        let observation = status.is_success().then(|| ResponseStreamContext {
                            state: state.clone(),
                            provider_id: provider.id.clone(),
                            model: model.clone(),
                            request_path: request_path.clone(),
                            policy: policy.clone(),
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
                        if !capacity_attempts.is_empty() {
                            let capacity =
                                last_capacity.as_ref().expect("容量重试必须保留错误原因");
                            capacity_attempts.push(RoutingAttempt {
                                provider_id: provider.id.clone(),
                                provider_name: provider.name.clone(),
                                status_code: None,
                                error: Some(last_error.clone()),
                                latency_ms: Some(latency),
                                retry_count: capacity_retry_index,
                                switched: false,
                            });
                            record_capacity_exhausted(&state, &provider.id, &last_error, latency);
                            push_capacity_event(
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
                        let wait_seconds = policy.retry_interval_sec;
                        let can_retry = retry_index < policy.retry_count
                            && waited_seconds.saturating_add(wait_seconds)
                                <= policy.max_retry_wait_sec;
                        attempts.push(RoutingAttempt {
                            provider_id: provider.id.clone(),
                            provider_name: provider.name.clone(),
                            status_code: None,
                            error: Some(last_error.clone()),
                            latency_ms: Some(latency),
                            retry_count: retry_index,
                            switched: !can_retry,
                        });
                        if can_retry {
                            waited_seconds += wait_seconds;
                            sleep(StdDuration::from_secs(wait_seconds)).await;
                            continue 'transient_retry;
                        }
                        mark_failure(&state, &provider.id, last_error.clone(), None, &policy);
                        first_failed_provider.get_or_insert_with(|| provider.id.clone());
                        first_failure_reason.get_or_insert_with(|| last_error.clone());
                        provider_failed = true;
                        break 'transient_retry;
                    }
                }
            }
        }
        if provider_failed {
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
    provider_id: String,
    model: Option<String>,
    request_path: String,
    policy: RoutingPolicy,
}

struct ObservedResponseStream {
    inner: GatewayByteStream,
    context: ResponseStreamContext,
    finished: bool,
    failed: bool,
}

impl ObservedResponseStream {
    fn new(stream: GatewayByteStream, context: ResponseStreamContext) -> Self {
        Self {
            inner: stream,
            context,
            finished: false,
            failed: false,
        }
    }
}

impl Stream for ObservedResponseStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => Poll::Ready(Some(Ok(chunk))),
            Poll::Ready(Some(Err(error))) => {
                if !self.failed {
                    self.failed = true;
                    let detail: String = error.to_string().chars().take(180).collect();
                    let message = format!(
                        "流式响应中途断开（{}）：{}",
                        self.context.request_path, detail
                    );
                    // 响应头之后无法迁移当前请求，但这次传输失败必须影响后续请求的健康路由。
                    mark_failure(
                        &self.context.state,
                        &self.context.provider_id,
                        message.clone(),
                        None,
                        &self.context.policy,
                    );
                    push_log_with_model(
                        &self.context.state,
                        "error",
                        message,
                        Some(&self.context.provider_id),
                        self.context.model.as_deref(),
                    );
                }
                Poll::Ready(Some(Err(std::io::Error::other("上游流式响应中途断开"))))
            }
            Poll::Ready(None) => {
                self.finished = true;
                mark_response_completed(&self.context.state, &self.context.provider_id);
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
            let probe_state = mark_failure(&state, &provider.id, error.clone(), None, &policy);
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
            let probe_state = mark_success(&state, &provider.id, latency);
            record_health_probe(&state, &provider.id, probe_state, Some(latency), None);
        }
        Ok(response) => {
            let error = format!("探测返回 {}", response.status());
            let probe_state = mark_failure(&state, &provider.id, error.clone(), None, &policy);
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
            let probe_state = mark_failure(&state, &provider.id, error.clone(), None, &policy);
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
    }
}

#[tauri::command]
fn list_providers(state: TauriState<'_, AppState>) -> Result<Vec<ProviderDto>, String> {
    Ok(state.config.read().unwrap().iter().map(dto).collect())
}
#[tauri::command]
fn get_health_snapshots(state: TauriState<'_, AppState>) -> Result<Vec<HealthSnapshot>, String> {
    let health = state
        .health
        .lock()
        .map_err(|_| "健康状态不可用".to_string())?;
    Ok(health
        .iter()
        .map(|(id, record)| health_snapshot(id, record))
        .collect())
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

fn save_configuration_state(state: &AppState, input: &ConfigurationInput) -> Result<(), String> {
    validate_probe_interval(input.probe_interval_sec)?;
    validate_log_retention_days(input.log_retention_days)?;
    validate_routing_policy(&input.routing_policy)?;
    persist_configuration(
        &state.store,
        input.probe_interval_sec,
        input.log_retention_days,
        &input.routing_policy,
    )?;

    // 同时持有两把写锁后再替换快照，确保事务成功前运行时永远看不到新配置。
    let mut settings = state.settings.write().unwrap();
    let mut policy = state.policy.write().unwrap();
    settings.probe_interval_sec = input.probe_interval_sec;
    settings.log_retention_days = input.log_retention_days;
    *policy = input.routing_policy.clone();
    drop(policy);
    drop(settings);
    // 缩短保留期限时立即清理；清理失败不回滚已提交配置，后台任务会在下一小时重试。
    if let Err(error) = prune_state_logs(state, input.log_retention_days) {
        warn!(%error, "event log cleanup after configuration save failed");
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

#[tauri::command]
fn save_configuration(
    input: ConfigurationInput,
    app: tauri::AppHandle,
    state: TauriState<'_, AppState>,
) -> Result<ConfigurationSnapshot, String> {
    save_configuration_state(&state, &input)?;

    let autolaunch = app.autolaunch();
    let current_autostart = autolaunch
        .is_enabled()
        .map_err(|error| format!("网关与路由配置已保存，但无法读取开机启动状态：{error}"))?;
    // Windows 插件删除不存在的注册表值会返回 os error 2；目标状态已经满足时必须视为成功。
    let autostart_result = if current_autostart == input.autostart_enabled {
        Ok(())
    } else if input.autostart_enabled {
        autolaunch.enable()
    } else {
        autolaunch.disable()
    };
    if let Err(error) = autostart_result {
        return Err(format!("网关与路由配置已保存，但开机启动更新失败：{error}"));
    }

    let actual_autostart = autolaunch
        .is_enabled()
        .map_err(|error| format!("网关与路由配置已保存，但无法确认开机启动状态：{error}"))?;
    if actual_autostart != input.autostart_enabled {
        return Err("网关与路由配置已保存，但开机启动状态未按预期生效".to_string());
    }

    push_log(&state, "info", "配置中心设置已更新", None);
    Ok(configuration_snapshot(&state, actual_autostart))
}

#[tauri::command]
fn save_routing_policy(
    policy: RoutingPolicy,
    state: TauriState<'_, AppState>,
) -> Result<RoutingPolicy, String> {
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
fn save_router_settings(
    port: u16,
    probe_interval_sec: u64,
    state: TauriState<'_, AppState>,
) -> Result<RouterSettings, String> {
    if port != DEFAULT_PORT {
        return Err(format!("当前版本固定监听端口 {DEFAULT_PORT}"));
    }
    validate_probe_interval(probe_interval_sec)?;
    save_setting(
        &state.store,
        "probe_interval_sec",
        &probe_interval_sec.to_string(),
    )?;
    state.settings.write().unwrap().probe_interval_sec = probe_interval_sec;
    Ok(state.settings.read().unwrap().clone())
}
#[tauri::command]
fn save_provider(
    input: ProviderInput,
    state: TauriState<'_, AppState>,
) -> Result<ProviderDto, String> {
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
    // 编辑时允许留空以保留原密钥；数据库事务中永远不会出现密钥明文。
    let api_key = match input.api_key.filter(|value| !value.trim().is_empty()) {
        Some(value) => {
            let value = value.trim().to_string();
            state
                .secrets
                .set(&id, &value)
                .map_err(|error| format!("API Key 无法保存到系统密钥库：{error}"))?;
            value
        }
        None => old
            .as_ref()
            .map(|item| item.api_key.clone())
            .unwrap_or_default(),
    };
    if api_key.is_empty() {
        return Err("API Key 不能为空".to_string());
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
    };
    let conn = state
        .store
        .connection
        .lock()
        .map_err(|_| "数据库锁已损坏".to_string())?;
    conn.execute("INSERT INTO providers(id,name,base_url,models_json,priority,enabled,timeout_ms,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,datetime('now')) ON CONFLICT(id) DO UPDATE SET name=excluded.name,base_url=excluded.base_url,models_json=excluded.models_json,priority=excluded.priority,enabled=excluded.enabled,timeout_ms=excluded.timeout_ms", params![provider.id, provider.name, provider.base_url, serde_json::to_string(&provider.models).unwrap(), provider.priority, provider.enabled as i64, provider.timeout_ms as i64]).map_err(|e| e.to_string())?;
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
            mark_success(&state, &id, latency);
            Ok(serde_json::json!({"ok": true, "message": "连接成功", "latency_ms": latency}))
        }
        Ok(response) => {
            let message = format!("上游返回 {}", response.status());
            let policy = state.policy.read().unwrap().clone();
            mark_failure(&state, &id, message.clone(), None, &policy);
            Ok(serde_json::json!({"ok": false, "message": message}))
        }
        Err(error) => {
            let policy = state.policy.read().unwrap().clone();
            mark_failure(&state, &id, error.to_string(), None, &policy);
            Ok(serde_json::json!({"ok": false, "message": error.to_string()}))
        }
    }
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
            tauri::async_runtime::spawn(async move {
                health_loop(monitor_state).await;
            });
            tauri::async_runtime::spawn(async move {
                log_cleanup_loop(log_cleanup_state).await;
            });
            tauri::async_runtime::spawn(async move {
                let router = Router::new()
                    .route("/healthz", get(|| async { "ok" }))
                    .route("/v1/*path", any(proxy_request))
                    .with_state(gateway_state.clone());
                let address: SocketAddr = format!("{DEFAULT_BIND}:{DEFAULT_PORT}").parse().unwrap();
                let listener = match tokio::net::TcpListener::bind(address).await {
                    Ok(listener) => listener,
                    Err(error) => {
                        let message = format!("本地端口 {address} 监听失败：{error}");
                        gateway_state.running.store(false, Ordering::Relaxed);
                        let mut settings = gateway_state.settings.write().unwrap();
                        settings.gateway_running = false;
                        settings.gateway_listening = false;
                        settings.gateway_error = Some(message.clone());
                        drop(settings);
                        push_log(&gateway_state, "error", message.clone(), None);
                        error!(%error, "gateway bind failed");
                        return;
                    }
                };
                {
                    let mut settings = gateway_state.settings.write().unwrap();
                    settings.gateway_listening = true;
                    settings.gateway_error = None;
                }
                info!(%address, "local gateway started");
                if let Err(error) = axum::serve(listener, router).await {
                    let message = format!("本地网关已停止：{error}");
                    let mut settings = gateway_state.settings.write().unwrap();
                    settings.gateway_listening = false;
                    settings.gateway_error = Some(message.clone());
                    drop(settings);
                    push_log(&gateway_state, "error", message, None);
                    error!(%error, "gateway stopped");
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
            save_routing_policy,
            list_route_groups,
            update_route_group,
            get_runtime_stats,
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
                    routing_event_json TEXT
                );",
            )
            .unwrap();
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

    async fn call_test_gateway(state: AppState) -> Response<Body> {
        Router::new()
            .route("/v1/*path", any(proxy_request))
            .with_state(state)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/v1/responses")
                    .header(header::AUTHORIZATION, "Bearer local-test-token")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":"gpt-test","input":"hello"}"#))
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    async fn call_test_gateway_with_headers(
        state: AppState,
        extra_headers: &[(&str, &str)],
    ) -> Response<Body> {
        let mut request = Request::builder()
            .method(Method::POST)
            .uri("/v1/responses")
            .header(header::AUTHORIZATION, "Bearer local-test-token")
            .header(header::CONTENT_TYPE, "application/json");
        for (name, value) in extra_headers {
            request = request.header(*name, *value);
        }
        Router::new()
            .route("/v1/*path", any(proxy_request))
            .with_state(state)
            .oneshot(
                request
                    .body(Body::from(r#"{"model":"gpt-test","input":"hello"}"#))
                    .unwrap(),
            )
            .await
            .unwrap()
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
    fn identifies_only_structured_model_capacity_errors() {
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

        assert!(capacity_error_from_json(&exact).is_some());
        assert!(capacity_error_from_json(&responses_event).is_some());
        assert!(capacity_error_from_json(&capacity_type).is_some());
        assert!(capacity_error_from_json(&unrelated).is_none());
    }

    #[test]
    fn old_routing_policy_json_uses_capacity_defaults() {
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
        let policy: RoutingPolicy = serde_json::from_value(old).unwrap();
        assert_eq!(policy.capacity_retry_count, 3);
        assert_eq!(policy.capacity_retry_interval_sec, 5);
        assert_eq!(policy.capacity_max_retry_wait_sec, 30);
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
        mark_success(&state, "provider", 20);
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
    fn routing_policy_rejects_unbounded_retry_count() {
        let policy = RoutingPolicy {
            retry_count: 4,
            ..RoutingPolicy::default()
        };
        assert!(validate_routing_policy(&policy).is_err());
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
            probe_interval_sec: 45,
            log_retention_days: 14,
            routing_policy: RoutingPolicy {
                retry_count: 2,
                ..RoutingPolicy::default()
            },
            autostart_enabled: false,
        };

        save_configuration_state(&state, &input).unwrap();

        assert_eq!(state.settings.read().unwrap().probe_interval_sec, 45);
        assert_eq!(state.settings.read().unwrap().log_retention_days, 14);
        assert_eq!(state.policy.read().unwrap().retry_count, 2);
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
            45,
            DEFAULT_LOG_RETENTION_DAYS,
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

    #[tokio::test]
    async fn retries_backup_provider_after_server_error() {
        let primary =
            mock_upstream(StatusCode::INTERNAL_SERVER_ERROR, r#"{"error":"failed"}"#).await;
        let backup = mock_upstream(StatusCode::OK, r#"{"provider":"backup"}"#).await;
        let response = call_test_gateway(test_state(vec![
            test_provider("primary", primary, 1),
            test_provider("backup", backup, 2),
        ]))
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, Bytes::from_static(br#"{"provider":"backup"}"#));
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
            policy.capacity_retry_count = 1;
            policy.capacity_retry_interval_sec = 1;
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
            .any(|entry| { entry.event_type.as_deref() == Some("capacity_recovered") }));
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
            policy.capacity_retry_count = 1;
            policy.capacity_retry_interval_sec = 1;
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
    async fn sse_capacity_before_output_retries_without_mixing_response_ids() {
        let first = concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"failed-id\"}}\r\n\r\n",
            "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"model_at_capacity\",\"message\":\"Selected model is at capacity\"}}}\r\n\r\n"
        );
        let second = concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"success-id\"}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n"
        );
        let (primary, calls) = mock_counted_sequence(vec![
            (StatusCode::OK, first, "text/event-stream"),
            (StatusCode::OK, second, "text/event-stream"),
        ])
        .await;
        let state = test_state(vec![test_provider("primary", primary, 1)]);
        {
            let mut policy = state.policy.write().unwrap();
            policy.capacity_retry_count = 1;
            policy.capacity_retry_interval_sec = 1;
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
    async fn fails_over_immediately_after_payload_too_large() {
        let primary =
            mock_upstream(StatusCode::PAYLOAD_TOO_LARGE, r#"{"error":"too large"}"#).await;
        let backup = mock_upstream(StatusCode::OK, r#"{"provider":"backup"}"#).await;
        let state = test_state(vec![
            test_provider("primary", primary, 1),
            test_provider("backup", backup, 2),
        ]);
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
        state.policy.write().unwrap().retry_count = 0;

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
            "data: {\"type\":\"response.output_text.delta\"}\n\n",
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
}
