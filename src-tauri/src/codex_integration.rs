use chrono::Utc;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet},
    env, fs,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::Command,
    sync::Mutex,
};
use sysinfo::{Pid, System};
use toml_edit::{value, DocumentMut, InlineTable, Item, Table};
use uuid::Uuid;
use zeroize::Zeroize;

const MANAGED_PROVIDER_ID: &str = "custom";
const CONFIG_BACKUP_LIMIT: usize = 10;
const HISTORY_BACKUP_LIMIT: usize = 3;
const HISTORY_BACKUP_DIR: &str = "ai-gateway-history-backups";
const HISTORY_MANIFEST_FILE: &str = "manifest.json";

static CONFIG_WRITE_LOCK: Mutex<()> = Mutex::new(());

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodexConnectionMode {
    Official,
    Gateway,
    External,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CodexLoginStatus {
    LoggedIn,
    LoggedOut,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CodexApplicationStatus {
    NotInstalled,
    InstalledStopped,
    InstalledRunning,
}

#[derive(Clone, Debug)]
pub struct CodexEnvironmentStatus {
    pub desktop_status: CodexApplicationStatus,
    pub cli_status: CodexApplicationStatus,
    pub login_status: CodexLoginStatus,
}

#[derive(Clone, Debug, Serialize)]
pub struct CodexIntegrationState {
    pub desktop_status: CodexApplicationStatus,
    pub cli_status: CodexApplicationStatus,
    pub login_status: CodexLoginStatus,
    pub current_mode: CodexConnectionMode,
    pub gateway_configured: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoryProviderCount {
    pub provider: String,
    pub count: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct CodexHistoryMigrationPreview {
    pub preview_id: String,
    pub fingerprint: String,
    pub normal_session_count: usize,
    pub archived_session_count: usize,
    pub migration_required_count: usize,
    pub provider_counts: Vec<HistoryProviderCount>,
    pub indexed_record_count: usize,
    pub encrypted_content_session_count: usize,
    pub warning_messages: Vec<String>,
    pub can_restore_latest: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct CodexHistoryMigrationResult {
    pub migrated_session_count: usize,
    pub updated_index_count: usize,
    pub backup_name: String,
    pub warning_messages: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct CodexHistoryRestoreResult {
    pub restored_session_count: usize,
    pub restored_index_count: usize,
    pub warning_messages: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct SwitchCodexModeResult {
    pub previous_mode: CodexConnectionMode,
    pub current_mode: CodexConnectionMode,
    pub changed: bool,
    pub restart_required: bool,
    pub warning_messages: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct GatewayCodexConfiguration {
    pub port: u16,
    pub local_token: String,
}

#[derive(Clone, Debug)]
struct ParsedConfig {
    document: DocumentMut,
    hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HistorySessionEntry {
    relative_path: String,
    session_id: String,
    provider: String,
    archived: bool,
    file_hash: String,
    size: u64,
    modified_ns: u128,
    has_encrypted_content: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HistoryIndexEntry {
    host_id: String,
    thread_id: String,
    provider: String,
}

#[derive(Clone, Debug)]
pub struct HistoryPreviewSnapshot {
    pub preview: CodexHistoryMigrationPreview,
    sessions: Vec<HistorySessionEntry>,
    index_entries: Vec<HistoryIndexEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HistoryBackupManifest {
    created_at: String,
    source_fingerprint: String,
    sessions: Vec<HistorySessionEntry>,
    index_entries: Vec<HistoryIndexEntry>,
}

pub fn config_path() -> Result<PathBuf, String> {
    let home = env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("USERPROFILE").map(|value| PathBuf::from(value).join(".codex")))
        .ok_or_else(|| "无法定位 Codex 配置目录：CODEX_HOME 和 USERPROFILE 均未设置".to_string())?;
    Ok(home.join("config.toml"))
}

pub(crate) fn codex_home() -> Result<PathBuf, String> {
    config_path()?
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| "Codex 配置路径缺少父目录".to_string())
}

fn hash_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// 返回 config.toml 的原始内容指纹，不解析 TOML，也不会接触 auth.json。
/// 监听器依靠该值识别外部工具写入以及格式损坏等所有实际文件变化。
pub fn config_fingerprint() -> Result<String, String> {
    let path = config_path()?;
    if !path.exists() {
        return Ok(hash_bytes(&[]));
    }
    fs::read(path)
        .map(|bytes| hash_bytes(&bytes))
        .map_err(|error| format!("无法读取 Codex 配置指纹：{error}"))
}

fn read_config(path: &Path) -> Result<ParsedConfig, String> {
    if !path.exists() {
        return Ok(ParsedConfig {
            document: DocumentMut::new(),
            hash: hash_bytes(&[]),
        });
    }
    let bytes = fs::read(path).map_err(|error| format!("无法读取 Codex 配置：{error}"))?;
    let text = String::from_utf8(bytes.clone())
        .map_err(|_| "Codex config.toml 不是有效 UTF-8 文件".to_string())?;
    let document = text
        .parse::<DocumentMut>()
        .map_err(|error| format!("Codex config.toml 格式无效：{error}"))?;
    Ok(ParsedConfig {
        document,
        hash: hash_bytes(&bytes),
    })
}

fn current_provider(document: &DocumentMut) -> Option<String> {
    document
        .get("model_provider")
        .and_then(Item::as_str)
        .map(ToOwned::to_owned)
}

fn detected_mode(document: &DocumentMut) -> (CodexConnectionMode, Option<String>) {
    let current = current_provider(document);
    if current.as_deref() != Some(MANAGED_PROVIDER_ID) {
        return match current {
            None => (CodexConnectionMode::External, None),
            Some(provider) => (CodexConnectionMode::External, Some(provider)),
        };
    }
    match managed_provider_table(document) {
        Some(table) if is_official_provider_shape(table) => (CodexConnectionMode::Official, None),
        Some(table) if is_gateway_provider_shape(table) => (CodexConnectionMode::Gateway, None),
        _ => (
            CodexConnectionMode::External,
            Some(MANAGED_PROVIDER_ID.to_string()),
        ),
    }
}

fn managed_provider_table(document: &DocumentMut) -> Option<&Table> {
    document
        .get("model_providers")
        .and_then(Item::as_table)
        .and_then(|providers| providers.get(MANAGED_PROVIDER_ID))
        .and_then(Item::as_table)
}

fn header_token(table: &Table) -> Option<&str> {
    table
        .get("http_headers")
        .and_then(Item::as_inline_table)
        .and_then(|headers| headers.get("X-AI-Gateway-Token"))
        .and_then(|token| token.as_str())
}

fn is_gateway_provider_shape(table: &Table) -> bool {
    let base_url_is_local_gateway =
        table
            .get("base_url")
            .and_then(Item::as_str)
            .is_some_and(|base_url| {
                base_url.starts_with("http://127.0.0.1:") && base_url.ends_with("/v1")
            });
    table.get("name").and_then(Item::as_str) == Some("AI Gateway")
        && base_url_is_local_gateway
        && table.get("wire_api").and_then(Item::as_str) == Some("responses")
        && table.get("requires_openai_auth").and_then(Item::as_bool) == Some(true)
        && table.get("request_max_retries").and_then(Item::as_integer) == Some(0)
        && table.get("stream_max_retries").and_then(Item::as_integer) == Some(2)
        && header_token(table).is_some_and(|token| !token.is_empty())
}

fn is_official_provider_shape(table: &Table) -> bool {
    table.get("name").and_then(Item::as_str) == Some("OpenAI")
        && table.get("base_url").is_none()
        && table.get("wire_api").and_then(Item::as_str) == Some("responses")
        && table.get("requires_openai_auth").and_then(Item::as_bool) == Some(true)
}

fn gateway_configuration_current(
    document: &DocumentMut,
    configuration: &GatewayCodexConfiguration,
) -> bool {
    let Some(table) = managed_provider_table(document) else {
        return false;
    };
    table.get("name").and_then(Item::as_str) == Some("AI Gateway")
        && table.get("base_url").and_then(Item::as_str)
            == Some(format!("http://127.0.0.1:{}/v1", configuration.port).as_str())
        && table.get("wire_api").and_then(Item::as_str) == Some("responses")
        && table.get("requires_openai_auth").and_then(Item::as_bool) == Some(true)
        && table.get("request_max_retries").and_then(Item::as_integer) == Some(0)
        && table.get("stream_max_retries").and_then(Item::as_integer) == Some(2)
        && header_token(table) == Some(configuration.local_token.as_str())
}

fn build_managed_provider(configuration: &GatewayCodexConfiguration) -> Table {
    let mut provider = Table::new();
    provider["name"] = value("AI Gateway");
    provider["base_url"] = value(format!("http://127.0.0.1:{}/v1", configuration.port));
    provider["wire_api"] = value("responses");
    // 固定启用 Codex 官方认证识别；本地网关会在转发前剥离所有官方身份头。
    provider["requires_openai_auth"] = value(true);
    provider["request_max_retries"] = value(0);
    // 网关只重试尚未输出的请求；输出后的 SSE 恢复交给了解会话状态的 Codex。
    provider["stream_max_retries"] = value(2);
    let mut headers = InlineTable::new();
    headers.insert(
        "X-AI-Gateway-Token",
        configuration.local_token.clone().into(),
    );
    provider["http_headers"] = Item::Value(headers.into());
    provider
}

fn build_official_provider() -> Table {
    let mut provider = Table::new();
    provider["name"] = value("OpenAI");
    provider["wire_api"] = value("responses");
    provider["requires_openai_auth"] = value(true);
    provider
}

fn apply_gateway(
    document: &mut DocumentMut,
    configuration: &GatewayCodexConfiguration,
) -> Result<(), String> {
    if !document.contains_key("model_providers") {
        document["model_providers"] = Item::Table(Table::new());
    }
    let providers = document["model_providers"]
        .as_table_mut()
        .ok_or_else(|| "Codex 配置中的 model_providers 不是有效表结构".to_string())?;
    providers[MANAGED_PROVIDER_ID] = Item::Table(build_managed_provider(configuration));
    document["model_provider"] = value(MANAGED_PROVIDER_ID);
    Ok(())
}

fn apply_official(document: &mut DocumentMut) -> Result<(), String> {
    if !document.contains_key("model_providers") {
        document["model_providers"] = Item::Table(Table::new());
    }
    let providers = document["model_providers"]
        .as_table_mut()
        .ok_or_else(|| "Codex 配置中的 model_providers 不是有效表结构".to_string())?;
    providers[MANAGED_PROVIDER_ID] = Item::Table(build_official_provider());
    document["model_provider"] = value(MANAGED_PROVIDER_ID);
    Ok(())
}

fn backup_existing(path: &Path) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }
    let parent = path
        .parent()
        .ok_or_else(|| "Codex 配置路径缺少父目录".to_string())?;
    let stamp = Utc::now().format("%Y%m%d-%H%M%S-%9f");
    let backup = parent.join(format!("config.toml.ai-gateway-{stamp}.bak"));
    fs::copy(path, &backup).map_err(|error| format!("无法备份 Codex 配置：{error}"))?;

    let mut backups = fs::read_dir(parent)
        .map_err(|error| format!("无法读取 Codex 配置目录：{error}"))?
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("config.toml.ai-gateway-")
        })
        .collect::<Vec<_>>();
    backups.sort_by_key(|entry| entry.file_name());
    let remove_count = backups.len().saturating_sub(CONFIG_BACKUP_LIMIT);
    for entry in backups.into_iter().take(remove_count) {
        fs::remove_file(entry.path())
            .map_err(|error| format!("无法轮转旧 Codex 配置备份：{error}"))?;
    }
    Ok(())
}

fn write_config(
    path: &Path,
    expected_hash: &str,
    document: &DocumentMut,
) -> Result<String, String> {
    let parent = path
        .parent()
        .ok_or_else(|| "Codex 配置路径缺少父目录".to_string())?;
    fs::create_dir_all(parent).map_err(|error| format!("无法创建 Codex 配置目录：{error}"))?;
    let current_hash = if path.exists() {
        hash_bytes(&fs::read(path).map_err(|error| format!("无法重新读取 Codex 配置：{error}"))?)
    } else {
        hash_bytes(&[])
    };
    if current_hash != expected_hash {
        return Err(
            "Codex config.toml 已被其他程序修改，本次切换已取消；请刷新状态后重试".to_string(),
        );
    }
    let contents = document.to_string();
    contents
        .parse::<DocumentMut>()
        .map_err(|error| format!("生成的 Codex 配置验证失败：{error}"))?;
    backup_existing(path)?;

    let operation_id = format!(
        "{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let temp_path = parent.join(format!(".config.toml.ai-gateway-{operation_id}.tmp"));
    let mut temp = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp_path)
        .map_err(|error| format!("无法创建 Codex 配置临时文件：{error}"))?;
    if let Err(error) = (|| -> Result<(), std::io::Error> {
        temp.write_all(contents.as_bytes())?;
        temp.sync_all()?;
        Ok(())
    })() {
        let _ = fs::remove_file(&temp_path);
        return Err(format!("无法写入 Codex 配置临时文件：{error}"));
    }
    // Windows 的 rename 不会覆盖现有文件，先使用可回滚的替换文件保存当前版本。
    let replaced_path = parent.join(format!(".config.toml.ai-gateway-{operation_id}.old"));
    if path.exists() {
        fs::rename(path, &replaced_path)
            .map_err(|error| format!("无法准备替换 Codex 配置：{error}"))?;
    }
    if let Err(error) = fs::rename(&temp_path, path) {
        if replaced_path.exists() {
            let _ = fs::rename(&replaced_path, path);
        }
        let _ = fs::remove_file(&temp_path);
        return Err(format!("无法原子替换 Codex 配置：{error}"));
    }
    // 回滚副本必须保留到重新解析和逐字节校验全部完成，防止磁盘或替换异常损坏用户配置。
    let verification = (|| -> Result<ParsedConfig, String> {
        let verified = read_config(path)?;
        let written =
            fs::read_to_string(path).map_err(|error| format!("无法回读 Codex 配置：{error}"))?;
        if written != contents {
            return Err("Codex 配置写入后的内容校验失败".to_string());
        }
        Ok(verified)
    })();
    let verified = match verification {
        Ok(verified) => verified,
        Err(error) => {
            let _ = fs::remove_file(path);
            if replaced_path.exists() {
                let _ = fs::rename(&replaced_path, path);
            }
            return Err(error);
        }
    };
    if replaced_path.exists() {
        // 配置已经验证成功，清理临时回滚副本失败不应把已完成的切换报告为失败。
        let _ = fs::remove_file(&replaced_path);
    }
    Ok(verified.hash)
}

fn session_roots(home: &Path) -> [(PathBuf, bool); 2] {
    [
        (home.join("sessions"), false),
        (home.join("archived_sessions"), true),
    ]
}

fn collect_jsonl_files(root: &Path, output: &mut Vec<PathBuf>) -> Result<(), String> {
    if !root.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(root).map_err(|error| format!("无法读取会话目录：{error}"))?
    {
        let entry = entry.map_err(|error| format!("无法读取会话目录项：{error}"))?;
        let path = entry.path();
        if path.is_dir() {
            collect_jsonl_files(&path, output)?;
        } else if path.extension().and_then(|value| value.to_str()) == Some("jsonl") {
            output.push(path);
        }
    }
    Ok(())
}

fn inspect_session_file(
    home: &Path,
    path: &Path,
    archived: bool,
) -> Result<HistorySessionEntry, String> {
    let file = fs::File::open(path).map_err(|error| format!("无法读取会话文件：{error}"))?;
    let metadata = file
        .metadata()
        .map_err(|error| format!("无法读取会话元数据：{error}"))?;
    let mut reader = BufReader::new(file);
    let mut first_line = Vec::new();
    reader
        .read_until(b'\n', &mut first_line)
        .map_err(|error| format!("无法读取会话首行：{error}"))?;
    let first_json = first_line
        .strip_suffix(b"\n")
        .unwrap_or(&first_line)
        .strip_suffix(b"\r")
        .unwrap_or_else(|| first_line.strip_suffix(b"\n").unwrap_or(&first_line));
    let value: serde_json::Value = serde_json::from_slice(first_json)
        .map_err(|error| format!("会话首行不是有效 JSON：{}：{error}", path.display()))?;
    if value.get("type").and_then(serde_json::Value::as_str) != Some("session_meta") {
        return Err(format!("会话文件缺少首条 session_meta：{}", path.display()));
    }
    let payload = value
        .get("payload")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| format!("会话元数据缺少 payload：{}", path.display()))?;
    let session_id = payload
        .get("session_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("会话元数据缺少 session_id：{}", path.display()))?
        .to_string();
    let provider = payload
        .get("model_provider")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("openai")
        .to_string();

    // 只流式查找字段名来提示跨 Provider 风险，不解析、不保存任何聊天正文。
    let mut has_encrypted_content = false;
    let mut line = Vec::new();
    while reader
        .read_until(b'\n', &mut line)
        .map_err(|error| format!("无法检查会话兼容性：{error}"))?
        != 0
    {
        if line
            .windows(b"\"encrypted_content\"".len())
            .any(|window| window == b"\"encrypted_content\"")
        {
            has_encrypted_content = true;
            break;
        }
        line.clear();
    }
    let bytes = fs::read(path).map_err(|error| format!("无法计算会话指纹：{error}"))?;
    let relative_path = path
        .strip_prefix(home)
        .map_err(|_| "会话文件不在 CODEX_HOME 内".to_string())?
        .to_string_lossy()
        .to_string();
    let modified_ns = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    Ok(HistorySessionEntry {
        relative_path,
        session_id,
        provider,
        archived,
        file_hash: hash_bytes(&bytes),
        size: metadata.len(),
        modified_ns,
        has_encrypted_content,
    })
}

fn load_history_index(home: &Path) -> Result<Vec<HistoryIndexEntry>, String> {
    let path = home.join("sqlite").join("codex-dev.db");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let connection = Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| format!("无法读取 Codex 会话索引：{error}"))?;
    let mut statement = connection
        .prepare(
            "SELECT host_id, thread_id, model_provider
             FROM local_thread_catalog ORDER BY host_id, thread_id",
        )
        .map_err(|error| format!("无法读取 Codex 会话索引结构：{error}"))?;
    let entries = statement
        .query_map([], |row| {
            Ok(HistoryIndexEntry {
                host_id: row.get(0)?,
                thread_id: row.get(1)?,
                provider: row.get(2)?,
            })
        })
        .map_err(|error| format!("无法查询 Codex 会话索引：{error}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("无法解析 Codex 会话索引：{error}"))?;
    Ok(entries)
}

fn history_fingerprint(
    sessions: &[HistorySessionEntry],
    index_entries: &[HistoryIndexEntry],
) -> Result<String, String> {
    serde_json::to_vec(&(sessions, index_entries))
        .map(|bytes| hash_bytes(&bytes))
        .map_err(|error| format!("无法生成会话预览指纹：{error}"))
}

fn history_backup_directories(home: &Path) -> Result<Vec<PathBuf>, String> {
    let root = home.join(HISTORY_BACKUP_DIR);
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut directories = fs::read_dir(root)
        .map_err(|error| format!("无法读取会话迁移备份：{error}"))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_dir() && path.join(HISTORY_MANIFEST_FILE).exists())
        .collect::<Vec<_>>();
    directories.sort();
    Ok(directories)
}

pub fn scan_history() -> Result<HistoryPreviewSnapshot, String> {
    let home = codex_home()?;
    let mut sessions = Vec::new();
    for (root, archived) in session_roots(&home) {
        let mut paths = Vec::new();
        collect_jsonl_files(&root, &mut paths)?;
        paths.sort();
        for path in paths {
            sessions.push(inspect_session_file(&home, &path, archived)?);
        }
    }
    sessions.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    let index_entries = load_history_index(&home)?;
    let fingerprint = history_fingerprint(&sessions, &index_entries)?;
    let mut counts = BTreeMap::<String, usize>::new();
    for session in &sessions {
        *counts.entry(session.provider.clone()).or_default() += 1;
    }
    let provider_counts = counts
        .into_iter()
        .map(|(provider, count)| HistoryProviderCount { provider, count })
        .collect::<Vec<_>>();
    let encrypted_count = sessions
        .iter()
        .filter(|session| session.has_encrypted_content)
        .count();
    let migration_required_count = sessions
        .iter()
        .filter(|session| session.provider != MANAGED_PROVIDER_ID)
        .count();
    let mut warning_messages = vec![
        "迁移只统一本地会话可见性，不保证第三方中转能继续处理官方 encrypted_content".to_string(),
    ];
    if encrypted_count == 0 {
        warning_messages.clear();
    }
    let preview = CodexHistoryMigrationPreview {
        preview_id: Uuid::new_v4().to_string(),
        fingerprint,
        normal_session_count: sessions.iter().filter(|item| !item.archived).count(),
        archived_session_count: sessions.iter().filter(|item| item.archived).count(),
        migration_required_count,
        provider_counts,
        indexed_record_count: index_entries.len(),
        encrypted_content_session_count: encrypted_count,
        warning_messages,
        can_restore_latest: !history_backup_directories(&home)?.is_empty(),
    };
    Ok(HistoryPreviewSnapshot {
        preview,
        sessions,
        index_entries,
    })
}

fn codex_is_running() -> Result<bool, String> {
    #[cfg(windows)]
    {
        let mut command = Command::new("tasklist");
        command.args(["/FI", "IMAGENAME eq Codex.exe", "/FO", "CSV", "/NH"]);
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x0800_0000);
        let output = command
            .output()
            .map_err(|error| format!("无法检测 Codex 进程：{error}"))?;
        let text = String::from_utf8_lossy(&output.stdout).to_ascii_lowercase();
        return Ok(text.lines().any(|line| line.contains("\"codex.exe\"")));
    }
    #[cfg(not(windows))]
    {
        Ok(false)
    }
}

fn rewrite_session_provider(bytes: &[u8], provider: &str) -> Result<Vec<u8>, String> {
    let newline = bytes
        .iter()
        .position(|byte| *byte == b'\n')
        .map(|index| index + 1)
        .unwrap_or(bytes.len());
    let line_without_newline = &bytes[..newline]
        .strip_suffix(b"\n")
        .unwrap_or(&bytes[..newline])
        .strip_suffix(b"\r")
        .unwrap_or_else(|| {
            bytes[..newline]
                .strip_suffix(b"\n")
                .unwrap_or(&bytes[..newline])
        });
    let mut value: serde_json::Value = serde_json::from_slice(line_without_newline)
        .map_err(|error| format!("会话首行不是有效 JSON：{error}"))?;
    if value.get("type").and_then(serde_json::Value::as_str) != Some("session_meta") {
        return Err("会话首行不是 session_meta".to_string());
    }
    let payload = value
        .get_mut("payload")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or_else(|| "会话首行缺少 payload".to_string())?;
    payload.insert(
        "model_provider".to_string(),
        serde_json::Value::String(provider.to_string()),
    );
    let mut rewritten =
        serde_json::to_vec(&value).map_err(|error| format!("无法序列化会话元数据：{error}"))?;
    if bytes[..newline].ends_with(b"\r\n") {
        rewritten.extend_from_slice(b"\r\n");
    } else if bytes[..newline].ends_with(b"\n") {
        rewritten.push(b'\n');
    }
    // 首行之后的聊天正文直接按原字节追加，避免迁移触碰任何消息内容。
    rewritten.extend_from_slice(&bytes[newline..]);
    Ok(rewritten)
}

fn write_verified_temp(
    target: &Path,
    contents: &[u8],
    operation_id: &str,
) -> Result<PathBuf, String> {
    let parent = target
        .parent()
        .ok_or_else(|| "会话文件缺少父目录".to_string())?;
    let file_name = target.file_name().unwrap_or_default().to_string_lossy();
    // 同一年月目录通常包含多个 JSONL，临时文件名必须带原文件名避免互相冲突。
    let temp = parent.join(format!(
        ".{file_name}.ai-gateway-history-{operation_id}.tmp"
    ));
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)
        .map_err(|error| format!("无法创建会话临时文件：{error}"))?;
    file.write_all(contents)
        .and_then(|_| file.sync_all())
        .map_err(|error| format!("无法写入会话临时文件：{error}"))?;
    let verified = fs::read(&temp).map_err(|error| format!("无法验证会话临时文件：{error}"))?;
    if verified != contents {
        let _ = fs::remove_file(&temp);
        return Err("会话临时文件回读校验失败".to_string());
    }
    Ok(temp)
}

fn remove_prepared_temps(prepared: &[(PathBuf, PathBuf)]) {
    for (_, temp) in prepared {
        let _ = fs::remove_file(temp);
    }
}

fn rotate_history_backups(home: &Path) -> Result<(), String> {
    let backups = history_backup_directories(home)?;
    let remove_count = backups.len().saturating_sub(HISTORY_BACKUP_LIMIT);
    for path in backups.into_iter().take(remove_count) {
        fs::remove_dir_all(path).map_err(|error| format!("无法轮转旧会话迁移备份：{error}"))?;
    }
    Ok(())
}

fn create_history_backup(
    home: &Path,
    snapshot: &HistoryPreviewSnapshot,
) -> Result<(PathBuf, HistoryBackupManifest), String> {
    let stamp = Utc::now().format("%Y%m%d-%H%M%S-%9f").to_string();
    let root = home.join(HISTORY_BACKUP_DIR).join(&stamp);
    fs::create_dir_all(&root).map_err(|error| format!("无法创建会话迁移备份目录：{error}"))?;
    let changed_sessions = snapshot
        .sessions
        .iter()
        .filter(|item| item.provider != MANAGED_PROVIDER_ID)
        .cloned()
        .collect::<Vec<_>>();
    for session in &changed_sessions {
        let source = home.join(&session.relative_path);
        let destination = root.join("sessions").join(&session.relative_path);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).map_err(|error| format!("无法创建会话备份目录：{error}"))?;
        }
        fs::copy(source, destination).map_err(|error| format!("无法备份会话文件：{error}"))?;
    }
    let manifest = HistoryBackupManifest {
        created_at: Utc::now().to_rfc3339(),
        source_fingerprint: snapshot.preview.fingerprint.clone(),
        sessions: changed_sessions,
        index_entries: snapshot
            .index_entries
            .iter()
            .filter(|entry| entry.provider != MANAGED_PROVIDER_ID)
            .cloned()
            .collect(),
    };
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|error| format!("无法生成会话备份清单：{error}"))?;
    fs::write(root.join(HISTORY_MANIFEST_FILE), manifest_bytes)
        .map_err(|error| format!("无法写入会话备份清单：{error}"))?;
    Ok((root, manifest))
}

fn replace_prepared_files(
    prepared: &[(PathBuf, PathBuf)],
    operation_id: &str,
) -> Result<Vec<(PathBuf, PathBuf)>, String> {
    let mut replaced = Vec::new();
    for (target, temp) in prepared {
        let old = target.with_file_name(format!(
            ".{}.ai-gateway-history-{operation_id}.old",
            target.file_name().unwrap_or_default().to_string_lossy()
        ));
        if let Err(error) = fs::rename(target, &old) {
            rollback_replaced_files(&replaced);
            remove_prepared_temps(prepared);
            return Err(format!("无法准备替换会话文件：{error}"));
        }
        if let Err(error) = fs::rename(temp, target) {
            let _ = fs::rename(&old, target);
            rollback_replaced_files(&replaced);
            remove_prepared_temps(prepared);
            return Err(format!("无法替换会话文件：{error}"));
        }
        replaced.push((target.clone(), old));
    }
    Ok(replaced)
}

fn rollback_replaced_files(replaced: &[(PathBuf, PathBuf)]) {
    for (target, old) in replaced.iter().rev() {
        let _ = fs::remove_file(target);
        let _ = fs::rename(old, target);
    }
}

fn finish_replaced_files(replaced: &[(PathBuf, PathBuf)]) {
    for (_, old) in replaced {
        let _ = fs::remove_file(old);
    }
}

fn restore_index_entries(db_path: &Path, entries: &[HistoryIndexEntry]) -> Result<usize, String> {
    if !db_path.exists() {
        return Ok(0);
    }
    let mut connection =
        Connection::open(db_path).map_err(|error| format!("无法打开 Codex 会话索引：{error}"))?;
    let transaction = connection
        .transaction()
        .map_err(|error| format!("无法开始 Codex 索引补偿事务：{error}"))?;
    let mut restored = 0;
    for entry in entries {
        restored += transaction
            .execute(
                "UPDATE local_thread_catalog SET model_provider = ?1
                 WHERE host_id = ?2 AND thread_id = ?3 AND model_provider = ?4",
                params![
                    entry.provider,
                    entry.host_id,
                    entry.thread_id,
                    MANAGED_PROVIDER_ID
                ],
            )
            .map_err(|error| format!("无法补偿 Codex 会话索引：{error}"))?;
    }
    transaction
        .commit()
        .map_err(|error| format!("无法提交 Codex 索引补偿：{error}"))?;
    Ok(restored)
}

pub fn apply_history_migration(
    expected: &HistoryPreviewSnapshot,
) -> Result<CodexHistoryMigrationResult, String> {
    if codex_is_running()? {
        return Err("检测到 Codex 仍在运行。请完全退出 Codex 后再执行会话迁移".to_string());
    }
    let current = scan_history()?;
    if current.preview.fingerprint != expected.preview.fingerprint {
        return Err("会话或本地索引在预览后发生变化，请重新扫描后再迁移".to_string());
    }
    let home = codex_home()?;
    let (backup_root, manifest) = create_history_backup(&home, &current)?;
    let operation_id = Uuid::new_v4().simple().to_string();
    let mut prepared = Vec::new();
    for session in &manifest.sessions {
        let target = home.join(&session.relative_path);
        let original = match fs::read(&target) {
            Ok(bytes) => bytes,
            Err(error) => {
                remove_prepared_temps(&prepared);
                return Err(format!("无法读取待迁移会话：{error}"));
            }
        };
        let rewritten = match rewrite_session_provider(&original, MANAGED_PROVIDER_ID) {
            Ok(bytes) => bytes,
            Err(error) => {
                remove_prepared_temps(&prepared);
                return Err(error);
            }
        };
        let temp = match write_verified_temp(&target, &rewritten, &operation_id) {
            Ok(path) => path,
            Err(error) => {
                remove_prepared_temps(&prepared);
                return Err(error);
            }
        };
        prepared.push((target, temp));
    }
    let replaced = replace_prepared_files(&prepared, &operation_id)?;
    let db_path = home.join("sqlite").join("codex-dev.db");
    let index_result = (|| -> Result<usize, String> {
        if !db_path.exists() {
            return Ok(0);
        }
        let mut connection = Connection::open(&db_path)
            .map_err(|error| format!("无法打开 Codex 会话索引：{error}"))?;
        let transaction = connection
            .transaction()
            .map_err(|error| format!("无法开始 Codex 索引事务：{error}"))?;
        let mut updated = 0;
        for entry in &manifest.index_entries {
            updated += transaction
                .execute(
                    "UPDATE local_thread_catalog SET model_provider = ?1
                     WHERE host_id = ?2 AND thread_id = ?3 AND model_provider = ?4",
                    params![
                        MANAGED_PROVIDER_ID,
                        entry.host_id,
                        entry.thread_id,
                        entry.provider
                    ],
                )
                .map_err(|error| format!("无法更新 Codex 会话索引：{error}"))?;
        }
        transaction
            .commit()
            .map_err(|error| format!("无法提交 Codex 会话索引：{error}"))?;
        Ok(updated)
    })();
    let updated_index_count = match index_result {
        Ok(count) => count,
        Err(error) => {
            rollback_replaced_files(&replaced);
            return Err(error);
        }
    };
    finish_replaced_files(&replaced);
    let mut warnings = current.preview.warning_messages;
    if let Err(error) = rotate_history_backups(&home) {
        // 轮转不影响迁移一致性，作为警告返回，避免把已提交操作误报成失败。
        warnings.push(format!("旧会话迁移备份轮转失败：{error}"));
    }
    Ok(CodexHistoryMigrationResult {
        migrated_session_count: manifest.sessions.len(),
        updated_index_count,
        backup_name: backup_root
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string(),
        warning_messages: warnings,
    })
}

pub fn restore_latest_history_migration() -> Result<CodexHistoryRestoreResult, String> {
    if codex_is_running()? {
        return Err("检测到 Codex 仍在运行。请完全退出 Codex 后再恢复会话迁移".to_string());
    }
    let home = codex_home()?;
    let latest = history_backup_directories(&home)?
        .pop()
        .ok_or_else(|| "没有可恢复的会话迁移备份".to_string())?;
    let manifest: HistoryBackupManifest = serde_json::from_slice(
        &fs::read(latest.join(HISTORY_MANIFEST_FILE))
            .map_err(|error| format!("无法读取会话备份清单：{error}"))?,
    )
    .map_err(|error| format!("会话备份清单已损坏：{error}"))?;
    let operation_id = Uuid::new_v4().simple().to_string();
    let mut prepared = Vec::new();
    let mut warnings = Vec::new();
    for session in &manifest.sessions {
        let target = home.join(&session.relative_path);
        if !target.exists() {
            warnings.push(format!(
                "会话文件已不存在，已跳过：{}",
                session.relative_path
            ));
            continue;
        }
        let current = match fs::read(&target) {
            Ok(bytes) => bytes,
            Err(error) => {
                remove_prepared_temps(&prepared);
                return Err(format!("无法读取待恢复会话：{error}"));
            }
        };
        let current_provider = serde_json::from_slice::<serde_json::Value>(
            current
                .split_inclusive(|byte| *byte == b'\n')
                .next()
                .unwrap_or(&current),
        )
        .ok()
        .and_then(|value| {
            value
                .pointer("/payload/model_provider")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned)
        });
        if current_provider.as_deref() != Some(MANAGED_PROVIDER_ID) {
            warnings.push(format!(
                "会话 Provider 已在迁移后被修改，已跳过：{}",
                session.relative_path
            ));
            continue;
        }
        let restored = match rewrite_session_provider(&current, &session.provider) {
            Ok(bytes) => bytes,
            Err(error) => {
                remove_prepared_temps(&prepared);
                return Err(error);
            }
        };
        let temp = match write_verified_temp(&target, &restored, &operation_id) {
            Ok(path) => path,
            Err(error) => {
                remove_prepared_temps(&prepared);
                return Err(error);
            }
        };
        prepared.push((target, temp));
    }
    let replaced = replace_prepared_files(&prepared, &operation_id)?;
    let db_path = home.join("sqlite").join("codex-dev.db");
    let index_result = (|| -> Result<usize, String> {
        if !db_path.exists() {
            return Ok(0);
        }
        restore_index_entries(&db_path, &manifest.index_entries)
    })();
    let restored_index_count = match index_result {
        Ok(count) => count,
        Err(error) => {
            rollback_replaced_files(&replaced);
            return Err(error);
        }
    };
    finish_replaced_files(&replaced);
    Ok(CodexHistoryRestoreResult {
        restored_session_count: prepared.len(),
        restored_index_count,
        warning_messages: warnings,
    })
}

#[derive(Deserialize)]
struct LocalAuthDocument {
    #[serde(rename = "OPENAI_API_KEY")]
    api_key: Option<String>,
    tokens: Option<LocalAuthTokens>,
}

#[derive(Deserialize)]
struct LocalAuthTokens {
    access_token: Option<String>,
    id_token: Option<String>,
    refresh_token: Option<String>,
}

fn non_empty_string(value: Option<&String>) -> bool {
    value.is_some_and(|value| !value.trim().is_empty())
}

/// 只判断本地是否保存了可识别的认证凭据。这里不解码、复制或输出任何 Token 内容。
fn login_status_from_auth_path(path: &Path) -> CodexLoginStatus {
    let contents = match fs::read(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return CodexLoginStatus::LoggedOut
        }
        Err(_) => return CodexLoginStatus::Unknown,
    };
    let mut document: LocalAuthDocument = match serde_json::from_slice(&contents) {
        Ok(document) => document,
        Err(_) => return CodexLoginStatus::Unknown,
    };
    let api_key_present = non_empty_string(document.api_key.as_ref());
    let oauth_token_present = document.tokens.as_ref().is_some_and(|tokens| {
        non_empty_string(tokens.access_token.as_ref())
            || non_empty_string(tokens.id_token.as_ref())
            || non_empty_string(tokens.refresh_token.as_ref())
    });
    let login_status = if api_key_present || oauth_token_present {
        CodexLoginStatus::LoggedIn
    } else {
        // 文件格式发生变化时不误报为退出登录，交给界面显示“无法判断”。
        CodexLoginStatus::Unknown
    };
    if let Some(api_key) = document.api_key.as_mut() {
        api_key.zeroize();
    }
    if let Some(tokens) = document.tokens.as_mut() {
        for token in [
            tokens.access_token.as_mut(),
            tokens.id_token.as_mut(),
            tokens.refresh_token.as_mut(),
        ]
        .into_iter()
        .flatten()
        {
            token.zeroize();
        }
    }
    login_status
}

fn normalized_path(path: &Path) -> String {
    path.to_string_lossy()
        .replace('/', "\\")
        .to_ascii_lowercase()
}

fn is_desktop_package_path(path: &Path) -> bool {
    let path = normalized_path(path);
    path.contains("\\windowsapps\\openai.codex_")
}

fn application_status(installed: bool, running: bool) -> CodexApplicationStatus {
    match (installed, running) {
        (_, true) => CodexApplicationStatus::InstalledRunning,
        (true, false) => CodexApplicationStatus::InstalledStopped,
        (false, false) => CodexApplicationStatus::NotInstalled,
    }
}

fn process_has_ancestor(system: &System, process_id: Pid, ancestor_ids: &HashSet<Pid>) -> bool {
    let mut current = system
        .process(process_id)
        .and_then(|process| process.parent());
    let mut visited = HashSet::new();
    while let Some(parent_id) = current {
        if ancestor_ids.contains(&parent_id) {
            return true;
        }
        if !visited.insert(parent_id) {
            break;
        }
        current = system
            .process(parent_id)
            .and_then(|process| process.parent());
    }
    false
}

#[cfg(windows)]
fn desktop_app_installed() -> bool {
    use std::os::windows::process::CommandExt;

    // AppX 没有稳定的普通文件别名，通过包注册信息判断关闭状态下是否仍已安装。
    let mut command = Command::new("powershell.exe");
    command.args([
        "-NoLogo",
        "-NoProfile",
        "-NonInteractive",
        "-Command",
        "if (Get-AppxPackage -Name OpenAI.Codex -ErrorAction SilentlyContinue) { exit 0 } else { exit 1 }",
    ]);
    command.creation_flags(0x0800_0000);
    command.status().is_ok_and(|status| status.success())
}

#[cfg(not(windows))]
fn desktop_app_installed() -> bool {
    false
}

#[cfg(windows)]
fn cli_installed() -> bool {
    use std::os::windows::process::CommandExt;

    let mut command = Command::new("where.exe");
    command.arg("codex");
    command.creation_flags(0x0800_0000);
    command.output().is_ok_and(|output| {
        output.status.success()
            && String::from_utf8_lossy(&output.stdout)
                .lines()
                .map(Path::new)
                .any(|path| !is_desktop_package_path(path))
    })
}

#[cfg(not(windows))]
fn cli_installed() -> bool {
    false
}

pub fn environment_status() -> CodexEnvironmentStatus {
    let mut system = System::new_all();
    system.refresh_all();

    let desktop_process_ids: HashSet<Pid> = system
        .processes()
        .iter()
        .filter_map(|(id, process)| {
            let executable = process.exe()?;
            (process.name().eq_ignore_ascii_case("ChatGPT.exe")
                && is_desktop_package_path(executable))
            .then_some(*id)
        })
        .collect();
    let desktop_running = !desktop_process_ids.is_empty();

    // 桌面端会启动自己的 codex.exe。通过安装路径和父进程链排除它，只保留独立 CLI。
    let cli_running = system.processes().iter().any(|(id, process)| {
        let process_name = process.name().to_string_lossy();
        let is_codex_process = process_name.eq_ignore_ascii_case("codex.exe")
            || process_name.eq_ignore_ascii_case("codex");
        is_codex_process
            && !process.exe().is_some_and(is_desktop_package_path)
            && !process_has_ancestor(&system, *id, &desktop_process_ids)
    });

    let desktop_installed = desktop_running || desktop_app_installed();
    let cli_installed = cli_running || cli_installed();
    let login_status = codex_home()
        .map(|home| login_status_from_auth_path(&home.join("auth.json")))
        .unwrap_or(CodexLoginStatus::Unknown);

    CodexEnvironmentStatus {
        desktop_status: application_status(desktop_installed, desktop_running),
        cli_status: application_status(cli_installed, cli_running),
        login_status,
    }
}

pub fn integration_state(
    configuration: &GatewayCodexConfiguration,
    environment: CodexEnvironmentStatus,
) -> CodexIntegrationState {
    let (current_mode, gateway_configured) = config_path()
        .and_then(|path| read_config(&path))
        .map(|parsed| {
            let (mode, _) = detected_mode(&parsed.document);
            let gateway_current = mode == CodexConnectionMode::Gateway
                && gateway_configuration_current(&parsed.document, configuration);
            (mode, gateway_current)
        })
        .unwrap_or((CodexConnectionMode::External, false));

    CodexIntegrationState {
        desktop_status: environment.desktop_status,
        cli_status: environment.cli_status,
        login_status: environment.login_status,
        current_mode,
        gateway_configured,
    }
}

pub fn switch_mode(
    requested: CodexConnectionMode,
    configuration: &GatewayCodexConfiguration,
) -> Result<SwitchCodexModeResult, String> {
    if requested == CodexConnectionMode::External {
        return Err("external 是检测状态，不能作为切换目标".to_string());
    }
    let _guard = CONFIG_WRITE_LOCK
        .lock()
        .map_err(|_| "Codex 配置写入锁已损坏".to_string())?;
    let path = config_path()?;
    let mut parsed = read_config(&path)?;
    let original_contents = parsed.document.to_string();
    let (previous_mode, _) = detected_mode(&parsed.document);
    match requested {
        CodexConnectionMode::Official => apply_official(&mut parsed.document)?,
        CodexConnectionMode::Gateway => apply_gateway(&mut parsed.document, configuration)?,
        CodexConnectionMode::External => unreachable!(),
    }
    let changed = parsed.document.to_string() != original_contents;
    if changed {
        write_config(&path, &parsed.hash, &parsed.document)?;
    }
    Ok(SwitchCodexModeResult {
        previous_mode,
        current_mode: requested,
        changed,
        restart_required: changed,
        warning_messages: Vec::new(),
    })
}

pub fn repair_gateway_configuration(
    configuration: &GatewayCodexConfiguration,
) -> Result<SwitchCodexModeResult, String> {
    switch_mode(CodexConnectionMode::Gateway, configuration)
}

pub fn update_managed_gateway_configuration(
    old_configuration: &GatewayCodexConfiguration,
    new_configuration: &GatewayCodexConfiguration,
) -> Result<Option<String>, String> {
    let _guard = CONFIG_WRITE_LOCK
        .lock()
        .map_err(|_| "Codex 配置写入锁已损坏".to_string())?;
    let path = config_path()?;
    let mut parsed = read_config(&path)?;
    let (mode, _) = detected_mode(&parsed.document);
    if mode != CodexConnectionMode::Gateway {
        return Ok(None);
    }
    if !gateway_configuration_current(&parsed.document, old_configuration) {
        return Err(
            "Codex 的 AI Gateway 配置已被外部修改；端口保存已取消，请先在“Codex 接入”页面修复配置"
                .to_string(),
        );
    }
    apply_gateway(&mut parsed.document, new_configuration)?;
    let hash = write_config(&path, &parsed.hash, &parsed.document)?;
    Ok(Some(hash))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as TestMutex;

    static ENV_LOCK: TestMutex<()> = TestMutex::new(());

    fn with_temp_codex_home(test: impl FnOnce(&Path)) {
        let _guard = ENV_LOCK.lock().unwrap();
        let path = env::temp_dir().join(format!(
            "ai-gateway-codex-test-{}",
            Utc::now().timestamp_nanos_opt().unwrap()
        ));
        fs::create_dir_all(&path).unwrap();
        let previous = env::var_os("CODEX_HOME");
        env::set_var("CODEX_HOME", &path);
        test(&path);
        match previous {
            Some(value) => env::set_var("CODEX_HOME", value),
            None => env::remove_var("CODEX_HOME"),
        }
        fs::remove_dir_all(path).unwrap();
    }

    fn gateway(port: u16, token: &str) -> GatewayCodexConfiguration {
        GatewayCodexConfiguration {
            port,
            local_token: token.to_string(),
        }
    }

    #[test]
    fn gateway_and_official_switch_preserve_unrelated_configuration() {
        with_temp_codex_home(|home| {
            fs::write(home.join("config.toml"), "# keep\nmodel = \"gpt-5\"\n[mcp_servers.demo]\ncommand = \"demo\"\n[model_providers.other]\nname = \"Other\"\nbase_url = \"https://example.com/v1\"\n").unwrap();
            switch_mode(CodexConnectionMode::Gateway, &gateway(9988, "local-secret")).unwrap();
            let managed = fs::read_to_string(home.join("config.toml")).unwrap();
            assert!(managed.contains("# keep"));
            assert!(managed.contains("X-AI-Gateway-Token"));
            assert!(managed.contains("model_provider = \"custom\""));
            assert!(managed.contains("requires_openai_auth = true"));

            switch_mode(
                CodexConnectionMode::Official,
                &gateway(9988, "local-secret"),
            )
            .unwrap();
            let official = fs::read_to_string(home.join("config.toml")).unwrap();
            assert!(official.contains("mcp_servers.demo"));
            assert!(official.contains("model_providers.other"));
            assert!(official.contains("model_providers.custom"));
            assert!(official.contains("name = \"OpenAI\""));
            assert!(official.contains("requires_openai_auth = true"));
            assert!(!official.contains("base_url"));
        });
    }

    #[test]
    fn existing_foreign_custom_provider_is_detected_before_explicit_takeover() {
        with_temp_codex_home(|home| {
            fs::write(home.join("config.toml"), "model_provider = \"custom\"\n[model_providers.custom]\nname = \"CC-Switch\"\nbase_url = \"http://127.0.0.1:15721/v1\"\nwire_api = \"responses\"\nrequires_openai_auth = true\n").unwrap();
            let state = integration_state(
                &gateway(8765, "token"),
                CodexEnvironmentStatus {
                    desktop_status: CodexApplicationStatus::NotInstalled,
                    cli_status: CodexApplicationStatus::NotInstalled,
                    login_status: CodexLoginStatus::Unknown,
                },
            );
            assert_eq!(state.current_mode, CodexConnectionMode::External);

            // 用户点击模式按钮即构成明确接管，之后配置固定为共享 custom Provider。
            switch_mode(CodexConnectionMode::Gateway, &gateway(8765, "token")).unwrap();
            let updated = fs::read_to_string(home.join("config.toml")).unwrap();
            assert!(updated.contains("name = \"AI Gateway\""));
        });
    }

    #[test]
    fn auth_json_is_never_touched() {
        with_temp_codex_home(|home| {
            fs::write(home.join("auth.json"), "sentinel-auth").unwrap();
            switch_mode(CodexConnectionMode::Gateway, &gateway(8765, "token")).unwrap();
            switch_mode(CodexConnectionMode::Official, &gateway(8765, "token")).unwrap();
            assert_eq!(
                fs::read_to_string(home.join("auth.json")).unwrap(),
                "sentinel-auth"
            );
        });
    }

    #[test]
    fn external_provider_is_detected_without_being_modified() {
        with_temp_codex_home(|home| {
            let original = "model_provider = \"cc_switch\"\n[model_providers.cc_switch]\nname = \"CC-Switch\"\n";
            fs::write(home.join("config.toml"), original).unwrap();

            let state = integration_state(
                &gateway(8765, "token"),
                CodexEnvironmentStatus {
                    desktop_status: CodexApplicationStatus::NotInstalled,
                    cli_status: CodexApplicationStatus::NotInstalled,
                    login_status: CodexLoginStatus::Unknown,
                },
            );

            assert_eq!(state.current_mode, CodexConnectionMode::External);
            assert_eq!(
                fs::read_to_string(home.join("config.toml")).unwrap(),
                original
            );
        });
    }

    #[test]
    fn managed_configuration_updates_port_and_token() {
        with_temp_codex_home(|home| {
            let old = gateway(8765, "old-token");
            let new = gateway(9876, "new-token");
            switch_mode(CodexConnectionMode::Gateway, &old).unwrap();

            let hash = update_managed_gateway_configuration(&old, &new).unwrap();
            let updated = fs::read_to_string(home.join("config.toml")).unwrap();

            assert!(hash.is_some());
            assert!(updated.contains("http://127.0.0.1:9876/v1"));
            assert!(updated.contains("new-token"));
            assert!(!updated.contains("old-token"));
        });
    }

    #[test]
    fn auth_status_is_read_only_and_recognizes_saved_oauth_tokens() {
        with_temp_codex_home(|home| {
            let auth_path = home.join("auth.json");
            let contents = br#"{"auth_mode":"chatgpt","tokens":{"access_token":"secret"}}"#;
            fs::write(&auth_path, contents).unwrap();
            let modified_before = fs::metadata(&auth_path).unwrap().modified().unwrap();

            assert_eq!(
                login_status_from_auth_path(&auth_path),
                CodexLoginStatus::LoggedIn
            );
            assert_eq!(fs::read(&auth_path).unwrap(), contents);
            assert_eq!(
                fs::metadata(&auth_path).unwrap().modified().unwrap(),
                modified_before
            );
        });
    }

    #[test]
    fn auth_status_distinguishes_missing_and_unrecognized_files() {
        with_temp_codex_home(|home| {
            let auth_path = home.join("auth.json");
            assert_eq!(
                login_status_from_auth_path(&auth_path),
                CodexLoginStatus::LoggedOut
            );

            fs::write(&auth_path, br#"{"auth_mode":"chatgpt"}"#).unwrap();
            assert_eq!(
                login_status_from_auth_path(&auth_path),
                CodexLoginStatus::Unknown
            );
            fs::write(&auth_path, b"not-json").unwrap();
            assert_eq!(
                login_status_from_auth_path(&auth_path),
                CodexLoginStatus::Unknown
            );
        });
    }

    #[test]
    fn application_status_and_desktop_paths_are_classified_without_cli_confusion() {
        assert_eq!(
            application_status(false, false),
            CodexApplicationStatus::NotInstalled
        );
        assert_eq!(
            application_status(true, false),
            CodexApplicationStatus::InstalledStopped
        );
        assert_eq!(
            application_status(true, true),
            CodexApplicationStatus::InstalledRunning
        );
        assert!(is_desktop_package_path(Path::new(
            r"C:\Program Files\WindowsApps\OpenAI.Codex_1.0_x64\app\resources\codex.exe"
        )));
        assert!(!is_desktop_package_path(Path::new(
            r"C:\Users\test\AppData\Roaming\npm\codex.exe"
        )));
    }

    #[test]
    fn config_backups_keep_only_the_latest_ten() {
        with_temp_codex_home(|home| {
            fs::write(home.join("config.toml"), "model = \"gpt-5\"\n").unwrap();
            for index in 0..12 {
                let old = gateway(8765 + index, &format!("token-{index}"));
                if index == 0 {
                    switch_mode(CodexConnectionMode::Gateway, &old).unwrap();
                } else {
                    let previous = gateway(8765 + index - 1, &format!("token-{}", index - 1));
                    update_managed_gateway_configuration(&previous, &old).unwrap();
                }
            }

            let backup_count = fs::read_dir(home)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| {
                    entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with("config.toml.ai-gateway-")
                })
                .count();
            assert_eq!(backup_count, CONFIG_BACKUP_LIMIT);
        });
    }

    #[test]
    fn rewriting_session_changes_only_first_line_provider() {
        let original = b"{\"type\":\"session_meta\",\"payload\":{\"session_id\":\"thread-1\",\"model_provider\":\"openai\"}}\r\n{\"type\":\"response_item\",\"payload\":{\"text\":\"keep exactly\"}}\r\n";
        let rewritten = rewrite_session_provider(original, MANAGED_PROVIDER_ID).unwrap();
        let original_tail =
            &original[original.iter().position(|byte| *byte == b'\n').unwrap() + 1..];
        let rewritten_tail =
            &rewritten[rewritten.iter().position(|byte| *byte == b'\n').unwrap() + 1..];
        assert_eq!(rewritten_tail, original_tail);
        assert!(String::from_utf8(rewritten)
            .unwrap()
            .contains("\"model_provider\":\"custom\""));
    }
}
