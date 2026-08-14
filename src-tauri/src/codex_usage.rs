use super::{setting, sqlite_token, CapturedUsage, Store};
use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
use rusqlite::{params, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    env,
    fs::{self, File},
    io::{BufRead, BufReader, Seek, SeekFrom},
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};

const MAX_CODEX_JSONL_LINE_BYTES: usize = 2 * 1024 * 1024;
const CODEX_USAGE_PARSER_VERSION: u32 = 2;
const CODEX_USAGE_PARSER_VERSION_KEY: &str = "codex_usage_parser_version";
pub(crate) const OFFICIAL_PROVIDER_ID: &str = "_openai_official";
pub const OFFICIAL_PROVIDER_NAME: &str = "OpenAI 官方";
pub(crate) const UNCONFIRMED_PROVIDER_ID: &str = "_codex_unconfirmed";
const UNCONFIRMED_PROVIDER_NAME: &str = "Codex 历史（来源未确认）";

#[derive(Clone, Default, Serialize)]
pub struct CodexUsageSyncStatus {
    pub state: String,
    pub last_synced_at: Option<String>,
    pub roots_scanned: u64,
    pub files_scanned: u64,
    pub token_events_found: u64,
    pub imported_count: u64,
    pub matched_count: u64,
    pub skipped_count: u64,
    pub deferred_count: u64,
    pub parser_version: u32,
    pub rebuild_performed: bool,
    pub warning_messages: Vec<String>,
}

#[derive(Default)]
struct SyncSummary {
    roots_scanned: u64,
    files_scanned: u64,
    token_events_found: u64,
    imported_count: u64,
    matched_count: u64,
    skipped_count: u64,
    deferred_count: u64,
    warning_messages: Vec<String>,
}

#[derive(Clone, Default, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct TokenCounters {
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    #[serde(alias = "cached_tokens", alias = "cache_read_tokens")]
    cached_input_tokens: Option<u64>,
    cache_read_input_tokens: Option<u64>,
    #[serde(alias = "cache_write_tokens", alias = "cache_creation_tokens")]
    cache_write_input_tokens: Option<u64>,
    #[serde(alias = "reasoning_tokens")]
    reasoning_output_tokens: Option<u64>,
    total_tokens: Option<u64>,
}

impl TokenCounters {
    fn cache_read(&self) -> Option<u64> {
        self.cached_input_tokens.or(self.cache_read_input_tokens)
    }

    fn captured(&self) -> CapturedUsage {
        // Codex/OpenAI 的 input_tokens 已包含缓存命中；缓存和推理均是细项，不能再次加总。
        CapturedUsage {
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            cache_read_tokens: self.cache_read(),
            cache_write_tokens: self.cache_write_input_tokens,
            reasoning_tokens: self.reasoning_output_tokens,
            total_tokens: self
                .input_tokens
                .zip(self.output_tokens)
                .map(|(input, output)| input.saturating_add(output))
                .or(self.total_tokens),
        }
    }

    fn delta_from(&self, previous: &Self) -> Option<Self> {
        // 累计值回退表示 Codex 开始了新的计数段；此时不能用饱和减法伪造零用量。
        let delta = |current: Option<u64>, old: Option<u64>| match (current, old) {
            (Some(current), Some(old)) if current >= old => Some(current - old),
            (Some(current), None) => Some(current),
            (None, _) => None,
            _ => return None,
        };
        Some(Self {
            input_tokens: delta(self.input_tokens, previous.input_tokens),
            output_tokens: delta(self.output_tokens, previous.output_tokens),
            cached_input_tokens: delta(self.cache_read(), previous.cache_read()),
            cache_read_input_tokens: None,
            cache_write_input_tokens: delta(
                self.cache_write_input_tokens,
                previous.cache_write_input_tokens,
            ),
            reasoning_output_tokens: delta(
                self.reasoning_output_tokens,
                previous.reasoning_output_tokens,
            ),
            total_tokens: delta(self.total_tokens, previous.total_tokens),
        })
    }
}

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default)]
struct UsageCursorState {
    high_water: Option<TokenCounters>,
    previous_signature: Option<TokenUsageSignature>,
    signatures_by_source: HashMap<String, TokenUsageSignature>,
    session_started_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
struct TokenUsageSignature {
    total: Option<TokenCounters>,
    last: Option<TokenCounters>,
}

#[derive(Clone, Default)]
struct FileCursor {
    file_identity: Option<String>,
    byte_offset: u64,
    line_offset: u64,
    usage_state: UsageCursorState,
    session_id: Option<String>,
    model_provider: Option<String>,
    current_model: Option<String>,
    parent_session_id: Option<String>,
}

#[derive(Clone)]
struct TokenEvent {
    timestamp: DateTime<Utc>,
    model: String,
    usage: CapturedUsage,
    event_key: String,
    has_official_rate_limits: bool,
}

#[derive(Clone)]
struct CodexRoot {
    path: PathBuf,
    id: String,
}

#[derive(Clone)]
struct CodexFile {
    path: PathBuf,
    cursor_key: String,
}

struct ParsedFile {
    cursor_key: String,
    identity: String,
    size: u64,
    modified: i64,
    cursor: FileCursor,
    events: Vec<TokenEvent>,
}

fn push_warning(summary: &mut SyncSummary, message: impl Into<String>) {
    if summary.warning_messages.len() < 20 {
        summary.warning_messages.push(message.into());
    }
}

fn status_from_summary(summary: SyncSummary, rebuild_performed: bool) -> CodexUsageSyncStatus {
    CodexUsageSyncStatus {
        state: if summary.warning_messages.is_empty() {
            "success".to_string()
        } else {
            "warning".to_string()
        },
        last_synced_at: Some(Utc::now().to_rfc3339()),
        roots_scanned: summary.roots_scanned,
        files_scanned: summary.files_scanned,
        token_events_found: summary.token_events_found,
        imported_count: summary.imported_count,
        matched_count: summary.matched_count,
        skipped_count: summary.skipped_count,
        deferred_count: summary.deferred_count,
        parser_version: CODEX_USAGE_PARSER_VERSION,
        rebuild_performed,
        warning_messages: summary.warning_messages,
    }
}

fn normalized_path_key(path: &Path) -> String {
    fs::canonicalize(path)
        .unwrap_or_else(|_| path.to_path_buf())
        .to_string_lossy()
        .replace('\\', "/")
        .to_lowercase()
}

fn discover_roots() -> Vec<CodexRoot> {
    let mut candidates = Vec::new();
    if let Some(profile) = env::var_os("USERPROFILE") {
        candidates.push(PathBuf::from(profile).join(".codex"));
    }
    if let Some(custom) = env::var_os("CODEX_HOME") {
        candidates.push(PathBuf::from(custom));
    }
    let mut seen = HashSet::new();
    candidates
        .into_iter()
        .filter(|path| path.is_dir())
        .filter_map(|path| {
            let key = normalized_path_key(&path);
            if !seen.insert(key.clone()) {
                return None;
            }
            Some(CodexRoot {
                path,
                id: format!("{:x}", Sha256::digest(key.as_bytes()))[..16].to_string(),
            })
        })
        .collect()
}

fn collect_files(roots: &[CodexRoot]) -> Result<Vec<CodexFile>, String> {
    let mut result = Vec::new();
    for root in roots {
        let mut paths = Vec::new();
        collect_jsonl_files(&root.path.join("sessions"), true, &mut paths)?;
        collect_jsonl_files(&root.path.join("archived_sessions"), false, &mut paths)?;
        for path in paths {
            let relative = relative_path(&root.path, &path);
            result.push(CodexFile {
                path,
                cursor_key: format!("{}:{relative}", root.id),
            });
        }
    }
    result.sort_by(|left, right| left.cursor_key.cmp(&right.cursor_key));
    Ok(result)
}

pub fn sync_current(store: &Store, retention_days: u16) -> Result<CodexUsageSyncStatus, String> {
    let version = setting(store, CODEX_USAGE_PARSER_VERSION_KEY)?
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or_default();
    if version < CODEX_USAGE_PARSER_VERSION {
        rebuild(store, retention_days)
    } else {
        sync(store, retention_days)
    }
}

pub fn sync(store: &Store, retention_days: u16) -> Result<CodexUsageSyncStatus, String> {
    let roots = discover_roots();
    let files = collect_files(&roots)?;
    let known_session_ids = files
        .iter()
        .filter_map(|file| session_id_from_filename(&file.path))
        .collect::<HashSet<_>>();

    let mut summary = SyncSummary {
        roots_scanned: roots.len() as u64,
        ..Default::default()
    };
    if roots.is_empty() {
        push_warning(&mut summary, "未找到 Codex Desktop 本地数据目录");
    }
    for file in files {
        summary.files_scanned += 1;
        if let Err(error) = sync_file(
            store,
            &file,
            retention_days,
            &known_session_ids,
            &mut summary,
        ) {
            summary.deferred_count += 1;
            push_warning(&mut summary, error);
        }
    }
    Ok(status_from_summary(summary, false))
}

fn collect_jsonl_files(
    root: &Path,
    recursive: bool,
    output: &mut Vec<PathBuf>,
) -> Result<(), String> {
    if !root.exists() {
        return Ok(());
    }
    let entries = fs::read_dir(root)
        .map_err(|error| format!("无法扫描 Codex 会话目录 {}：{error}", root.display()))?;
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        let path = entry.path();
        if path.is_dir() && recursive {
            collect_jsonl_files(&path, true, output)?;
        } else if path.extension().and_then(|value| value.to_str()) == Some("jsonl") {
            output.push(path);
        }
    }
    Ok(())
}

fn modified_millis(metadata: &fs::Metadata) -> i64 {
    metadata
        .modified()
        .ok()
        .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
        .map(|value| value.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}

fn relative_path(home: &Path, path: &Path) -> String {
    path.strip_prefix(home)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn session_id_from_filename(path: &Path) -> Option<String> {
    let stem = path.file_stem()?.to_str()?;
    let candidate = stem.get(stem.len().checked_sub(36)?..)?;
    uuid::Uuid::parse_str(candidate)
        .ok()
        .map(|value| value.hyphenated().to_string())
}

fn file_identity(path: &Path) -> String {
    // 事件唯一键负责跨目录移动去重；文件身份只用于判断同一路径是否被重建。
    let created = fs::metadata(path)
        .ok()
        .and_then(|metadata| metadata.created().ok())
        .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
        .map(|value| value.as_millis())
        .unwrap_or_default();
    format!(
        "{}:{created}",
        path.file_name()
            .and_then(|value| value.to_str())
            .unwrap_or_default()
    )
}

fn load_cursor(connection: &rusqlite::Connection, relative: &str) -> Result<FileCursor, String> {
    connection
        .query_row(
            "SELECT file_identity,byte_offset,line_offset,last_total_json,session_id,model_provider,
                    current_model,parent_session_id
             FROM codex_usage_sync_state WHERE relative_path=?1",
            [relative],
            |row| {
                let state_json: Option<String> = row.get(3)?;
                let usage_state = state_json
                    .as_deref()
                    .and_then(|value| serde_json::from_str::<UsageCursorState>(value).ok())
                    .or_else(|| {
                        state_json
                            .as_deref()
                            .and_then(|value| serde_json::from_str::<TokenCounters>(value).ok())
                            .map(|high_water| UsageCursorState {
                                high_water: Some(high_water),
                                ..Default::default()
                            })
                    })
                    .unwrap_or_default();
                Ok(FileCursor {
                    file_identity: row.get(0)?,
                    byte_offset: row.get::<_, i64>(1)?.max(0) as u64,
                    line_offset: row.get::<_, i64>(2)?.max(0) as u64,
                    usage_state,
                    session_id: row.get(4)?,
                    model_provider: row.get(5)?,
                    current_model: row.get(6)?,
                    parent_session_id: row.get(7)?,
                })
            },
        )
        .optional()
        .map(|value| value.unwrap_or_default())
        .map_err(|error| format!("无法读取 Codex 用量游标：{error}"))
}

fn parse_timestamp(value: Option<&str>) -> Option<DateTime<Utc>> {
    value
        .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
        .map(|value| value.with_timezone(&Utc))
}

fn string_field(value: &serde_json::Value, names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        value
            .get(*name)
            .and_then(serde_json::Value::as_str)
            .filter(|text| !text.is_empty())
            .map(str::to_string)
    })
}

fn parse_parent(payload: &serde_json::Value) -> Result<Option<String>, String> {
    let forked = string_field(payload, &["forked_from_id"]);
    let spawned = payload
        .get("source")
        .and_then(|source| source.get("subagent"))
        .and_then(|subagent| subagent.get("thread_spawn"))
        .and_then(|spawn| string_field(spawn, &["parent_thread_id"]));
    match (forked, spawned) {
        (None, None) => Ok(None),
        (Some(parent), None) | (None, Some(parent)) => Ok(Some(parent)),
        (Some(forked), Some(spawned)) if forked == spawned => Ok(Some(forked)),
        (Some(_), Some(_)) => Err("Codex 子会话的父线程元数据不一致".to_string()),
    }
}

fn parse_counters(value: Option<&serde_json::Value>) -> Option<TokenCounters> {
    let value = value?.as_object()?;
    let token = |names: &[&str]| {
        names
            .iter()
            .find_map(|name| value.get(*name).and_then(serde_json::Value::as_u64))
    };
    let counters = TokenCounters {
        input_tokens: token(&["input_tokens"]),
        output_tokens: token(&["output_tokens"]),
        cached_input_tokens: token(&[
            "cached_input_tokens",
            "cache_read_input_tokens",
            "cached_tokens",
            "cache_read_tokens",
        ]),
        cache_read_input_tokens: None,
        cache_write_input_tokens: token(&[
            "cache_write_input_tokens",
            "cache_write_tokens",
            "cache_creation_tokens",
        ]),
        reasoning_output_tokens: token(&["reasoning_output_tokens", "reasoning_tokens"]),
        total_tokens: token(&["total_tokens"]),
    };
    (counters.input_tokens.is_some()
        || counters.output_tokens.is_some()
        || counters.cached_input_tokens.is_some()
        || counters.total_tokens.is_some())
    .then_some(counters)
}

fn fallback_timestamp(path: &Path, metadata: &fs::Metadata) -> DateTime<Utc> {
    let normalized = path.to_string_lossy().replace('\\', "/");
    let parts = normalized.split('/').collect::<Vec<_>>();
    if let Some(index) = parts.iter().position(|part| *part == "sessions") {
        let date = parts
            .get(index + 1)
            .and_then(|value| value.parse::<i32>().ok())
            .zip(
                parts
                    .get(index + 2)
                    .and_then(|value| value.parse::<u32>().ok()),
            )
            .zip(
                parts
                    .get(index + 3)
                    .and_then(|value| value.parse::<u32>().ok()),
            )
            .and_then(|((year, month), day)| NaiveDate::from_ymd_opt(year, month, day));
        if let Some(timestamp) = date.and_then(|date| date.and_hms_opt(12, 0, 0)) {
            return Utc.from_utc_datetime(&timestamp);
        }
    }
    metadata
        .modified()
        .ok()
        .map(DateTime::<Utc>::from)
        .unwrap_or_else(Utc::now)
}

fn update_session_metadata(
    record: &serde_json::Value,
    cursor: &mut FileCursor,
) -> Result<(), String> {
    let payload = record.get("payload").unwrap_or(&serde_json::Value::Null);
    if let Some(id) = string_field(payload, &["id", "session_id", "thread_id", "threadId"]) {
        if cursor
            .session_id
            .as_ref()
            .is_none_or(|current| current == &id)
        {
            cursor.session_id = Some(id);
        }
    }
    cursor.model_provider =
        string_field(payload, &["model_provider"]).or(cursor.model_provider.take());
    let parent = parse_parent(payload)?;
    cursor.parent_session_id = parent.or(cursor.parent_session_id.take());
    cursor.usage_state.session_started_at =
        parse_timestamp(record.get("timestamp").and_then(serde_json::Value::as_str))
            .or_else(|| parse_timestamp(string_field(payload, &["timestamp"]).as_deref()));
    Ok(())
}

fn update_model(record: &serde_json::Value, cursor: &mut FileCursor) {
    let payload = record.get("payload").unwrap_or(&serde_json::Value::Null);
    cursor.current_model = string_field(payload, &["model"])
        .or_else(|| {
            payload
                .get("info")
                .and_then(|info| string_field(info, &["model"]))
        })
        .or(cursor.current_model.take());
}

fn sync_file(
    store: &Store,
    file: &CodexFile,
    retention_days: u16,
    known_session_ids: &HashSet<String>,
    summary: &mut SyncSummary,
) -> Result<(), String> {
    let parsed = parse_file(
        store,
        file,
        retention_days,
        known_session_ids,
        summary,
        true,
    )?;
    let mut connection = store
        .connection
        .lock()
        .map_err(|_| "数据库锁已损坏".to_string())?;
    let transaction = connection
        .transaction()
        .map_err(|error| format!("无法开始 Codex 用量同步事务：{error}"))?;
    persist_parsed_file(&transaction, &parsed, summary)?;
    transaction
        .commit()
        .map_err(|error| format!("无法提交 Codex 用量同步：{error}"))
}

fn parse_file(
    store: &Store,
    file: &CodexFile,
    retention_days: u16,
    known_session_ids: &HashSet<String>,
    summary: &mut SyncSummary,
    incremental: bool,
) -> Result<ParsedFile, String> {
    let metadata =
        fs::metadata(&file.path).map_err(|_| "Codex 会话文件暂时不可读取".to_string())?;
    let identity = file_identity(&file.path);
    let modified = modified_millis(&metadata);
    let mut cursor = if incremental {
        let connection = store
            .connection
            .lock()
            .map_err(|_| "数据库锁已损坏".to_string())?;
        load_cursor(&connection, &file.cursor_key)?
    } else {
        FileCursor::default()
    };
    if metadata.len() < cursor.byte_offset
        || cursor
            .file_identity
            .as_deref()
            .is_some_and(|stored| stored != identity)
    {
        cursor = FileCursor::default();
    }
    cursor.session_id = cursor
        .session_id
        .or_else(|| session_id_from_filename(&file.path));
    let mut source = File::open(&file.path).map_err(|_| "Codex 会话文件正在被占用".to_string())?;
    source
        .seek(SeekFrom::Start(cursor.byte_offset))
        .map_err(|error| format!("无法定位 Codex 会话增量：{error}"))?;
    let mut reader = BufReader::new(source);
    let cutoff = Utc::now() - Duration::days(i64::from(retention_days));
    let fallback_time = fallback_timestamp(&file.path, &metadata);
    let mut line = Vec::new();
    let mut events = Vec::new();
    loop {
        line.clear();
        let read = reader
            .read_until(b'\n', &mut line)
            .map_err(|error| format!("读取 Codex 会话失败：{error}"))?;
        if read == 0 {
            break;
        }
        // 末尾没有换行通常代表 Codex 正在写这一行，游标停在行首等待下一轮。
        if line.last() != Some(&b'\n') {
            break;
        }
        if line.len() > MAX_CODEX_JSONL_LINE_BYTES {
            cursor.byte_offset = cursor.byte_offset.saturating_add(read as u64);
            cursor.line_offset = cursor.line_offset.saturating_add(1);
            summary.skipped_count += 1;
            push_warning(summary, "部分 Codex 记录超过 2 MiB，已跳过且不读取内容");
            continue;
        }
        let record: serde_json::Value = match serde_json::from_slice(&line) {
            Ok(value) => value,
            // 已完整换行的损坏记录不会在下一轮自行恢复，跳过后继续处理后续有效事件。
            Err(_) => {
                cursor.byte_offset = cursor.byte_offset.saturating_add(read as u64);
                cursor.line_offset = cursor.line_offset.saturating_add(1);
                summary.skipped_count += 1;
                push_warning(summary, "部分 Codex JSONL 记录无法解析，已跳过");
                continue;
            }
        };
        cursor.byte_offset = cursor.byte_offset.saturating_add(read as u64);
        cursor.line_offset = cursor.line_offset.saturating_add(1);
        let kind = record
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        match kind {
            "session_meta" => {
                update_session_metadata(&record, &mut cursor)?;
                if let Some(parent) = cursor.parent_session_id.as_ref() {
                    if !known_session_ids.contains(parent) {
                        return Err("Codex 子会话的父线程暂时不可用，已延后同步".to_string());
                    }
                }
            }
            "turn_context" => update_model(&record, &mut cursor),
            _ => {}
        }
        if kind != "event_msg" {
            continue;
        }
        let payload = record.get("payload").unwrap_or(&serde_json::Value::Null);
        if payload.get("type").and_then(serde_json::Value::as_str) != Some("token_count") {
            continue;
        }
        let Some(info) = payload.get("info").filter(|value| !value.is_null()) else {
            summary.skipped_count += 1;
            continue;
        };
        let total = parse_counters(info.get("total_token_usage"));
        let last = parse_counters(info.get("last_token_usage"));
        if total.is_none() && last.is_none() {
            summary.skipped_count += 1;
            continue;
        }
        summary.token_events_found += 1;
        cursor.current_model = string_field(info, &["model", "model_name"])
            .or_else(|| string_field(payload, &["model"]))
            .or(cursor.current_model.take());
        let signature = TokenUsageSignature {
            total: total.clone(),
            last: last.clone(),
        };
        let source_key = payload
            .get("rate_limits")
            .and_then(|limits| string_field(limits, &["limit_id"]))
            .unwrap_or_else(|| "_default".to_string());
        let duplicate = total.is_some()
            && (cursor.usage_state.signatures_by_source.get(&source_key) == Some(&signature)
                || cursor.usage_state.previous_signature.as_ref() == Some(&signature));
        if total.is_some() {
            cursor
                .usage_state
                .signatures_by_source
                .insert(source_key, signature.clone());
        }
        cursor.usage_state.previous_signature = Some(signature.clone());
        if duplicate {
            summary.skipped_count += 1;
            continue;
        }
        let counters = if let Some(last) =
            last.filter(|usage| usage.input_tokens.is_some() || usage.output_tokens.is_some())
        {
            last
        } else {
            let Some(current) = total.clone() else {
                summary.skipped_count += 1;
                continue;
            };
            match cursor.usage_state.high_water.as_ref() {
                Some(previous) => current
                    .delta_from(previous)
                    .unwrap_or_else(|| current.clone()),
                None => current.clone(),
            }
        };
        if let Some(total) = total {
            cursor.usage_state.high_water = Some(total);
        }
        let timestamp =
            parse_timestamp(record.get("timestamp").and_then(serde_json::Value::as_str))
                .or(cursor.usage_state.session_started_at)
                .unwrap_or(fallback_time);
        if record
            .get("timestamp")
            .and_then(serde_json::Value::as_str)
            .is_none()
        {
            push_warning(summary, "部分 Token 事件缺少时间，已使用会话或文件时间归属");
        }
        if cursor.parent_session_id.is_some()
            && cursor
                .usage_state
                .session_started_at
                .is_some_and(|started| timestamp <= started)
        {
            summary.skipped_count += 1;
            continue;
        }
        let usage = counters.captured();
        let has_tokens = [
            usage.input_tokens,
            usage.output_tokens,
            usage.cache_read_tokens,
            usage.cache_write_tokens,
            usage.reasoning_tokens,
            usage.total_tokens,
        ]
        .into_iter()
        .flatten()
        .any(|value| value > 0);
        if !usage.is_reported() || !has_tokens {
            summary.skipped_count += 1;
            continue;
        }
        if counters
            .total_tokens
            .zip(usage.total_tokens)
            .is_some_and(|(reported, normalized)| reported != normalized)
        {
            push_warning(
                summary,
                "部分 Codex total_tokens 与输入加输出不一致，已按拆分字段归一",
            );
        }
        let Some(session_id) = cursor.session_id.as_deref() else {
            return Err("Codex 会话缺少可识别的会话 ID，已延后同步".to_string());
        };
        let model = cursor
            .current_model
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        let event_signature =
            serde_json::to_vec(&(session_id, cursor.line_offset, &model, &signature))
                .map_err(|error| format!("无法生成 Codex 用量事件键：{error}"))?;
        if timestamp >= cutoff {
            events.push(TokenEvent {
                timestamp,
                model,
                usage,
                event_key: format!("{:x}", Sha256::digest(event_signature)),
                has_official_rate_limits: payload
                    .get("rate_limits")
                    .is_some_and(|limits| !limits.is_null()),
            });
        }
    }
    Ok(ParsedFile {
        cursor_key: file.cursor_key.clone(),
        identity,
        size: metadata.len(),
        modified,
        cursor,
        events,
    })
}

fn persist_parsed_file(
    transaction: &Transaction<'_>,
    parsed: &ParsedFile,
    summary: &mut SyncSummary,
) -> Result<(), String> {
    let mut imported = 0;
    let mut matched = 0;
    for event in &parsed.events {
        match persist_or_match_event(transaction, &parsed.cursor, event)? {
            PersistOutcome::Inserted => imported += 1,
            PersistOutcome::Matched => matched += 1,
            PersistOutcome::Duplicate => {}
        }
    }
    let state_json = serde_json::to_string(&parsed.cursor.usage_state)
        .map_err(|error| format!("无法保存 Codex 用量游标状态：{error}"))?;
    transaction
        .execute(
            "INSERT INTO codex_usage_sync_state(
                relative_path,file_identity,file_size,modified_at_ms,line_offset,byte_offset,
                last_total_json,session_id,model_provider,current_model,parent_session_id,
                last_synced_at,last_warning
             ) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,NULL)
             ON CONFLICT(relative_path) DO UPDATE SET
                file_identity=excluded.file_identity,file_size=excluded.file_size,
                modified_at_ms=excluded.modified_at_ms,line_offset=excluded.line_offset,
                byte_offset=excluded.byte_offset,last_total_json=excluded.last_total_json,
                session_id=excluded.session_id,model_provider=excluded.model_provider,
                current_model=excluded.current_model,parent_session_id=excluded.parent_session_id,
                last_synced_at=excluded.last_synced_at,last_warning=NULL",
            params![
                parsed.cursor_key,
                parsed.identity,
                i64::try_from(parsed.size).unwrap_or(i64::MAX),
                parsed.modified,
                i64::try_from(parsed.cursor.line_offset).unwrap_or(i64::MAX),
                i64::try_from(parsed.cursor.byte_offset).unwrap_or(i64::MAX),
                state_json,
                parsed.cursor.session_id,
                parsed.cursor.model_provider,
                parsed.cursor.current_model,
                parsed.cursor.parent_session_id,
                Utc::now().to_rfc3339(),
            ],
        )
        .map_err(|error| format!("无法保存 Codex 用量游标：{error}"))?;
    summary.imported_count += imported;
    summary.matched_count += matched;
    Ok(())
}

enum PersistOutcome {
    Inserted,
    Matched,
    Duplicate,
}

#[derive(Clone)]
struct Candidate {
    id: i64,
    completed_at: DateTime<Utc>,
    usage: CapturedUsage,
    reported: bool,
    token_source: String,
    estimate_method: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct PreviousGatewayUsage {
    usage: CapturedUsage,
    token_source: String,
    estimate_method: Option<String>,
}

fn same_tokens(left: &CapturedUsage, right: &CapturedUsage) -> bool {
    let compatible = |left: Option<u64>, right: Option<u64>| match (left, right) {
        (Some(left), Some(right)) => left == right,
        _ => true,
    };
    left.input_tokens == right.input_tokens
        && left.output_tokens == right.output_tokens
        && left.total_tokens == right.total_tokens
        && compatible(left.cache_read_tokens, right.cache_read_tokens)
        && compatible(left.cache_write_tokens, right.cache_write_tokens)
        && compatible(left.reasoning_tokens, right.reasoning_tokens)
}

fn gateway_candidates(
    transaction: &Transaction<'_>,
    event: &TokenEvent,
) -> Result<Vec<Candidate>, String> {
    let start = event.timestamp - Duration::minutes(10);
    let end = event.timestamp + Duration::minutes(10);
    let mut statement = transaction
        .prepare(
            "SELECT id,completed_at,input_tokens,output_tokens,cached_tokens,
                    cache_write_tokens,reasoning_tokens,total_tokens,usage_reported,token_source,
                    estimate_method
             FROM usage_records
             WHERE data_source='gateway_response' AND client_origin='codex'
               AND codex_event_key IS NULL AND model=?1 AND is_final_attempt=1
               AND outcome='completed'
               AND completed_at>=?2 AND completed_at<=?3",
        )
        .map_err(|error| format!("无法查询跨来源去重候选：{error}"))?;
    let rows = statement
        .query_map(
            params![event.model, start.to_rfc3339(), end.to_rfc3339()],
            |row| {
                let timestamp: String = row.get(1)?;
                Ok((
                    row.get::<_, i64>(0)?,
                    timestamp,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                    row.get::<_, i64>(8)? != 0,
                    row.get::<_, String>(9)?,
                    row.get::<_, Option<String>>(10)?,
                ))
            },
        )
        .map_err(|error| format!("无法读取跨来源去重候选：{error}"))?;
    let mut candidates = Vec::new();
    for row in rows {
        let (
            id,
            timestamp,
            input,
            output,
            read,
            write,
            reasoning,
            total,
            reported,
            token_source,
            estimate_method,
        ) = row.map_err(|error| error.to_string())?;
        let Ok(completed_at) = DateTime::parse_from_rfc3339(&timestamp) else {
            continue;
        };
        let nonnegative = |value: Option<i64>| value.and_then(|value| u64::try_from(value).ok());
        candidates.push(Candidate {
            id,
            completed_at: completed_at.with_timezone(&Utc),
            usage: CapturedUsage {
                input_tokens: nonnegative(input),
                output_tokens: nonnegative(output),
                cache_read_tokens: nonnegative(read),
                cache_write_tokens: nonnegative(write),
                reasoning_tokens: nonnegative(reasoning),
                total_tokens: nonnegative(total),
            },
            reported,
            token_source,
            estimate_method,
        });
    }
    Ok(candidates)
}

fn mode_at(
    transaction: &Transaction<'_>,
    timestamp: DateTime<Utc>,
) -> Result<Option<String>, String> {
    transaction
        .query_row(
            "SELECT mode FROM codex_mode_history WHERE changed_at<=?1
             ORDER BY changed_at DESC LIMIT 1",
            [timestamp.to_rfc3339()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| format!("无法读取 Codex 模式时间线：{error}"))
}

fn persist_or_match_event(
    transaction: &Transaction<'_>,
    cursor: &FileCursor,
    event: &TokenEvent,
) -> Result<PersistOutcome, String> {
    let exists: bool = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM usage_records WHERE codex_event_key=?1)",
            [&event.event_key],
            |row| row.get(0),
        )
        .map_err(|error| format!("无法检查 Codex 用量事件：{error}"))?;
    if exists {
        return Ok(PersistOutcome::Duplicate);
    }

    let candidates = gateway_candidates(transaction, event)?;
    let exact = candidates
        .iter()
        .filter(|candidate| {
            candidate.reported
                && candidate.token_source == "upstream"
                && same_tokens(&candidate.usage, &event.usage)
        })
        .collect::<Vec<_>>();
    let unreported = candidates
        .iter()
        .filter(|candidate| {
            (!candidate.reported || candidate.token_source == "estimated")
                && (candidate.completed_at - event.timestamp)
                    .num_seconds()
                    .abs()
                    <= 60
        })
        .collect::<Vec<_>>();
    let candidate = if exact.len() == 1 {
        exact.first().copied()
    } else if exact.is_empty() && unreported.len() == 1 {
        unreported.first().copied()
    } else {
        None
    };
    if let Some(candidate) = candidate {
        if candidate.token_source == "upstream" {
            transaction
                .execute(
                    "UPDATE usage_records SET codex_event_key=?1 WHERE id=?2",
                    params![event.event_key, candidate.id],
                )
                .map_err(|error| format!("无法关联 Codex 与网关用量：{error}"))?;
        } else {
            let previous = serde_json::to_string(&PreviousGatewayUsage {
                usage: candidate.usage.clone(),
                token_source: candidate.token_source.clone(),
                estimate_method: candidate.estimate_method.clone(),
            })
            .map_err(|error| format!("无法保存 Codex 补全前的用量快照：{error}"))?;
            transaction
                .execute(
                    "UPDATE usage_records SET input_tokens=?1,output_tokens=?2,cached_tokens=?3,
                        cache_write_tokens=?4,reasoning_tokens=?5,total_tokens=?6,
                        usage_reported=1,codex_event_key=?7,codex_usage_supplemented=1,
                        token_source='codex_session',estimate_method=NULL,
                        codex_previous_usage_json=?8 WHERE id=?9",
                    params![
                        sqlite_token(event.usage.input_tokens)?,
                        sqlite_token(event.usage.output_tokens)?,
                        sqlite_token(event.usage.cache_read_tokens)?,
                        sqlite_token(event.usage.cache_write_tokens)?,
                        sqlite_token(event.usage.reasoning_tokens)?,
                        sqlite_token(event.usage.total_tokens)?,
                        event.event_key,
                        previous,
                        candidate.id,
                    ],
                )
                .map_err(|error| format!("无法补全 Codex 网关用量：{error}"))?;
        }
        return Ok(PersistOutcome::Matched);
    }

    let official = cursor.model_provider.as_deref() == Some("openai")
        || event.has_official_rate_limits
        || mode_at(transaction, event.timestamp)?.as_deref() == Some("official");
    let (provider_id, provider_name, origin) = if official {
        (OFFICIAL_PROVIDER_ID, OFFICIAL_PROVIDER_NAME, "official")
    } else {
        (
            UNCONFIRMED_PROVIDER_ID,
            UNCONFIRMED_PROVIDER_NAME,
            "unconfirmed",
        )
    };
    transaction
        .execute(
            "INSERT INTO usage_records(
                completed_at,provider_id,provider_name,model,request_path,input_tokens,
                output_tokens,cached_tokens,cache_write_tokens,reasoning_tokens,total_tokens,
                usage_reported,data_source,usage_origin,client_origin,codex_event_key,token_source
             ) VALUES(?1,?2,?3,?4,'codex_session',?5,?6,?7,?8,?9,?10,1,
                      'codex_session',?11,'codex',?12,'codex_session')",
            params![
                event.timestamp.to_rfc3339(),
                provider_id,
                provider_name,
                event.model,
                sqlite_token(event.usage.input_tokens)?,
                sqlite_token(event.usage.output_tokens)?,
                sqlite_token(event.usage.cache_read_tokens)?,
                sqlite_token(event.usage.cache_write_tokens)?,
                sqlite_token(event.usage.reasoning_tokens)?,
                sqlite_token(event.usage.total_tokens)?,
                origin,
                event.event_key,
            ],
        )
        .map_err(|error| format!("无法保存 Codex 本地用量：{error}"))?;
    Ok(PersistOutcome::Inserted)
}

fn clear_codex_usage(transaction: &Transaction<'_>) -> Result<(), String> {
    // 只删除会话来源行；网关行恢复到 Codex 补全前的精确或估算状态。
    transaction
        .execute(
            "DELETE FROM usage_records WHERE data_source='codex_session'",
            [],
        )
        .map_err(|error| format!("无法清理 Codex 会话用量：{error}"))?;
    let previous_rows = {
        let mut statement = transaction
            .prepare(
                "SELECT id,codex_previous_usage_json FROM usage_records
                 WHERE data_source='gateway_response' AND codex_usage_supplemented=1",
            )
            .map_err(|error| format!("无法读取 Codex 补全前的网关用量：{error}"))?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?))
            })
            .map_err(|error| format!("无法查询 Codex 补全前的网关用量：{error}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("无法读取 Codex 补全前的网关用量：{error}"))?;
        rows
    };
    for (id, snapshot) in previous_rows {
        let previous = snapshot
            .as_deref()
            .and_then(|value| serde_json::from_str::<PreviousGatewayUsage>(value).ok());
        let (usage, token_source, estimate_method) = previous
            .map(|value| (value.usage, value.token_source, value.estimate_method))
            .unwrap_or_else(|| (CapturedUsage::default(), "none".to_string(), None));
        transaction
            .execute(
                "UPDATE usage_records SET input_tokens=?1,output_tokens=?2,cached_tokens=?3,
                    cache_write_tokens=?4,reasoning_tokens=?5,total_tokens=?6,
                    usage_reported=?7,codex_event_key=NULL,codex_usage_supplemented=0,
                    token_source=?8,estimate_method=?9,codex_previous_usage_json=NULL
                 WHERE id=?10",
                params![
                    sqlite_token(usage.input_tokens)?,
                    sqlite_token(usage.output_tokens)?,
                    sqlite_token(usage.cache_read_tokens)?,
                    sqlite_token(usage.cache_write_tokens)?,
                    sqlite_token(usage.reasoning_tokens)?,
                    sqlite_token(usage.total_tokens)?,
                    i64::from(usage.is_reported()),
                    token_source,
                    estimate_method,
                    id,
                ],
            )
            .map_err(|error| format!("无法还原由 Codex 补全的网关记录：{error}"))?;
    }
    transaction
        .execute(
            "UPDATE usage_records SET codex_event_key=NULL
             WHERE data_source='gateway_response' AND codex_event_key IS NOT NULL
               AND codex_usage_supplemented=0",
            [],
        )
        .map_err(|error| format!("无法解除 Codex 用量关联：{error}"))?;
    transaction
        .execute("DELETE FROM codex_usage_sync_state", [])
        .map_err(|error| format!("无法重置 Codex 同步游标：{error}"))?;
    Ok(())
}

pub fn rebuild(store: &Store, retention_days: u16) -> Result<CodexUsageSyncStatus, String> {
    let roots = discover_roots();
    let files = collect_files(&roots)?;
    let known_session_ids = files
        .iter()
        .filter_map(|file| session_id_from_filename(&file.path))
        .collect::<HashSet<_>>();
    let mut summary = SyncSummary {
        roots_scanned: roots.len() as u64,
        files_scanned: files.len() as u64,
        ..Default::default()
    };
    // 没有任何源文件时无法证明旧数据应被删除；保留旧记录和解析器版本，等待后续重新扫描。
    if files.is_empty() {
        push_warning(
            &mut summary,
            if roots.is_empty() {
                "未找到 Codex Desktop 本地数据目录"
            } else {
                "未找到可扫描的 Codex 会话文件"
            },
        );
        return Ok(status_from_summary(summary, false));
    }
    let mut parsed_files = Vec::with_capacity(files.len());
    for file in &files {
        match parse_file(
            store,
            file,
            retention_days,
            &known_session_ids,
            &mut summary,
            false,
        ) {
            Ok(parsed) => parsed_files.push(parsed),
            Err(error) => {
                summary.deferred_count += 1;
                push_warning(&mut summary, error);
            }
        }
    }
    // 自动重建必须是全量一致的；存在无法读取的文件时保留旧数据和旧游标。
    if summary.deferred_count > 0 {
        return Err(summary
            .warning_messages
            .first()
            .cloned()
            .unwrap_or_else(|| "Codex 用量重建已延后".to_string()));
    }

    let mut connection = store
        .connection
        .lock()
        .map_err(|_| "数据库锁已损坏".to_string())?;
    let transaction = connection
        .transaction()
        .map_err(|error| format!("无法开始 Codex 用量重建：{error}"))?;
    clear_codex_usage(&transaction)?;
    for parsed in &parsed_files {
        persist_parsed_file(&transaction, parsed, &mut summary)?;
    }
    transaction
        .execute(
            "INSERT INTO settings(key,value) VALUES(?1,?2)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![
                CODEX_USAGE_PARSER_VERSION_KEY,
                CODEX_USAGE_PARSER_VERSION.to_string()
            ],
        )
        .map_err(|error| format!("无法保存 Codex 用量解析器版本：{error}"))?;
    transaction
        .commit()
        .map_err(|error| format!("无法提交 Codex 用量重建：{error}"))?;

    if roots.is_empty() {
        push_warning(&mut summary, "未找到 Codex Desktop 本地数据目录");
    } else if summary.files_scanned > 0 && summary.token_events_found == 0 {
        push_warning(
            &mut summary,
            "已发现 Codex 会话文件，但没有找到可用的 Token 事件",
        );
    }
    Ok(status_from_summary(summary, true))
}

pub fn record_mode(store: &Store, mode: &str) -> Result<(), String> {
    let connection = store
        .connection
        .lock()
        .map_err(|_| "数据库锁已损坏".to_string())?;
    connection
        .execute(
            "INSERT INTO codex_mode_history(changed_at,mode) VALUES(?1,?2)",
            params![Utc::now().to_rfc3339(), mode],
        )
        .map(|_| ())
        .map_err(|error| format!("无法保存 Codex 模式时间线：{error}"))
}

pub fn stored_status(store: &Store) -> CodexUsageSyncStatus {
    let Ok(connection) = store.connection.lock() else {
        return CodexUsageSyncStatus {
            state: "error".to_string(),
            warning_messages: vec!["用量数据库暂时不可用".to_string()],
            ..Default::default()
        };
    };
    let last_synced_at = connection
        .query_row(
            "SELECT MAX(last_synced_at) FROM codex_usage_sync_state",
            [],
            |row| row.get::<_, Option<String>>(0),
        )
        .unwrap_or(None);
    let parser_version = connection
        .query_row(
            "SELECT value FROM settings WHERE key=?1",
            [CODEX_USAGE_PARSER_VERSION_KEY],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .ok()
        .flatten()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or_default();
    CodexUsageSyncStatus {
        state: if last_synced_at.is_some() {
            "success"
        } else {
            "idle"
        }
        .to_string(),
        last_synced_at,
        parser_version,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::sync::Mutex;

    #[test]
    fn cumulative_delta_handles_cache_write_and_reasoning() {
        let previous = TokenCounters {
            input_tokens: Some(100),
            output_tokens: Some(20),
            cached_input_tokens: Some(40),
            cache_write_input_tokens: Some(5),
            reasoning_output_tokens: Some(8),
            total_tokens: Some(120),
            ..Default::default()
        };
        let current = TokenCounters {
            input_tokens: Some(160),
            output_tokens: Some(35),
            cached_input_tokens: Some(70),
            cache_write_input_tokens: Some(9),
            reasoning_output_tokens: Some(12),
            total_tokens: Some(195),
            ..Default::default()
        };
        let delta = current.delta_from(&previous).unwrap().captured();
        assert_eq!(delta.input_tokens, Some(60));
        assert_eq!(delta.output_tokens, Some(15));
        assert_eq!(delta.cache_read_tokens, Some(30));
        assert_eq!(delta.cache_write_tokens, Some(4));
        assert_eq!(delta.reasoning_tokens, Some(4));
        assert_eq!(delta.total_tokens, Some(75));
    }

    #[test]
    fn total_tokens_does_not_add_cache_or_reasoning_twice() {
        let usage = TokenCounters {
            input_tokens: Some(100),
            output_tokens: Some(20),
            cached_input_tokens: Some(50),
            reasoning_output_tokens: Some(10),
            ..Default::default()
        }
        .captured();
        assert_eq!(usage.total_tokens, Some(120));
    }

    #[test]
    fn token_counter_accepts_current_field_aliases() {
        let value = serde_json::json!({
            "input_tokens": 20,
            "output_tokens": 5,
            "cache_read_tokens": 7,
            "cache_write_tokens": 2,
            "reasoning_tokens": 1
        });
        let counters = parse_counters(Some(&value)).unwrap();
        let usage = counters.captured();
        assert_eq!(usage.cache_read_tokens, Some(7));
        assert_eq!(usage.cache_write_tokens, Some(2));
        assert_eq!(usage.reasoning_tokens, Some(1));
        assert_eq!(usage.total_tokens, Some(25));
    }

    #[test]
    fn official_total_uses_cache_inclusive_input() {
        let usage = TokenCounters {
            input_tokens: Some(18_041_000),
            output_tokens: Some(64_000),
            cached_input_tokens: Some(16_659_000),
            total_tokens: Some(18_105_000),
            ..Default::default()
        }
        .captured();
        let fresh_input = usage
            .input_tokens
            .unwrap()
            .saturating_sub(usage.cache_read_tokens.unwrap());

        assert_eq!(fresh_input, 1_382_000);
        assert_eq!(usage.total_tokens, Some(18_105_000));
    }

    #[test]
    fn malformed_and_incomplete_events_do_not_block_later_usage() {
        let temp = env::temp_dir().join(format!("ai-gateway-codex-{}", uuid::Uuid::new_v4()));
        let session_dir = temp.join("sessions").join("2026").join("08").join("14");
        fs::create_dir_all(&session_dir).unwrap();
        let session_id = uuid::Uuid::new_v4().hyphenated().to_string();
        let path = session_dir.join(format!("rollout-2026-08-14T09-00-00-{session_id}.jsonl"));
        let timestamp = Utc::now().to_rfc3339();
        let content = format!(
            "{{\"timestamp\":\"{timestamp}\",\"type\":\"session_meta\",\"payload\":{{\"threadId\":\"{session_id}\",\"model_provider\":\"openai\"}}}}\n\
             {{broken json}}\n\
             {{\"timestamp\":\"{timestamp}\",\"type\":\"event_msg\",\"payload\":{{\"type\":\"token_count\"}}}}\n\
             {{\"timestamp\":\"{timestamp}\",\"type\":\"turn_context\",\"payload\":{{\"info\":{{\"model\":\"gpt-5.4\"}}}}}}\n\
             {{\"timestamp\":\"{timestamp}\",\"type\":\"event_msg\",\"payload\":{{\"type\":\"token_count\",\"rate_limits\":{{\"limit_id\":\"weekly\"}},\"info\":{{\"last_token_usage\":{{\"input_tokens\":18041000,\"cached_input_tokens\":16659000,\"output_tokens\":64000,\"reasoning_output_tokens\":1000,\"total_tokens\":18105000}},\"total_token_usage\":{{\"input_tokens\":18041000,\"cached_input_tokens\":16659000,\"output_tokens\":64000,\"total_tokens\":18105000}}}}}}}}\n"
        );
        fs::write(&path, content).unwrap();
        let root = CodexRoot {
            path: temp.clone(),
            id: "test-root".to_string(),
        };
        let file = CodexFile {
            cursor_key: format!("{}:{}", root.id, relative_path(&root.path, &path)),
            path,
        };
        let store = Store {
            connection: Mutex::new(Connection::open_in_memory().unwrap()),
        };
        let mut summary = SyncSummary::default();
        let parsed = parse_file(
            &store,
            &file,
            365,
            &HashSet::from([session_id.clone()]),
            &mut summary,
            false,
        )
        .unwrap();

        assert_eq!(
            parsed.cursor.session_id.as_deref(),
            Some(session_id.as_str())
        );
        assert_eq!(parsed.cursor.current_model.as_deref(), Some("gpt-5.4"));
        assert_eq!(parsed.events.len(), 1);
        assert_eq!(parsed.events[0].usage.total_tokens, Some(18_105_000));
        assert_eq!(summary.token_events_found, 1);
        assert_eq!(summary.skipped_count, 2);

        fs::remove_dir_all(temp).ok();
    }

    #[test]
    fn zero_token_lifecycle_event_is_not_imported() {
        let temp = env::temp_dir().join(format!("ai-gateway-codex-zero-{}", uuid::Uuid::new_v4()));
        let session_dir = temp.join("sessions").join("2026").join("08").join("14");
        fs::create_dir_all(&session_dir).unwrap();
        let session_id = uuid::Uuid::new_v4().hyphenated().to_string();
        let path = session_dir.join(format!("rollout-2026-08-14T09-00-00-{session_id}.jsonl"));
        let timestamp = Utc::now().to_rfc3339();
        let content = format!(
            "{{\"timestamp\":\"{timestamp}\",\"type\":\"session_meta\",\"payload\":{{\"id\":\"{session_id}\",\"model_provider\":\"openai\"}}}}\n\
             {{\"timestamp\":\"{timestamp}\",\"type\":\"event_msg\",\"payload\":{{\"type\":\"token_count\",\"info\":{{\"last_token_usage\":{{\"input_tokens\":0,\"cached_input_tokens\":0,\"output_tokens\":0,\"total_tokens\":0}},\"total_token_usage\":{{\"input_tokens\":0,\"cached_input_tokens\":0,\"output_tokens\":0,\"total_tokens\":0}}}}}}}}\n"
        );
        fs::write(&path, content).unwrap();
        let root = CodexRoot {
            path: temp.clone(),
            id: "test-root".to_string(),
        };
        let file = CodexFile {
            cursor_key: format!("{}:{}", root.id, relative_path(&root.path, &path)),
            path,
        };
        let store = Store {
            connection: Mutex::new(Connection::open_in_memory().unwrap()),
        };
        let mut summary = SyncSummary::default();
        let parsed = parse_file(
            &store,
            &file,
            365,
            &HashSet::from([session_id]),
            &mut summary,
            false,
        )
        .unwrap();

        assert!(parsed.events.is_empty());
        assert_eq!(summary.token_events_found, 1);
        assert_eq!(summary.skipped_count, 1);

        fs::remove_dir_all(temp).ok();
    }
}
