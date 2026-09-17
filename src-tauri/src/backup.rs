use super::{
    codex_integration, gateway_codex_configuration, normalized_routing_policy,
    persist_codex_management_metadata, push_log, routing_policy_values, sanitize_base_url,
    save_and_verify_provider_secret, validate_probe_interval, validate_routing_policy, AppState,
    ProviderConfig, ProviderSecretStore, RoutingPolicy,
};
use argon2::{Algorithm, Argon2, Params, Version};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chacha20poly1305::{
    aead::{Aead, KeyInit, Payload},
    XChaCha20Poly1305, XNonce,
};
use chrono::{DateTime, Utc};
use rand::{rngs::OsRng, RngCore};
use rusqlite::{params, Transaction};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};
use tauri::State as TauriState;
use zeroize::Zeroizing;

const BACKUP_FORMAT: &str = "ai-gateway-backup";
const BACKUP_FORMAT_VERSION: u16 = 1;
const BACKUP_AAD: &[u8] = b"ai-gateway-backup-v1";
const BACKUP_EXTENSION: &str = "aigateway";
const MAX_BACKUP_BYTES: u64 = 10 * 1024 * 1024;
const MAX_BACKUP_PROVIDERS: usize = 500;
const KDF_MEMORY_KIB: u32 = 65_536;
const KDF_ITERATIONS: u32 = 3;
const KDF_PARALLELISM: u32 = 1;

#[derive(Clone, Serialize, Deserialize)]
struct BackupProvider {
    id: String,
    name: String,
    base_url: String,
    models: Vec<String>,
    priority: i64,
    enabled: bool,
    timeout_ms: u64,
    api_key: String,
}

impl From<BackupProvider> for ProviderConfig {
    fn from(provider: BackupProvider) -> Self {
        Self {
            id: provider.id,
            name: provider.name,
            base_url: provider.base_url,
            models: provider.models,
            priority: provider.priority,
            enabled: provider.enabled,
            timeout_ms: provider.timeout_ms,
            api_key: provider.api_key,
            api_key_error: None,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct BackupPayloadData {
    format_version: u16,
    exported_at: DateTime<Utc>,
    app_version: String,
    providers: Vec<BackupProvider>,
    probe_interval_sec: u64,
    routing_policy: RoutingPolicy,
    locked_provider_id: Option<String>,
    local_token: String,
}

#[derive(Serialize, Deserialize)]
struct BackupEnvelope {
    format: String,
    version: u16,
    kdf: BackupKdf,
    cipher: BackupCipher,
}

#[derive(Serialize, Deserialize)]
struct BackupKdf {
    algorithm: String,
    salt: String,
    memory_kib: u32,
    iterations: u32,
    parallelism: u32,
}

#[derive(Serialize, Deserialize)]
struct BackupCipher {
    algorithm: String,
    nonce: String,
    ciphertext: String,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BackupImportMode {
    Replace,
    Merge,
}

#[derive(Serialize)]
pub(crate) struct BackupPreview {
    format_version: u16,
    exported_at: DateTime<Utc>,
    app_version: String,
    provider_count: usize,
    conflict_count: usize,
    includes_local_token: bool,
}

#[derive(Serialize)]
pub(crate) struct BackupExportResult {
    file_name: String,
    provider_count: usize,
    warning_messages: Vec<String>,
}

#[derive(Serialize)]
pub(crate) struct BackupImportResult {
    created_count: usize,
    updated_count: usize,
    removed_count: usize,
    warning_messages: Vec<String>,
}

fn validate_password(password: &str) -> Result<(), String> {
    if password.chars().count() < 8 {
        return Err("备份密码至少需要 8 个字符".to_string());
    }
    Ok(())
}

fn build_payload(state: &AppState) -> Result<(BackupPayloadData, Vec<String>), String> {
    let providers = state.config.read().unwrap().clone();
    if providers.len() > MAX_BACKUP_PROVIDERS {
        return Err(format!("中转数量不能超过 {MAX_BACKUP_PROVIDERS} 个"));
    }

    let mut backup_providers = Vec::with_capacity(providers.len());
    let mut warnings = Vec::new();
    for provider in providers {
        // 系统密钥库是首选来源；当前路由正在使用的内存密钥只在凭据缺失或暂时不可读时兜底。
        let stored = state.secrets.get(&provider.id);
        let api_key = match stored {
            Ok(Some(value)) if !value.is_empty() => value,
            missing_or_failed if !provider.api_key.is_empty() => {
                if save_and_verify_provider_secret(
                    state.secrets.as_ref(),
                    &provider.id,
                    &provider.api_key,
                )
                .is_err()
                {
                    warnings.push(format!(
                        "「{}」的系统凭据无法自动修复；备份已包含当前运行时密钥，请重新保存该中转的 API Key",
                        provider.name
                    ));
                }
                let _ = missing_or_failed;
                provider.api_key.clone()
            }
            Err(error) => {
                return Err(format!("无法读取「{}」的 API Key：{error}", provider.name));
            }
            _ => return Err(format!("「{}」没有可导出的 API Key", provider.name)),
        };
        backup_providers.push(BackupProvider {
            id: provider.id,
            name: provider.name,
            base_url: provider.base_url,
            models: provider.models,
            priority: provider.priority,
            enabled: provider.enabled,
            timeout_ms: provider.timeout_ms,
            api_key,
        });
    }

    let settings = state.settings.read().unwrap().clone();
    Ok((
        BackupPayloadData {
            format_version: BACKUP_FORMAT_VERSION,
            exported_at: Utc::now(),
            app_version: env!("CARGO_PKG_VERSION").to_string(),
            providers: backup_providers,
            probe_interval_sec: settings.probe_interval_sec,
            routing_policy: state.policy.read().unwrap().clone(),
            locked_provider_id: state.locked_provider_id.read().unwrap().clone(),
            local_token: settings.local_token,
        },
        warnings,
    ))
}

fn derive_key(password: &str, kdf: &BackupKdf, salt: &[u8]) -> Result<Zeroizing<[u8; 32]>, String> {
    if kdf.algorithm != "argon2id"
        || !(8_192..=262_144).contains(&kdf.memory_kib)
        || !(1..=10).contains(&kdf.iterations)
        || !(1..=4).contains(&kdf.parallelism)
    {
        return Err("备份文件使用了不受支持的密钥派生参数".to_string());
    }
    let params = Params::new(kdf.memory_kib, kdf.iterations, kdf.parallelism, Some(32))
        .map_err(|_| "备份文件的密钥派生参数无效".to_string())?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut key = Zeroizing::new([0u8; 32]);
    argon2
        .hash_password_into(password.as_bytes(), salt, key.as_mut())
        .map_err(|_| "无法派生备份加密密钥".to_string())?;
    Ok(key)
}

fn encrypt_payload(payload: &BackupPayloadData, password: &str) -> Result<Vec<u8>, String> {
    validate_password(password)?;
    let plaintext = Zeroizing::new(
        serde_json::to_vec(payload).map_err(|error| format!("无法序列化备份：{error}"))?,
    );
    if plaintext.len() as u64 > MAX_BACKUP_BYTES {
        return Err("备份内容超过 10MB 限制".to_string());
    }

    let mut salt = [0u8; 16];
    let mut nonce = [0u8; 24];
    OsRng.fill_bytes(&mut salt);
    OsRng.fill_bytes(&mut nonce);
    let kdf = BackupKdf {
        algorithm: "argon2id".to_string(),
        salt: BASE64.encode(salt),
        memory_kib: KDF_MEMORY_KIB,
        iterations: KDF_ITERATIONS,
        parallelism: KDF_PARALLELISM,
    };
    let key = derive_key(password, &kdf, &salt)?;
    let cipher = XChaCha20Poly1305::new_from_slice(key.as_ref())
        .map_err(|_| "无法初始化备份加密器".to_string())?;
    let ciphertext = cipher
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: plaintext.as_ref(),
                aad: BACKUP_AAD,
            },
        )
        .map_err(|_| "备份加密失败".to_string())?;
    let envelope = BackupEnvelope {
        format: BACKUP_FORMAT.to_string(),
        version: BACKUP_FORMAT_VERSION,
        kdf,
        cipher: BackupCipher {
            algorithm: "xchacha20-poly1305".to_string(),
            nonce: BASE64.encode(nonce),
            ciphertext: BASE64.encode(ciphertext),
        },
    };
    serde_json::to_vec_pretty(&envelope).map_err(|error| format!("无法生成备份文件：{error}"))
}

fn decrypt_payload(bytes: &[u8], password: &str) -> Result<BackupPayloadData, String> {
    validate_password(password)?;
    if bytes.len() as u64 > MAX_BACKUP_BYTES {
        return Err("备份文件超过 10MB 限制".to_string());
    }
    let envelope: BackupEnvelope =
        serde_json::from_slice(bytes).map_err(|_| "备份文件格式无效或已经损坏".to_string())?;
    if envelope.format != BACKUP_FORMAT || envelope.version != BACKUP_FORMAT_VERSION {
        return Err("当前版本不支持此备份文件".to_string());
    }
    if envelope.cipher.algorithm != "xchacha20-poly1305" {
        return Err("备份文件使用了不受支持的加密算法".to_string());
    }
    let salt = BASE64
        .decode(&envelope.kdf.salt)
        .map_err(|_| "备份文件的盐值无效".to_string())?;
    let nonce = BASE64
        .decode(&envelope.cipher.nonce)
        .map_err(|_| "备份文件的 Nonce 无效".to_string())?;
    let ciphertext = BASE64
        .decode(&envelope.cipher.ciphertext)
        .map_err(|_| "备份文件的密文无效".to_string())?;
    if salt.len() != 16 || nonce.len() != 24 {
        return Err("备份文件的加密参数长度无效".to_string());
    }

    let key = derive_key(password, &envelope.kdf, &salt)?;
    let cipher = XChaCha20Poly1305::new_from_slice(key.as_ref())
        .map_err(|_| "无法初始化备份解密器".to_string())?;
    let plaintext = Zeroizing::new(
        cipher
            .decrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: &ciphertext,
                    aad: BACKUP_AAD,
                },
            )
            .map_err(|_| "备份密码错误，或文件已经损坏".to_string())?,
    );
    let payload: BackupPayloadData =
        serde_json::from_slice(plaintext.as_ref()).map_err(|_| "备份内容无法解析".to_string())?;
    validate_payload(payload)
}

fn validate_payload(mut payload: BackupPayloadData) -> Result<BackupPayloadData, String> {
    if payload.format_version != BACKUP_FORMAT_VERSION {
        return Err("备份内容版本不受支持".to_string());
    }
    if payload.providers.len() > MAX_BACKUP_PROVIDERS {
        return Err(format!(
            "备份中的中转数量不能超过 {MAX_BACKUP_PROVIDERS} 个"
        ));
    }
    validate_probe_interval(payload.probe_interval_sec)?;
    // 格式版本 1 的容量字段仅作兼容镜像，导入前统一以全局重试配置为准。
    payload.routing_policy = normalized_routing_policy(&payload.routing_policy);
    validate_routing_policy(&payload.routing_policy)?;
    if !(16..=256).contains(&payload.local_token.len()) {
        return Err("备份中的本地访问令牌无效".to_string());
    }

    let mut ids = HashSet::with_capacity(payload.providers.len());
    for provider in &mut payload.providers {
        provider.id = provider.id.trim().to_string();
        provider.name = provider.name.trim().to_string();
        provider.api_key = provider.api_key.trim().to_string();
        if provider.id.is_empty() || provider.id.len() > 128 || !ids.insert(provider.id.clone()) {
            return Err("备份中存在无效或重复的中转 ID".to_string());
        }
        if provider.name.is_empty() || provider.name.chars().count() > 120 {
            return Err(format!("中转「{}」的名称无效", provider.name));
        }
        provider.base_url = sanitize_base_url(&provider.base_url)?;
        if !(1_000..=600_000).contains(&provider.timeout_ms) {
            return Err(format!("中转「{}」的超时时间无效", provider.name));
        }
        if provider.api_key.is_empty() || provider.api_key.len() > 4_096 {
            return Err(format!("中转「{}」的 API Key 无效", provider.name));
        }
        if provider.models.len() > 200
            || provider
                .models
                .iter()
                .any(|model| model.chars().count() > 120)
        {
            return Err(format!("中转「{}」的模型匹配规则无效", provider.name));
        }
        provider.models = provider
            .models
            .drain(..)
            .map(|model| model.trim().to_string())
            .filter(|model| !model.is_empty())
            .collect();
    }

    payload.providers.sort_by_key(|provider| provider.priority);
    for (index, provider) in payload.providers.iter_mut().enumerate() {
        // 优先级间隙没有业务含义，导入时归一化为稳定连续顺序。
        provider.priority = index as i64 + 1;
    }
    if payload
        .locked_provider_id
        .as_ref()
        .is_some_and(|id| !ids.contains(id))
    {
        return Err("备份中的首选中转不存在".to_string());
    }
    Ok(payload)
}

fn read_backup(path: &Path) -> Result<Vec<u8>, String> {
    let metadata = fs::metadata(path).map_err(|error| format!("无法读取备份文件：{error}"))?;
    if !metadata.is_file() {
        return Err("选择的路径不是备份文件".to_string());
    }
    if metadata.len() > MAX_BACKUP_BYTES {
        return Err("备份文件超过 10MB 限制".to_string());
    }
    fs::read(path).map_err(|error| format!("无法读取备份文件：{error}"))
}

fn export_path(path: String) -> Result<PathBuf, String> {
    let mut path = PathBuf::from(path);
    if !path.is_absolute() {
        return Err("备份文件必须保存到绝对路径".to_string());
    }
    if path.extension().and_then(|value| value.to_str()) != Some(BACKUP_EXTENSION) {
        path.set_extension(BACKUP_EXTENSION);
    }
    Ok(path)
}

fn write_backup(path: &Path, bytes: &[u8]) -> Result<(), String> {
    if bytes.len() as u64 > MAX_BACKUP_BYTES {
        return Err("备份文件超过 10MB 限制".to_string());
    }
    let parent = path
        .parent()
        .ok_or_else(|| "备份保存目录无效".to_string())?;
    if !parent.exists() {
        return Err("备份保存目录不存在".to_string());
    }
    // 先写入同目录临时文件，写入完整后再替换目标，避免中断时留下半份备份。
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("backup"),
        uuid::Uuid::new_v4().simple()
    ));
    fs::write(&temporary, bytes).map_err(|error| format!("无法写入备份文件：{error}"))?;
    let previous = path.exists().then(|| {
        parent.join(format!(
            ".{}.{}.previous",
            path.file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("backup"),
            uuid::Uuid::new_v4().simple()
        ))
    });
    if let Some(previous) = previous.as_ref() {
        fs::rename(path, previous).map_err(|error| format!("无法准备覆盖已有备份：{error}"))?;
    }
    if let Err(error) = fs::rename(&temporary, path) {
        let _ = fs::remove_file(&temporary);
        if let Some(previous) = previous.as_ref() {
            let _ = fs::rename(previous, path);
        }
        return Err(format!("无法完成备份文件保存：{error}"));
    }
    if let Some(previous) = previous {
        let _ = fs::remove_file(previous);
    }
    Ok(())
}

fn upsert_provider(transaction: &Transaction<'_>, provider: &ProviderConfig) -> Result<(), String> {
    transaction
        .execute(
            "INSERT INTO providers(id,name,base_url,models_json,priority,enabled,timeout_ms,created_at)
             VALUES(?1,?2,?3,?4,?5,?6,?7,datetime('now'))
             ON CONFLICT(id) DO UPDATE SET
               name=excluded.name,base_url=excluded.base_url,models_json=excluded.models_json,
               priority=excluded.priority,enabled=excluded.enabled,timeout_ms=excluded.timeout_ms",
            params![
                provider.id,
                provider.name,
                provider.base_url,
                serde_json::to_string(&provider.models).map_err(|error| error.to_string())?,
                provider.priority,
                provider.enabled as i64,
                provider.timeout_ms as i64
            ],
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn save_transaction_setting(
    transaction: &Transaction<'_>,
    key: &str,
    value: &str,
) -> Result<(), String> {
    transaction
        .execute(
            "INSERT INTO settings(key,value) VALUES(?1,?2)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![key, value],
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn restore_secrets(
    secrets: &dyn ProviderSecretStore,
    originals: &[(String, Option<String>)],
) -> Vec<String> {
    let mut warnings = Vec::new();
    for (id, previous) in originals.iter().rev() {
        let result = match previous {
            Some(value) => save_and_verify_provider_secret(secrets, id, value),
            None => secrets.delete(id),
        };
        if result.is_err() {
            warnings.push(format!("中转 {id} 的原密钥无法自动恢复"));
        }
    }
    warnings
}

fn apply_import(
    state: &AppState,
    payload: BackupPayloadData,
    mode: BackupImportMode,
) -> Result<BackupImportResult, String> {
    let current = state.config.read().unwrap().clone();
    let current_by_id: HashMap<String, ProviderConfig> = current
        .iter()
        .cloned()
        .map(|provider| (provider.id.clone(), provider))
        .collect();
    let imported: Vec<ProviderConfig> = payload
        .providers
        .iter()
        .cloned()
        .map(ProviderConfig::from)
        .collect();
    let imported_ids: HashSet<String> = imported.iter().map(|item| item.id.clone()).collect();
    let updated_count = imported
        .iter()
        .filter(|provider| current_by_id.contains_key(&provider.id))
        .count();
    let created_count = imported.len() - updated_count;
    let removed_count = match mode {
        BackupImportMode::Replace => current
            .iter()
            .filter(|provider| !imported_ids.contains(&provider.id))
            .count(),
        BackupImportMode::Merge => 0,
    };

    let mut touched_ids = HashSet::new();
    let next = match mode {
        BackupImportMode::Replace => {
            touched_ids.extend(imported_ids.iter().cloned());
            imported.clone()
        }
        BackupImportMode::Merge => {
            let mut next = current.clone();
            let mut next_priority = next.iter().map(|item| item.priority).max().unwrap_or(0) + 1;
            for mut provider in imported.clone() {
                touched_ids.insert(provider.id.clone());
                if let Some(existing) = next.iter_mut().find(|item| item.id == provider.id) {
                    // 合并只更新中转内容，保留目标电脑当前排序和全局路由设置。
                    provider.priority = existing.priority;
                    *existing = provider;
                } else {
                    provider.priority = next_priority;
                    next_priority += 1;
                    next.push(provider);
                }
            }
            next.sort_by_key(|provider| provider.priority);
            next
        }
    };

    let imported_keys: HashMap<&str, &str> = payload
        .providers
        .iter()
        .map(|provider| (provider.id.as_str(), provider.api_key.as_str()))
        .collect();
    let mut originals = Vec::with_capacity(imported_keys.len());
    for (id, api_key) in imported_keys {
        let previous = state
            .secrets
            .get(id)
            .map_err(|error| format!("无法读取现有密钥，导入已取消：{error}"))?;
        originals.push((id.to_string(), previous));
        if let Err(error) = save_and_verify_provider_secret(state.secrets.as_ref(), id, api_key) {
            let rollback_warnings = restore_secrets(state.secrets.as_ref(), &originals);
            return Err(format!(
                "无法写入系统密钥库，导入已取消：{error}{}",
                if rollback_warnings.is_empty() {
                    String::new()
                } else {
                    format!("；{}", rollback_warnings.join("；"))
                }
            ));
        }
    }

    let persist_result = (|| -> Result<(), String> {
        let mut connection = state
            .store
            .connection
            .lock()
            .map_err(|_| "数据库锁已损坏".to_string())?;
        let transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        match mode {
            BackupImportMode::Replace => {
                transaction
                    .execute("DELETE FROM providers", [])
                    .map_err(|error| error.to_string())?;
                for provider in &next {
                    upsert_provider(&transaction, provider)?;
                }
                save_transaction_setting(
                    &transaction,
                    "probe_interval_sec",
                    &payload.probe_interval_sec.to_string(),
                )?;
                for (key, value) in routing_policy_values(&payload.routing_policy) {
                    save_transaction_setting(&transaction, key, &value)?;
                }
                save_transaction_setting(&transaction, "local_token", &payload.local_token)?;
                save_transaction_setting(
                    &transaction,
                    "locked_provider_id",
                    payload.locked_provider_id.as_deref().unwrap_or_default(),
                )?;
            }
            BackupImportMode::Merge => {
                for provider in next
                    .iter()
                    .filter(|provider| touched_ids.contains(&provider.id))
                {
                    upsert_provider(&transaction, provider)?;
                }
            }
        }
        transaction.commit().map_err(|error| error.to_string())
    })();
    if let Err(error) = persist_result {
        let rollback_warnings = restore_secrets(state.secrets.as_ref(), &originals);
        return Err(format!(
            "数据库更新失败，导入已取消：{error}{}",
            if rollback_warnings.is_empty() {
                String::new()
            } else {
                format!("；{}", rollback_warnings.join("；"))
            }
        ));
    }

    let mut warnings = Vec::new();
    if matches!(mode, BackupImportMode::Replace) {
        for provider in &current {
            if !imported_ids.contains(&provider.id) {
                if state.secrets.delete(&provider.id).is_err() {
                    warnings.push(format!("旧中转「{}」的孤立密钥未能清理", provider.name));
                }
            }
        }
        let mut settings = state.settings.write().unwrap();
        settings.local_token = payload.local_token;
        settings.probe_interval_sec = payload.probe_interval_sec;
        drop(settings);
        *state.policy.write().unwrap() = payload.routing_policy;
        *state.locked_provider_id.write().unwrap() = payload.locked_provider_id;
        state.health.lock().unwrap().clear();
    } else {
        let mut health = state.health.lock().unwrap();
        for id in &touched_ids {
            health.remove(id);
        }
    }
    *state.config.write().unwrap() = next;
    push_log(
        state,
        "info",
        format!("配置备份已导入：新增 {created_count}，更新 {updated_count}，删除 {removed_count}"),
        None,
    );
    Ok(BackupImportResult {
        created_count,
        updated_count,
        removed_count,
        warning_messages: warnings,
    })
}

#[tauri::command]
pub(crate) async fn export_configuration_backup(
    path: String,
    password: String,
    state: TauriState<'_, AppState>,
) -> Result<BackupExportResult, String> {
    validate_password(&password)?;
    let path = export_path(path)?;
    let (payload, warning_messages) = build_payload(&state)?;
    let provider_count = payload.providers.len();
    let password = Zeroizing::new(password);
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("AI-Gateway-backup.aigateway")
        .to_string();
    tokio::task::spawn_blocking(move || {
        let encrypted = encrypt_payload(&payload, password.as_str())?;
        write_backup(&path, &encrypted)
    })
    .await
    .map_err(|error| format!("备份任务异常结束：{error}"))??;
    push_log(
        &state,
        "info",
        format!("已导出 {provider_count} 个中转的加密备份"),
        None,
    );
    Ok(BackupExportResult {
        file_name,
        provider_count,
        warning_messages,
    })
}

#[tauri::command]
pub(crate) async fn inspect_configuration_backup(
    path: String,
    password: String,
    state: TauriState<'_, AppState>,
) -> Result<BackupPreview, String> {
    validate_password(&password)?;
    let password = Zeroizing::new(password);
    let payload = tokio::task::spawn_blocking(move || {
        let bytes = read_backup(Path::new(&path))?;
        decrypt_payload(&bytes, password.as_str())
    })
    .await
    .map_err(|error| format!("备份检查任务异常结束：{error}"))??;
    let current_ids: HashSet<String> = state
        .config
        .read()
        .unwrap()
        .iter()
        .map(|provider| provider.id.clone())
        .collect();
    let conflict_count = payload
        .providers
        .iter()
        .filter(|provider| current_ids.contains(&provider.id))
        .count();
    Ok(BackupPreview {
        format_version: payload.format_version,
        exported_at: payload.exported_at,
        app_version: payload.app_version,
        provider_count: payload.providers.len(),
        conflict_count,
        includes_local_token: !payload.local_token.is_empty(),
    })
}

#[tauri::command]
pub(crate) async fn import_configuration_backup(
    path: String,
    password: String,
    mode: BackupImportMode,
    state: TauriState<'_, AppState>,
) -> Result<BackupImportResult, String> {
    validate_password(&password)?;
    let password = Zeroizing::new(password);
    let payload = tokio::task::spawn_blocking(move || {
        let bytes = read_backup(Path::new(&path))?;
        decrypt_payload(&bytes, password.as_str())
    })
    .await
    .map_err(|error| format!("备份导入任务异常结束：{error}"))??;
    let old_codex = gateway_codex_configuration(
        &state.settings.read().unwrap(),
        &state.policy.read().unwrap(),
    );
    let next_codex = codex_integration::GatewayCodexConfiguration {
        port: old_codex.port,
        local_token: if matches!(mode, BackupImportMode::Replace) {
            payload.local_token.clone()
        } else {
            old_codex.local_token.clone()
        },
        stream_max_retries: if matches!(mode, BackupImportMode::Replace) {
            payload.routing_policy.codex_stream_max_retries
        } else {
            old_codex.stream_max_retries
        },
    };
    // 完整覆盖会恢复本地令牌和 Codex 流恢复次数；合并模式保持目标电脑的全局设置。
    // 先更新可补偿的 config.toml，导入失败时再恢复旧受管配置。
    let codex_changed = if old_codex != next_codex {
        codex_integration::update_managed_gateway_configuration(&old_codex, &next_codex)?.is_some()
    } else {
        false
    };
    match apply_import(&state, payload, mode) {
        Ok(mut result) => {
            if codex_changed {
                if let Err(error) = persist_codex_management_metadata(&state, false) {
                    result.warning_messages.push(format!(
                        "Codex 接入配置已同步，但本地管理元数据保存失败：{error}"
                    ));
                }
                result
                    .warning_messages
                    .push("Codex 配置已修改，请完全退出并重新启动 Codex".to_string());
            }
            Ok(result)
        }
        Err(error) => {
            if codex_changed {
                if let Err(rollback_error) =
                    codex_integration::update_managed_gateway_configuration(&next_codex, &old_codex)
                {
                    return Err(format!(
                        "备份导入失败：{error}；Codex 配置回滚也失败：{rollback_error}"
                    ));
                }
            }
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HealthRecord, MemoryProviderSecretStore, RouterSettings, Store};
    use reqwest::Client;
    use rusqlite::Connection;
    use std::collections::VecDeque;
    use std::sync::{atomic::AtomicBool, Arc, Mutex, RwLock};

    struct UnavailableSecretStore;

    impl ProviderSecretStore for UnavailableSecretStore {
        fn get(&self, _id: &str) -> Result<Option<String>, String> {
            Err("credential backend unavailable".to_string())
        }

        fn set(&self, _id: &str, _value: &str) -> Result<(), String> {
            Err("credential backend unavailable".to_string())
        }

        fn delete(&self, _id: &str) -> Result<(), String> {
            Err("credential backend unavailable".to_string())
        }
    }

    fn sample_payload() -> BackupPayloadData {
        BackupPayloadData {
            format_version: BACKUP_FORMAT_VERSION,
            exported_at: Utc::now(),
            app_version: "0.1.0".to_string(),
            providers: vec![BackupProvider {
                id: "provider-1".to_string(),
                name: "主力中转".to_string(),
                base_url: "https://example.com/v1".to_string(),
                models: vec!["gpt-*".to_string()],
                priority: 1,
                enabled: true,
                timeout_ms: 120_000,
                api_key: "sample-provider-key".to_string(),
            }],
            probe_interval_sec: 60,
            routing_policy: RoutingPolicy::default(),
            locked_provider_id: Some("provider-1".to_string()),
            local_token: "sample-local-token".to_string(),
        }
    }

    fn import_test_state(
        providers: Vec<ProviderConfig>,
        initialize_provider_table: bool,
    ) -> (AppState, Arc<MemoryProviderSecretStore>) {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );",
            )
            .unwrap();
        if initialize_provider_table {
            connection
                .execute_batch(
                    "CREATE TABLE providers (
                        id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        base_url TEXT NOT NULL,
                        models_json TEXT NOT NULL,
                        priority INTEGER NOT NULL,
                        enabled INTEGER NOT NULL,
                        timeout_ms INTEGER NOT NULL,
                        created_at TEXT NOT NULL
                    );",
                )
                .unwrap();
            for provider in &providers {
                connection
                    .execute(
                        "INSERT INTO providers(id,name,base_url,models_json,priority,enabled,timeout_ms,created_at)
                         VALUES(?1,?2,?3,?4,?5,?6,?7,datetime('now'))",
                        params![
                            provider.id,
                            provider.name,
                            provider.base_url,
                            serde_json::to_string(&provider.models).unwrap(),
                            provider.priority,
                            provider.enabled as i64,
                            provider.timeout_ms as i64
                        ],
                    )
                    .unwrap();
            }
        }
        let secrets = Arc::new(MemoryProviderSecretStore::default());
        for provider in &providers {
            secrets.set(&provider.id, &provider.api_key).unwrap();
        }
        let state = AppState {
            config: Arc::new(RwLock::new(providers)),
            health: Arc::new(Mutex::new(HashMap::<String, HealthRecord>::new())),
            settings: Arc::new(RwLock::new(RouterSettings {
                bind_address: "127.0.0.1".to_string(),
                port: 8765,
                local_token: "sample-target-local-token".to_string(),
                probe_interval_sec: 30,
                log_retention_days: 7,
                usage_retention_days: 365,
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
            secrets: secrets.clone(),
            gateway: Arc::new(crate::GatewayController::default()),
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
        };
        (state, secrets)
    }

    fn current_provider(id: &str, priority: i64) -> ProviderConfig {
        ProviderConfig {
            id: id.to_string(),
            name: format!("当前-{id}"),
            base_url: format!("https://{id}.example.com/v1"),
            models: Vec::new(),
            priority,
            enabled: true,
            timeout_ms: 30_000,
            api_key: format!("current-key-{id}"),
            api_key_error: None,
        }
    }

    #[test]
    fn encrypted_backup_round_trips_without_plaintext_secrets() {
        let payload = sample_payload();
        let encrypted = encrypt_payload(&payload, "sample-correct-password").unwrap();
        let serialized = String::from_utf8(encrypted.clone()).unwrap();
        assert!(!serialized.contains("sample-provider-key"));
        assert!(!serialized.contains("sample-local-token"));
        assert!(!serialized.contains("https://example.com/v1"));

        let restored = decrypt_payload(&encrypted, "sample-correct-password").unwrap();
        assert_eq!(restored.providers[0].api_key, "sample-provider-key");
        assert_eq!(restored.local_token, "sample-local-token");
    }

    #[test]
    fn old_backup_without_codex_stream_retries_uses_default() {
        let mut payload = serde_json::to_value(sample_payload()).unwrap();
        payload["routing_policy"]
            .as_object_mut()
            .unwrap()
            .remove("codex_stream_max_retries");

        let restored: BackupPayloadData = serde_json::from_value(payload).unwrap();

        assert_eq!(restored.routing_policy.codex_stream_max_retries, 5);
    }

    #[test]
    fn export_repairs_missing_credential_from_runtime_secret() {
        let provider = current_provider("provider-1", 1);
        let expected = provider.api_key.clone();
        let (state, secrets) = import_test_state(vec![provider], true);
        secrets.delete("provider-1").unwrap();

        let (payload, warnings) = build_payload(&state).unwrap();

        assert!(warnings.is_empty());
        assert_eq!(payload.providers[0].api_key, expected);
        assert_eq!(
            secrets.get("provider-1").unwrap().as_deref(),
            Some(expected.as_str())
        );
    }

    #[test]
    fn export_uses_runtime_secret_and_warns_when_credential_repair_fails() {
        let provider = current_provider("provider-1", 1);
        let expected = provider.api_key.clone();
        let (mut state, _) = import_test_state(vec![provider], true);
        state.secrets = Arc::new(UnavailableSecretStore);

        let (payload, warnings) = build_payload(&state).unwrap();

        assert_eq!(payload.providers[0].api_key, expected);
        assert_eq!(warnings.len(), 1);
        assert!(!warnings[0].contains(&expected));
    }

    #[test]
    fn export_rejects_provider_without_stored_or_runtime_secret() {
        let mut provider = current_provider("provider-1", 1);
        provider.api_key.clear();
        let (state, secrets) = import_test_state(vec![provider], true);
        secrets.delete("provider-1").unwrap();

        let error = match build_payload(&state) {
            Ok(_) => panic!("缺少密钥时不应生成备份"),
            Err(error) => error,
        };

        assert!(error.contains("没有可导出的 API Key"));
    }

    #[test]
    fn wrong_password_and_tampered_ciphertext_are_rejected() {
        let encrypted = encrypt_payload(&sample_payload(), "sample-correct-password").unwrap();
        assert!(decrypt_payload(&encrypted, "sample-wrong-password").is_err());

        let mut envelope: BackupEnvelope = serde_json::from_slice(&encrypted).unwrap();
        envelope.cipher.ciphertext.push('A');
        let tampered = serde_json::to_vec(&envelope).unwrap();
        assert!(decrypt_payload(&tampered, "sample-correct-password").is_err());
    }

    #[test]
    fn unsupported_version_is_rejected_before_decryption() {
        let encrypted = encrypt_payload(&sample_payload(), "sample-correct-password").unwrap();
        let mut envelope: BackupEnvelope = serde_json::from_slice(&encrypted).unwrap();
        envelope.version = 99;
        let unsupported = serde_json::to_vec(&envelope).unwrap();
        let error = match decrypt_payload(&unsupported, "sample-correct-password") {
            Ok(_) => panic!("未知备份版本不应成功解密"),
            Err(error) => error,
        };
        assert!(error.contains("不支持"));
    }

    #[test]
    fn replace_import_restores_full_configuration_and_removes_old_secrets() {
        let (state, secrets) = import_test_state(vec![current_provider("old", 1)], true);
        let payload = sample_payload();
        let result = apply_import(&state, payload, BackupImportMode::Replace).unwrap();

        assert_eq!(result.created_count, 1);
        assert_eq!(result.removed_count, 1);
        let providers = state.config.read().unwrap();
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id, "provider-1");
        assert_eq!(providers[0].api_key, "sample-provider-key");
        drop(providers);
        assert_eq!(
            secrets.get("provider-1").unwrap().as_deref(),
            Some("sample-provider-key")
        );
        assert!(secrets.get("old").unwrap().is_none());
        assert_eq!(state.settings.read().unwrap().probe_interval_sec, 60);
        assert_eq!(state.settings.read().unwrap().port, 8765);
        assert_eq!(
            state.settings.read().unwrap().local_token,
            "sample-local-token"
        );
        assert_eq!(
            state.locked_provider_id.read().unwrap().as_deref(),
            Some("provider-1")
        );
    }

    #[test]
    fn merge_import_keeps_target_settings_lock_and_existing_priority() {
        let current = current_provider("provider-1", 4);
        let second = current_provider("target-only", 7);
        let (state, secrets) = import_test_state(vec![current, second], true);
        *state.locked_provider_id.write().unwrap() = Some("target-only".to_string());
        let mut payload = sample_payload();
        payload.providers.push(BackupProvider {
            id: "new-provider".to_string(),
            name: "新增中转".to_string(),
            base_url: "https://new.example.com/v1".to_string(),
            models: Vec::new(),
            priority: 2,
            enabled: true,
            timeout_ms: 45_000,
            api_key: "new-provider-key".to_string(),
        });

        let result = apply_import(&state, payload, BackupImportMode::Merge).unwrap();
        assert_eq!(result.created_count, 1);
        assert_eq!(result.updated_count, 1);
        assert_eq!(result.removed_count, 0);
        let providers = state.config.read().unwrap();
        assert_eq!(
            providers
                .iter()
                .find(|item| item.id == "provider-1")
                .unwrap()
                .priority,
            4
        );
        assert_eq!(
            providers
                .iter()
                .find(|item| item.id == "new-provider")
                .unwrap()
                .priority,
            8
        );
        drop(providers);
        assert_eq!(state.settings.read().unwrap().probe_interval_sec, 30);
        assert_eq!(state.settings.read().unwrap().port, 8765);
        assert_eq!(
            state.settings.read().unwrap().local_token,
            "sample-target-local-token"
        );
        assert_eq!(
            state.locked_provider_id.read().unwrap().as_deref(),
            Some("target-only")
        );
        assert_eq!(
            secrets.get("provider-1").unwrap().as_deref(),
            Some("sample-provider-key")
        );
    }

    #[test]
    fn database_failure_rolls_back_imported_secrets() {
        let current = current_provider("provider-1", 1);
        let original_key = current.api_key.clone();
        let (state, secrets) = import_test_state(vec![current], false);
        let mut payload = sample_payload();
        payload.providers.push(BackupProvider {
            id: "new-provider".to_string(),
            name: "新增中转".to_string(),
            base_url: "https://new.example.com/v1".to_string(),
            models: Vec::new(),
            priority: 2,
            enabled: true,
            timeout_ms: 45_000,
            api_key: "new-provider-key".to_string(),
        });

        assert!(apply_import(&state, payload, BackupImportMode::Replace).is_err());
        assert_eq!(
            secrets.get("provider-1").unwrap().as_deref(),
            Some(original_key.as_str())
        );
        assert!(secrets.get("new-provider").unwrap().is_none());
        assert_eq!(state.config.read().unwrap().len(), 1);
    }
}
