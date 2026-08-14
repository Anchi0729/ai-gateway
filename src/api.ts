import { invoke } from "@tauri-apps/api/core";

export type ProviderHealth =
  "healthy" | "degraded" | "open" | "half_open" | "unknown";

export interface Provider {
  id: string;
  name: string;
  base_url: string;
  models: string[];
  priority: number;
  enabled: boolean;
  timeout_ms: number;
  has_api_key: boolean;
  api_key_error: string | null;
}

export interface HealthProbeSample {
  sequence: number;
  timestamp: string;
  state: ProviderHealth;
  latency_ms: number | null;
  error: string | null;
}

export interface HealthSnapshot {
  provider_id: string;
  state: ProviderHealth;
  latency_ms: number | null;
  consecutive_failures: number;
  last_error: string | null;
  cooldown_until: string | null;
  total_requests: number;
  successful_requests: number;
  payload_rejected_size: number | null;
  payload_rejected_until: string | null;
  recent_probes: HealthProbeSample[];
}

export interface RouterSettings {
  bind_address: string;
  port: number;
  local_token: string;
  probe_interval_sec: number;
  log_retention_days: number;
  usage_retention_days: number;
  gateway_running: boolean;
  gateway_listening: boolean;
  gateway_error: string | null;
}

export interface GatewayTestResult {
  ok: boolean;
  status_code: number | null;
  message: string;
  provider_count: number;
  enabled_provider_count: number;
}

export interface RouteGroup {
  id: string;
  name: string;
  provider_ids: string[];
  locked_provider_id: string | null;
}

export interface RuntimeStats {
  total_requests: number;
  successful_requests: number;
  average_latency_ms: number | null;
  active_provider_id: string | null;
}

export interface EventLog {
  timestamp: string;
  level: "info" | "warning" | "error";
  message: string;
  provider_id: string | null;
  model: string | null;
  event_type: string | null;
  routing_event: RoutingEvent | null;
  diagnostic_detail: DiagnosticDetail | null;
}

export type PayloadTooLargeAction = "failover" | "return";

export interface RoutingPolicy {
  retry_count: number;
  retry_interval_sec: number;
  max_retry_wait_sec: number;
  retry_unknown_transient_errors: boolean;
  allow_provider_failover: boolean;
  capacity_retry_count: number;
  capacity_retry_interval_sec: number;
  capacity_max_retry_wait_sec: number;
  circuit_failure_threshold: number;
  circuit_cooldown_sec: number;
  payload_too_large_skip_sec: number;
  payload_too_large_action: PayloadTooLargeAction;
  notify_switches: boolean;
}

export interface DiagnosticDetail {
  request_id: string;
  provider_id: string;
  model: string | null;
  request_path: string;
  error_class: "retryable" | "permanent" | "incomplete_business" | "unknown";
  http_status: number | null;
  error_code: string | null;
  error_type: string | null;
  error_message: string | null;
  incomplete_reason: string | null;
  upstream_request_id: string | null;
  last_sse_event: string | null;
  terminal_event: string | null;
  terminal_received: boolean;
  business_output_started: boolean;
  bytes_received: number;
  elapsed_ms: number;
  attempt_index: number;
  max_attempts: number;
  retry_wait_sec: number | null;
  fallback_retry_enabled: boolean;
  failover_enabled: boolean;
  action: string;
  parse_error: string | null;
  attempts: RoutingAttempt[];
}

export interface ConfigurationSnapshot {
  router_settings: RouterSettings;
  routing_policy: RoutingPolicy;
  autostart_enabled: boolean;
}

export interface ConfigurationInput {
  port: number;
  probe_interval_sec: number;
  log_retention_days: number;
  usage_retention_days: number;
  routing_policy: RoutingPolicy;
  autostart_enabled: boolean;
}

export interface RoutingAttempt {
  provider_id: string;
  provider_name: string;
  status_code: number | null;
  error: string | null;
  latency_ms: number | null;
  retry_count: number;
  switched: boolean;
}

export interface RoutingEvent {
  id: string;
  timestamp: string;
  event_type: string;
  model: string | null;
  request_path: string;
  request_body_bytes: number;
  source_provider_id: string | null;
  target_provider_id: string | null;
  status_code: number | null;
  retry_count: number;
  attempts: RoutingAttempt[];
  reason: string;
  final_result: string;
}

export type BackupImportMode = "replace" | "merge";

export interface BackupPreview {
  format_version: number;
  exported_at: string;
  app_version: string;
  provider_count: number;
  conflict_count: number;
  includes_local_token: boolean;
}

export interface BackupExportResult {
  file_name: string;
  provider_count: number;
  warning_messages: string[];
}

export interface BackupImportResult {
  created_count: number;
  updated_count: number;
  removed_count: number;
  warning_messages: string[];
}

export type UsageQueryMode = "today" | "range" | "month";
export type UsageSource = "all" | "official" | "gateway" | "unconfirmed";

export interface UsageQuery {
  mode: UsageQueryMode;
  start_date?: string;
  end_date?: string;
  month?: string;
  source: UsageSource;
}

export interface TokenMetric {
  value: number;
  estimated_value: number;
  reported_count: number;
}

export interface UsageMetrics {
  input_tokens: TokenMetric;
  output_tokens: TokenMetric;
  cache_hit_tokens: TokenMetric;
  total_tokens: TokenMetric;
}

export interface UsagePeriod {
  mode: UsageQueryMode;
  start_date: string;
  end_date: string;
  label: string;
  resolution: "hour" | "day";
}

export interface ProviderUsageStatistics {
  provider_id: string;
  provider_name: string;
  deleted: boolean;
  completed_requests: number;
  reported_requests: number;
  coverage_percent: number;
  data_quality: "exact" | "mixed" | "estimated" | "unknown";
  metrics: UsageMetrics;
}

export interface UsageSeriesPoint {
  key: string;
  label: string;
  completed_requests: number;
  reported_requests: number;
  metrics: UsageMetrics;
}

export interface UsageStatistics {
  period: UsagePeriod;
  totals: UsageMetrics;
  providers: ProviderUsageStatistics[];
  series: UsageSeriesPoint[];
  completed_requests: number;
  reported_requests: number;
  coverage_percent: number;
  data_quality: "exact" | "mixed" | "estimated" | "unknown";
  codex_sync_status: CodexUsageSyncStatus;
}

export interface CodexUsageSyncStatus {
  state: "idle" | "success" | "warning" | "error" | "syncing";
  last_synced_at: string | null;
  roots_scanned: number;
  files_scanned: number;
  token_events_found: number;
  imported_count: number;
  matched_count: number;
  skipped_count: number;
  deferred_count: number;
  parser_version: number;
  rebuild_performed: boolean;
  warning_messages: string[];
}

export type CodexConnectionMode = "official" | "gateway" | "external";
export type CodexLoginStatus = "logged_in" | "logged_out" | "unknown";
export type CodexApplicationStatus =
  | "not_installed"
  | "installed_stopped"
  | "installed_running";

export interface CodexIntegrationState {
  desktop_status: CodexApplicationStatus;
  cli_status: CodexApplicationStatus;
  login_status: CodexLoginStatus;
  current_mode: CodexConnectionMode;
  gateway_configured: boolean;
}

export interface HistoryProviderCount {
  provider: string;
  count: number;
}

export interface CodexHistoryMigrationPreview {
  preview_id: string;
  fingerprint: string;
  normal_session_count: number;
  archived_session_count: number;
  migration_required_count: number;
  provider_counts: HistoryProviderCount[];
  indexed_record_count: number;
  encrypted_content_session_count: number;
  warning_messages: string[];
  can_restore_latest: boolean;
}

export interface CodexHistoryMigrationResult {
  migrated_session_count: number;
  updated_index_count: number;
  backup_name: string;
  warning_messages: string[];
}

export interface CodexHistoryRestoreResult {
  restored_session_count: number;
  restored_index_count: number;
  warning_messages: string[];
}

export interface SwitchCodexModeResult {
  previous_mode: CodexConnectionMode;
  current_mode: CodexConnectionMode;
  changed: boolean;
  restart_required: boolean;
  warning_messages: string[];
}

export async function listProviders() {
  return invoke<Provider[]>("list_providers");
}

export async function saveProvider(
  input: Partial<Provider> & { api_key?: string },
) {
  return invoke<Provider>("save_provider", { input });
}

export async function deleteProvider(id: string) {
  return invoke<void>("delete_provider", { id });
}

export async function setProviderEnabled(id: string, enabled: boolean) {
  return invoke<void>("set_provider_enabled", { id, enabled });
}

export async function reorderProviders(ids: string[]) {
  return invoke<void>("reorder_providers", { ids });
}

export async function testProvider(id: string) {
  return invoke<{ ok: boolean; message: string; latency_ms?: number }>(
    "test_provider",
    { id },
  );
}

export async function testLocalGateway() {
  return invoke<GatewayTestResult>("test_local_gateway");
}

export async function getHealthSnapshots() {
  return invoke<HealthSnapshot[]>("get_health_snapshots");
}

export async function getRouterSettings() {
  return invoke<RouterSettings>("get_router_settings");
}

export async function getRoutingPolicy() {
  return invoke<RoutingPolicy>("get_routing_policy");
}

export async function getConfiguration() {
  return invoke<ConfigurationSnapshot>("get_configuration");
}

export async function getCodexIntegrationState() {
  return invoke<CodexIntegrationState>("get_codex_integration_state");
}

export async function switchCodexConnectionMode(mode: "official" | "gateway") {
  return invoke<SwitchCodexModeResult>("switch_codex_connection_mode", { mode });
}

export async function refreshCodexLoginStatus() {
  return invoke<CodexIntegrationState>("refresh_codex_login_status");
}

export async function repairCodexGatewayConfiguration() {
  return invoke<SwitchCodexModeResult>("repair_codex_gateway_configuration");
}

export async function previewCodexHistoryMigration() {
  return invoke<CodexHistoryMigrationPreview>("preview_codex_history_migration");
}

export async function applyCodexHistoryMigration(previewId: string) {
  return invoke<CodexHistoryMigrationResult>("apply_codex_history_migration", { previewId });
}

export async function restoreCodexHistoryMigration() {
  return invoke<CodexHistoryRestoreResult>("restore_codex_history_migration");
}

export async function saveConfiguration(input: ConfigurationInput) {
  return invoke<ConfigurationSnapshot>("save_configuration", { input });
}

export async function saveRoutingPolicy(policy: RoutingPolicy) {
  return invoke<RoutingPolicy>("save_routing_policy", { policy });
}

export async function saveRouterSettings(
  port: number,
  probeIntervalSec: number,
) {
  return invoke<RouterSettings>("save_router_settings", {
    port,
    probeIntervalSec,
  });
}

export async function toggleGateway(running: boolean) {
  return invoke<void>("toggle_gateway", { running });
}

export async function listRouteGroups() {
  return invoke<RouteGroup[]>("list_route_groups");
}

export async function updateRouteGroup(
  providerIds: string[],
  lockedProviderId: string | null,
) {
  return invoke<RouteGroup>("update_route_group", {
    input: { provider_ids: providerIds, locked_provider_id: lockedProviderId },
  });
}

export async function getRuntimeStats() {
  return invoke<RuntimeStats>("get_runtime_stats");
}

export async function getUsageStatistics(query: UsageQuery) {
  return invoke<UsageStatistics>("get_usage_statistics", { query });
}

export async function syncCodexUsage() {
  return invoke<CodexUsageSyncStatus>("sync_codex_usage");
}

export async function rebuildCodexUsage() {
  return invoke<CodexUsageSyncStatus>("rebuild_codex_usage");
}

export async function getRecentLogs() {
  return invoke<EventLog[]>("get_recent_logs");
}

export async function reloadConfig() {
  return invoke<void>("reload_config");
}

export async function exportConfigurationBackup(
  path: string,
  password: string,
) {
  return invoke<BackupExportResult>("export_configuration_backup", {
    path,
    password,
  });
}

export async function inspectConfigurationBackup(
  path: string,
  password: string,
) {
  return invoke<BackupPreview>("inspect_configuration_backup", {
    path,
    password,
  });
}

export async function importConfigurationBackup(
  path: string,
  password: string,
  mode: BackupImportMode,
) {
  return invoke<BackupImportResult>("import_configuration_backup", {
    path,
    password,
    mode,
  });
}
