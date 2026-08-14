# AI Gateway 设计说明

本文说明 AI Gateway 的内部架构、请求处理边界、路由策略、Codex 接入和用量采集口径。它不是 API 参考，而是帮助维护者理解系统行为的设计文档。

## 总体架构

```text
React + TypeScript + Vite
          |
       Tauri Commands
          |
      Rust AppState
          |
  +------------------------+
  | 本地 HTTP 网关         |
  | 路由与重试             |
  | 健康检查与熔断         |
  | SSE 流观察与透传       |
  | 用量采集与 Codex 同步  |
  | Codex 配置管理         |
  | 加密备份服务           |
  +------------------------+
          |
  SQLite + Windows Credential Manager
```

主要代码职责：

- `src-tauri/src/lib.rs`：Tauri 入口、本地 Axum 网关、路由、重试、SSE、健康状态、日志和用量落库。
- `src-tauri/src/backup.rs`：`.aigateway` 加密备份的导出、导入和兼容处理。
- `src-tauri/src/codex_integration.rs`：Codex `config.toml` 的模式切换、配置校验和会话历史管理。
- `src-tauri/src/codex_usage.rs`：只读扫描 Codex 本地会话 Token 记录并同步到用量数据库。
- `src/App.tsx`：桌面页面、配置中心、中转管理、用量统计和 Codex 接入界面。
- `src/api.ts`：前端类型和 Tauri Command 调用封装。

## 请求处理流程

一次第三方请求大致经过以下阶段：

```text
客户端请求
  -> 检查网关运行状态
  -> 验证 X-AI-Gateway-Token 或本地 Bearer Token
  -> 检查 Content-Length
  -> 在 200 MiB 上限内读取请求体
  -> 只提取 model 元数据
  -> 选择当前中转
  -> 等待上游响应头
  -> 分类响应或 SSE 预检错误
  -> 当前中转重试
  -> 按总开关决定是否切换备用中转
  -> Axum 流式透传响应
  -> 记录健康状态、日志和 usage
```

本地鉴权发生在读取请求体之前，避免未授权进程通过大请求消耗内存。请求体在网关内缓冲一次，以支持模型选择和安全重试；重试使用引用计数的 `Bytes`，不重复构造 JSON 副本。

## 超时和流式响应

Provider 的 `timeout_ms` 是响应头超时，不是完整请求总时长。它限制 DNS、连接、TLS、上传请求体和等待上游响应头；响应头已经返回后，SSE 可以持续到正常结束或客户端取消。

Responses SSE 需要观察合法终态：

- `response.completed`：正常完成，可落库 usage。
- `response.failed`：上游明确失败，按错误分类处理。
- `response.incomplete`：上游业务或临时不完整，按 `incomplete_details.reason` 区分。

Chat Completions 继续遵循自身的 `[DONE]` 结束语义。SSE 事件达到语义观察上限时会降级为原始透传，不让诊断缓冲影响响应内容或内存使用。

输出前只暂存生命周期事件，尚未产生文本、推理或工具调用时可以安全重试。输出开始后不再重放当前请求；底层断流只影响当前响应和后续健康状态，Codex 可使用有限的 `stream_max_retries` 尝试恢复。

## 路由、重试和故障转移

`retry_count` 表示额外重试次数，默认 5 次，初始请求加重试最多 6 次。`retry_interval_sec` 是默认间隔，`Retry-After` 在适用时优先；累计等待超过 `max_retry_wait_sec` 后停止。

`allow_provider_failover` 是自动切换总开关，默认关闭：

- 关闭时只坚持当前中转，熔断时返回“自动切换已关闭”的本地错误。
- 开启时，普通瞬时错误在当前中转重试耗尽后才允许尝试备用中转。
- 容量不足和账号并发限制始终只重试原中转，不扩散到其他线路。
- 上游 413 还要同时满足 413 策略，才可能切换。
- 本地请求体超限 413 永远不会进入路由切换。

错误处理矩阵：

| 错误                       |   当前中转重试 |     自动切换 |
| -------------------------- | -------------: | -----------: |
| 连接、DNS、TLS、响应头超时 |             是 | 受总开关控制 |
| 408、429、5xx              |             是 | 受总开关控制 |
| 模型容量不足               |             是 |           否 |
| 账号并发限制               |             是 |           否 |
| 普通 400/401/403/404       |             否 |           否 |
| 本地请求体超过 200 MiB     |             否 |           否 |
| 上游 413                   |    按 413 策略 | 受总开关控制 |
| 输出前未知 SSE 临时错误    | 受兜底开关控制 | 受总开关控制 |
| 输出后流式断开             |     网关不重放 |           否 |

每个请求使用开始时捕获的策略快照，配置保存不会改变已经运行中的请求。每次尝试、等待、切换和断流都生成脱敏诊断信息，不能包含 prompt、正文、API Key、Cookie 或完整请求头。

## Codex 接入设计

AI Gateway 不把 OpenAI 官方线路放入第三方 Provider 池。官方模式和第三方模式都使用共享 `custom` Provider：

```toml
model_provider = "custom"

[model_providers.custom]
requires_openai_auth = true
```

官方模式删除网关自己的 `base_url`，Codex 直连官方。第三方模式写入本地 Base URL、本地令牌和关闭 Codex 自身普通请求重试的配置：

```toml
base_url = "http://127.0.0.1:<port>/v1"
request_max_retries = 0
stream_max_retries = 2
```

模式切换需要重启 Codex，但第三方中转池内部切换只改变网关的内存路由快照，不修改 `config.toml`，因此不需要重启 Codex。

转发第三方前会移除官方 `Authorization`、`Cookie`、`ChatGPT-Account-ID`、组织/项目头和 `X-OpenAI-*` 身份头，再写入选定中转的 API Key。状态检测只读解析 `auth.json` 是否包含可识别凭据，不解码或输出 Token，也不修改、删除或备份该文件。

Codex 接入页分别检测桌面端和 CLI 的安装、运行状态。桌面端内部的 `codex.exe` 通过安装路径和父进程关系排除，避免误报为独立 CLI；登录状态只代表本地凭据存在，不代表 Token 已通过在线校验。

## 用量采集

第三方用量优先采集上游明确返回的结构化 `usage`：

- Responses JSON 顶层 `usage`。
- Responses SSE 的 `response.completed.response.usage`。
- Chat Completions JSON 的 `prompt_tokens`、`completion_tokens` 和 `total_tokens`。
- Chat Completions SSE 最终 usage chunk。

上游缺少 `usage` 时，仅对已确认进入模型生成阶段的请求使用 `tiktoken` 估算输入和已观察输出。连接失败、无模型输出的 429/5xx、本地鉴权错误和本地请求体限制只记录尝试结果，不虚构 Token。缓存命中只接受明确上报值。

每次实际上游尝试使用网关请求 ID、尝试序号和中转 ID 幂等落库。重试与自动切换产生的先前尝试不会被最终响应覆盖；Codex 本地 `token_count` 只会匹配或补正最终成功尝试。

官方模式完全绕过 AI Gateway，因此网关不能从网络请求中观察官方用量。官方统计器只读：

```text
%CODEX_HOME%\sessions\**\*.jsonl
%CODEX_HOME%\archived_sessions\*.jsonl
```

只解析 `session_meta`、`turn_context` 和 `event_msg.token_count`，不保存聊天正文，也不调用官方接口或对官方数据使用 tokenizer 估算。解析器使用文件名 UUID 和多个元数据字段识别会话，完整但损坏的单行只会被跳过，不会阻断后续事件。

Codex 的 `input_tokens` 采用包含缓存命中的 OpenAI 语义，`cached_input_tokens` 是输入细项，因此 `total_tokens = input_tokens + output_tokens`。同步游标包含来源目录指纹；解析器版本变化时先解析全部文件，再在单个 SQLite 事务中替换 Codex 记录、游标和事件关联，失败时回滚。通过网关的 Codex 请求会依据会话、模型、Token 快照和时间窗口去重；无法可靠确认来源的旧记录显示为“来源未确认”。

缓存命中和推理 Token 是输入/输出的细项，不再次加到总 Token。数据库保留完整细项，主页面只展示总量、输入、输出和缓存命中。

## SQLite、凭据和备份边界

- SQLite 保存中转元数据、路由设置、日志和用量统计。
- API Key 使用 Windows Credential Manager，不保存到 SQLite 明文。
- `.aigateway` 备份使用 Argon2id 派生密钥和 XChaCha20-Poly1305 加密。
- 备份不包含日志、用量历史、Codex 登录态、会话文件、监听端口或开机启动状态。
- 监听端口属于本机环境，跨电脑导入时保留目标机端口。

## 设计取舍

- 完整缓冲请求体换取重试和故障切换能力，因此设置固定 200 MiB 上限。
- 输出后不自动重放，优先避免重复工具调用和重复计费。
- 官方用量采用本地 Token 记录而不是自行估算；第三方缺失 usage 时使用有来源标记的本地估算。
- 不接管 OAuth，避免破坏 Codex 原生登录和官方能力。
