# AI Gateway 故障排查与已知限制

排查前先确认以下基础状态：

1. 托盘中只有一个 AI Gateway 实例。
2. 运行状态显示网关正在监听预期端口。
3. 浏览器或 PowerShell 访问 `/healthz` 返回 `ok`。
4. Codex 或 cc-switch 使用的 Base URL 与当前端口一致。
5. 运行日志中的时间、中转和模型与问题请求对应。

默认健康地址：

```powershell
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8765/healthz
```

日志和截图必须先脱敏，不能公开 API Key、Token、Cookie、请求正文、数据库或 `auth.json`。

## `stream disconnected before completion`

这表示客户端没有等到完整流式响应。可能原因包括：

- 上游主动关闭连接。
- 中转返回内容解码或传输错误。
- Responses SSE 没有合法终态。
- 网络、VPN 或中转服务发生短暂中断。
- 账号并发或容量限制包装在 SSE `response.failed` 中。

检查运行日志的诊断详情：

- HTTP 状态。
- 错误代码和类型。
- 最后一个 SSE 事件。
- 是否收到合法终态。
- 是否已经产生业务输出。
- 当前尝试次数和处理结果。

输出前的可重试错误会按配置重试当前中转；输出已经开始后，网关不会重放请求，以避免重复工具调用和重复计费。Codex 侧按“Codex 流恢复次数”继续处理，默认最多恢复 5 次；修改该值后需要完全退出并重新启动 Codex。

## `stream closed before response.completed`

该错误专门表示 Responses 流在结束前没有收到：

```text
response.completed
response.failed
response.incomplete
```

如果第三方中转只发送 `[DONE]`，但接口实际是 `/v1/responses`，其协议并不完整。AI Gateway 会在输出前尝试重试；输出后则向客户端发送结构化 `upstream_stream_incomplete` 失败事件并记录诊断。

Chat Completions 的 `[DONE]` 是正常结束标记，不适用 Responses 的终态要求。

## `Selected model is at capacity`

模型容量不足会按统一配置重试同一个中转和同一个模型：

- 默认额外重试 5 次。
- 默认间隔 1 秒。
- 重试次数不设产品级上限，`0` 表示不重试。
- 遵循可用的 `Retry-After`；最大累计等待为 `0` 时不限制等待，正数时不得超过该预算。
- 不增加中转健康失败数。
- 不触发熔断。
- 即使允许自动切换，也不会改走备用中转。

重试耗尽后保留最后一次上游状态和错误内容。

如果请求长时间保持重试状态，请检查是否同时配置了很大的重试次数和 `0` 秒最大累计等待。此组合会严格按次数执行，可改回有限等待预算以提前止损。

## `Concurrency limit exceeded for account`

这是上游账号级并发限制，不是 AI Gateway 本地并发限制。处理方式与容量不足一致：只重试当前中转，不自动切换。

持续出现时应减少并发请求、等待账号额度释放，或联系中转服务商确认账号限制。手动再次输入“继续”能够成功，通常表示并发窗口已经释放。

## `No available channel for model/group`

这表示当前中转内部没有可用于该模型或分组的通道，不等同于整个中转宕机。AI Gateway 会按统一配置只重试当前中转，重试耗尽后显示“模型暂不可用”，不会增加线路连续失败次数，也不会自动切换到备用中转。

## 熔断后手动测试正常，但 Codex 仍无法使用

`0.3.7` 起，自动切换关闭时熔断不再是本地请求门禁。监控页会显示“近期不稳定”，但 Codex 的新请求仍会继续尝试当前中转。

自动切换开启时，只有连接、DNS、TLS、响应头超时、通用 502/503/504 和流传输故障会打开线路熔断。冷却结束后状态进入半开，下一次真实模型请求确认恢复。手动测试调用的是 `/models`，成功只表示基础连接正常，不能证明某个具体模型已经恢复。

## 401 本地访问令牌无效

常见原因：

- 客户端没有填写 API Key。
- 填写了第三方 API Key，而不是配置中心的本地访问令牌。
- 修改或重新生成本地令牌后，客户端仍使用旧值。
- Codex `config.toml` 中的 AI Gateway Provider 未同步当前令牌。

处理步骤：

1. 在配置中心复制当前本地访问令牌。
2. cc-switch 使用 `Authorization: Bearer <本地令牌>`。
3. Codex 集成使用 `X-AI-Gateway-Token`，必要时执行“修复接入配置”。
4. 完全重启 Codex 后重试。

## 请求体超过 200 MiB

本地超限返回：

```text
HTTP 413
local_request_body_too_large
limit = 209715200
```

这是 AI Gateway 的本地保护，不会请求任何中转，也不会重试、熔断或切换线路。需要缩小请求、附件或上下文。

如果错误来自第三方上游，日志会显示上游 413。只有开启自动切换且 413 策略选择切换备用中转时，网关才会尝试其他线路。

## 端口被占用

检查默认端口：

```powershell
Get-NetTCPConnection -State Listen -LocalPort 8765
```

安装版和便携版共用配置，不能同时运行。也可能有其他程序占用端口。

在配置中心选择 `1024-65535` 范围内的新端口并保存。绑定失败时旧端口继续运行；保存成功后要同步更新 Codex 或 cc-switch 的 Base URL。

## API Key 重启后消失或保存失败

`0.2.8` 之前的构建曾未启用 Windows 原生密钥后端，写入只存在进程内存中，因此退出后丢失，并可能在回读验证时报错。

当前版本使用 Windows Credential Manager。旧版本已经丢失的密钥无法从 SQLite 恢复，需要重新填写一次。若当前版本仍保存失败：

1. 确认运行的是 `0.2.8` 或更高版本。
2. 检查 Windows Credential Manager 服务和当前用户权限。
3. 确认安装版与便携版使用相同 Windows 用户。
4. 查看日志中是“未配置密钥”还是“系统密钥库读取失败”。
5. 尝试重新保存对应中转的 API Key。

不要把真实密钥写进 SQLite、日志或配置文件作为绕过方案。

## Codex 登录信息或账号能力不显示

AI Gateway 只读检查 `auth.json` 中是否存在可识别凭据，不会修改该文件。页面显示“已登录”代表本地凭据存在；凭据是否仍有效，以 Codex 桌面端实际状态为准。

检查：

- `model_provider` 是否为共享 `custom`。
- `model_providers.custom.requires_openai_auth` 是否为 `true`。
- 是否在模式切换后完全退出并重启 Codex。
- 是否有 CC-Switch 或其他工具覆盖 `config.toml`。
- 接入页是否能识别本地 ChatGPT 登录凭据。

如果 AI Gateway 显示“外部配置”，先停止其他工具的路由接管，再明确选择由 AI Gateway 接管或切回官方模式。

## 会话历史不完整

Codex 可能按 Provider 元数据筛选本地历史。“统一会话历史”可以将普通和归档会话的 Provider 统一为 `custom`。

- 执行前完全退出 Codex。
- 先扫描并核对预览。
- 迁移不会解除归档状态。
- 迁移只保证会话可见，不保证包含 `encrypted_content` 的旧会话能跨供应商继续推理。
- 出现异常时使用“恢复最近一次迁移”。

## cc-switch 与 AI Gateway 配置冲突

两个程序同时管理 Codex `custom` Provider 时，最后写入者会覆盖另一个工具的连接参数。建议选择一种接管方式：

- 让 AI Gateway 直接管理 Codex，退出 CC-Switch 的路由接管。
- 或只把 AI Gateway 当作 cc-switch 的一个本地 OpenAI 兼容 Provider，不再使用 AI Gateway 的 Codex 配置切换。

不要同时运行安装版和便携版，也不要同时让多个工具自动修复同一 `config.toml`。

## 用量统计为空或不一致

第三方用量优先使用上游明确返回的 `usage`。如果服务商没有上报：

- 已确认进入生成阶段的请求使用 `tiktoken` 估算输入和已观察输出。
- 连接失败、无输出的 429/5xx 等请求保持“未统计”，不会虚构 Token。
- 缓存命中没有明确值时保持未上报，不参与估算。
- 重试和自动切换的每次实际上游尝试分别计量，因此请求次数可能高于 Codex 中看到的轮次数。

官方用量来自 Codex Desktop 本地 `token_count`。`0.3.10` 首次启动会使用解析器 v3 自动重建一次保留期内的会话，点击刷新按钮可以执行后续增量同步。共享 `custom` Provider 下即使事件带有 `rate_limits`，也不会因此归为官方；第三方模式无法唯一匹配网关请求的事件不会计入统计。

如果官方行仍显示“未统计”，先查看页面同步摘要：

- 扫描文件为 `0`：检查 `%USERPROFILE%\.codex\sessions` 是否存在；独立 `CODEX_HOME` 也会被扫描。
- 扫描到文件但发现事件为 `0`：执行“重建 Codex 用量”，并检查是否存在解析警告。
- 发现事件但没有导入：可能是 Gateway 模式下没有唯一网关候选、重复事件、跨来源关联或文件仍在写入；这类事件会保留诊断但不会被计入官方或第三方总量。完全退出 Codex 后重建可获得稳定快照。

输入 Token 已包含缓存命中，总 Token 等于输入加输出。缓存和推理是细项，不应再次叠加。无法可靠判断官方或第三方来源的旧记录显示为“来源未确认”。

统计不等同于 OpenAI 或第三方服务商账单，可能受到本地会话缺失、上游未上报、来源歧义和保留期限影响。

## Rust 测试报 `STATUS_ENTRYPOINT_NOT_FOUND`

完整错误通常包含：

```text
0xC0000139 STATUS_ENTRYPOINT_NOT_FOUND
```

这是 Windows 在启动 Cargo 测试运行器时加载到不匹配 DLL 的错误，不是 AI Gateway 运行时返回的网关错误。正式桌面 EXE 带有匹配的发布依赖，因此可以正常运行。

处理方式见[开发与构建](development.md#status_entrypoint_not_found)。在修复环境前，只能报告“测试目标编译通过”，不能报告“测试运行通过”。

## 已知限制

- 当前优先支持 Windows x64，未正式支持 macOS、Linux、WSL、SSH 远程 Codex 和 profile 覆盖。
- 官方模式完全绕过 AI Gateway，因此不能从网络链路采集官方请求和响应。
- 官方 Token 来自 Codex 本地记录，不等同于 OpenAI 账单、订阅额度或重置时间。
- 已经产生业务输出后的流断开不会由网关自动重放。
- 第三方中转之间不保证支持相同模型、工具、加密上下文或响应扩展。
- 统一会话可见性不代表旧会话能在任意第三方继续推理。
- 本地请求体固定上限为 200 MiB，目前不能在页面修改。
- `.aigateway` 密码无法找回，忘记密码只能重新导出。
- 安装版和便携版共享数据与监听端口，不能同时运行。
