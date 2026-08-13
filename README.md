# AI Gateway

AI Gateway 是一个 Windows 优先的本地桌面路由器。它可以直接管理 Codex 的官方直连与第三方接入模式；第三方模式下 Codex 只连接固定的本地 OpenAI 兼容地址，应用负责真实中转、健康探测、熔断和优先级故障转移。

## Codex 接入

- OpenAI 官方：固定使用共享 `model_providers.custom`，不设置 `base_url`，Codex 使用原生 ChatGPT 登录并完全绕过本地网关。
- 第三方中转：仍使用同一个 `custom` Provider，只把 `base_url` 指向本地网关。第三方中转之间的切换由网关内部完成，不需要重启 Codex。
- 两种模式都设置 `requires_openai_auth = true`，因此切换第三方后仍保留 ChatGPT 账号、额度和依赖官方登录的能力。
- 官方和第三方模式互相切换后，需要完全退出并重新启动 Codex。
- AI Gateway 永远不读取、修改或备份 `auth.json`，官方登录态由 Codex 自己维护。
- 配置路径优先使用 `CODEX_HOME`，否则使用 `%USERPROFILE%\.codex\config.toml`。写入前会在同目录创建备份并保留最近 10 份。

第三方模式支持 `X-AI-Gateway-Token` 本地鉴权头，同时继续兼容 cc-switch 使用的 `Authorization: Bearer <本地令牌>`。网关转发前会删除官方 Authorization、Cookie、账号、组织、项目及 `X-OpenAI-*` 身份头，再写入中转自己的 API Key。

“统一会话历史”会在用户预览并确认后，将普通和归档 JSONL 首条 `session_meta.payload.model_provider` 及 Codex 本地索引统一为 `custom`。归档会话仍保持归档，聊天正文不修改，备份最多保留 3 份。该操作只保证历史可见；包含 `encrypted_content` 的旧会话不保证能由任意第三方继续推理。使用 CC-Switch 时请先关闭其路由接管，避免两个工具同时改写 `custom` Provider。

## 技术栈

- Tauri 2
- Rust、Tokio、Axum、Reqwest
- React、TypeScript、Vite
- SQLite
- Windows Credential Manager

## 本地开发

前置依赖：

- Node.js 20+
- Rust stable MSVC toolchain
- Visual Studio Build Tools，包含 Desktop development with C++
- WebView2 Runtime

安装依赖并启动：

```powershell
npm install
npm run tauri dev
```

仅验证前端：

```powershell
npm run build
```

验证 Rust：

```powershell
cargo check --manifest-path src-tauri/Cargo.toml
cargo test --manifest-path src-tauri/Cargo.toml
```

## Windows 发布

生成 NSIS 安装包：

```powershell
npm run tauri -- build --bundles nsis
```

安装包、便携版目录、数据库、运行日志和本地 QA 截图均属于本机产物，不提交到 Git。安装版与便携版共用 `%APPDATA%\com.ai-gateway.desktop` 中的配置和 Windows Credential Manager 中的中转密钥。

`0.3.0` 将 Codex 官方和第三方接入统一到共享 `custom` Provider，并增加本地会话历史迁移与恢复。此前 `0.2.8` 修复了旧版未启用 Windows 原生凭据后端的问题：旧版已经丢失的 API Key 不存在于 SQLite，无法自动恢复；升级后需要重新填写一次。删除便携版目录不会删除 Windows Credential Manager 中的密钥。

## cc-switch 接入

应用启动后，在 cc-switch 新增 OpenAI 兼容 Provider：

```text
Base URL: http://127.0.0.1:8765/v1
API Key: 网关设置页面显示的本地访问令牌
```

`8765` 是默认端口，可在配置中心修改为 `1024–65535` 范围内的其他端口。保存前会检查新端口是否可用；切换成功后需要同步更新 cc-switch 的 Base URL，本地 API Key 不变。端口被占用时保存失败，原端口会继续提供服务。

只填写 Base URL 会收到 `401 本地访问令牌无效`；API Key 不是任意真实中转密钥，必须复制“配置中心”中的本地访问令牌。若 cc-switch 自动拼接接口路径，请保持 Base URL 以 `/v1` 结尾，不要重复填写 `/v1`。

真实中转的 Base URL 和 API Key 只在 AI Gateway 内配置。API Key 保存到操作系统密钥库，普通日志不会记录请求正文或鉴权头。

AI Gateway 在验证本地访问令牌后读取请求体，本地上限为 200 MiB。该限制用于避免本机异常进程无限占用内存，并高于 Axum 默认的 2 MiB。超过本地限制时错误类型为 `local_request_body_too_large`，不会触发中转切换或熔断；真实中转返回的 HTTP 413 才会进入下方的故障转移规则。

## Token 用量

“用量统计”页面按中转展示输入、输出、缓存、推理和总 Token，默认查询当天，也支持最多 30 个自然日的日期范围和完整自然月。统计只读取中转明确返回的 `usage`，不会修改请求或本地估算，因此页面会同时显示上报覆盖率。用量从安装支持该功能的版本后开始累计，无法从旧运行日志准确补录。

用量数据默认保留 365 天，可在配置中心调整为 30–3650 天。历史记录保存在本机 SQLite 中，不包含 prompt、响应正文或 API Key，也不会写入 `.aigateway` 配置备份。

## 故障转移边界

- 连接失败、超时、HTTP 408、429 和 5xx 会尝试下一个可用中转。
- 模型容量不足会按独立策略重试当前中转，不触发熔断，也不会自动切换到其他中转。
- HTTP 413 默认立即尝试备用中转，并在配置的跳过期内避开无法承载相同或更大请求体的线路。
- 400、401、403、404 等请求或配置错误直接返回，不做级联切换。
- 上游已经返回响应头并开始流式输出后，不重放当前请求，避免重复执行和重复计费。
- 连续失败阈值、熔断冷却、重试、健康探测、日志和用量保留期限均可在配置中心调整；429 优先使用 `Retry-After`。

## 配置备份

中转管理支持导出和导入 `.aigateway` 加密备份。备份可包含中转 API Key、本地访问令牌和路由规则，导出密码不会保存到应用或日志中。监听端口属于每台电脑的本机配置，不会被备份覆盖。跨电脑迁移时使用加密备份，不要复制数据库或导出明文密钥。
