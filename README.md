# AI Gateway

AI Gateway 是一个 Windows 优先的本地桌面路由器。Codex 或 cc-switch 只连接固定的本地 OpenAI 兼容地址，应用负责管理真实中转、健康探测、熔断和优先级故障转移。

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

## cc-switch 接入

应用启动后，在 cc-switch 新增 OpenAI 兼容 Provider：

```text
Base URL: http://127.0.0.1:8765/v1
API Key: 网关设置页面显示的本地访问令牌
```

只填写 Base URL 会收到 `401 本地访问令牌无效`；API Key 不是任意真实中转密钥，必须复制“网关设置”中的本地访问令牌。若 cc-switch 自动拼接接口路径，请保持 Base URL 以 `/v1` 结尾，不要重复填写 `/v1`。

真实中转的 Base URL 和 API Key 只在 AI Gateway 内配置。API Key 保存到操作系统密钥库，普通日志不会记录请求正文或鉴权头。

## 故障转移边界

- 连接失败、超时、HTTP 408、429 和 5xx 会尝试下一个可用中转。
- 模型容量不足会按独立策略重试当前中转，不触发熔断，也不会自动切换到其他中转。
- HTTP 413 默认立即尝试备用中转，并在配置的跳过期内避开无法承载相同或更大请求体的线路。
- 400、401、403、404 等请求或配置错误直接返回，不做级联切换。
- 上游已经返回响应头并开始流式输出后，不重放当前请求，避免重复执行和重复计费。
- 连续失败阈值、熔断冷却、重试、健康探测和日志保留期限均可在配置中心调整；429 优先使用 `Retry-After`。

## 配置备份

中转管理支持导出和导入 `.aigateway` 加密备份。备份可包含中转 API Key、本地访问令牌和路由规则，导出密码不会保存到应用或日志中。跨电脑迁移时使用加密备份，不要复制数据库或导出明文密钥。
