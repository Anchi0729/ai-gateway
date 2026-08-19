# AI Gateway 开发与构建

本文面向需要运行源码、修改功能、执行测试或生成 Windows 发布包的开发者。

## 技术栈

| 层               | 技术                                                  |
| ---------------- | ----------------------------------------------------- |
| 桌面框架         | Tauri 2                                               |
| 后端             | Rust 2021                                             |
| 异步运行时       | Tokio                                                 |
| 本地 HTTP 服务   | Axum                                                  |
| 上游 HTTP 客户端 | Reqwest + rustls                                      |
| 前端             | React 18 + TypeScript + Vite                          |
| 数据库           | SQLite + rusqlite bundled                             |
| Codex 配置       | `toml_edit`                                           |
| API Key 存储     | Windows Credential Manager + `keyring` windows-native |
| 备份加密         | Argon2id + XChaCha20-Poly1305                         |

## 开发环境

推荐环境：

- Windows 10/11 x64。
- Node.js 20 或更高版本。
- npm。
- Rust stable。
- Microsoft Edge WebView2 Runtime。
- Visual Studio Build Tools，包含 Desktop development with C++，用于标准 MSVC 开发链路。

项目正式发布历史也使用 GNU/MSVCRT 工具链。需要复现该发布链路时，还需安装：

- `stable-x86_64-pc-windows-gnu` Rust toolchain。
- `x86_64-pc-windows-gnu` target。
- 与目标匹配的 MinGW/WinLibs UCRT 工具链，并把其 `bin` 目录加入当前终端 `PATH`。

安装 Rust 目标示例：

```powershell
rustup toolchain install stable-x86_64-pc-windows-gnu
rustup target add x86_64-pc-windows-gnu --toolchain stable-x86_64-pc-windows-gnu
```

## 安装依赖

在仓库根目录执行：

```powershell
npm install
```

前端依赖由 `package-lock.json` 锁定，Rust 依赖由 `src-tauri/Cargo.lock` 锁定。非必要不要批量升级依赖。

## 本地运行

启动完整 Tauri 开发版：

```powershell
npm run tauri dev
```

只启动前端 Vite：

```powershell
npm run dev
```

只启动前端时，应用会使用仓库现有的 Demo 数据或无法调用 Tauri Command，适合布局检查，不代表真实网关功能已经运行。

默认端口是 `8765`。如果用户正在运行正式 AI Gateway，不要启动第二个使用同一应用数据和端口的实例。开发测试应使用隔离应用标识、隔离数据目录和临时端口，测试结束后只停止临时实例。

## 前端构建

```powershell
npm run build
```

该命令依次执行 TypeScript 检查和 Vite 生产构建，输出写入已忽略的 `dist/`。

## Rust 格式、检查和测试

使用当前默认 Rust 工具链：

```powershell
cargo fmt --manifest-path src-tauri/Cargo.toml --check
cargo check --manifest-path src-tauri/Cargo.toml --all-targets
cargo test --manifest-path src-tauri/Cargo.toml --lib --no-run
```

实际运行测试：

```powershell
cargo test --manifest-path src-tauri/Cargo.toml --lib
```

`--no-run` 只证明测试代码成功编译成可执行文件，不证明断言已经执行。发布或评审报告必须区分“测试目标编译通过”和“测试运行通过”。

### GNU 工具链检查

如果默认工具链是 MSVC，但本机没有 `link.exe`，可以明确使用完整 GNU 主机工具链：

```powershell
$env:PATH = "<WinLibs或MinGW的bin目录>;" + $env:PATH
cargo +stable-x86_64-pc-windows-gnu check --manifest-path src-tauri/Cargo.toml --all-targets
cargo +stable-x86_64-pc-windows-gnu test --manifest-path src-tauri/Cargo.toml --lib --no-run
```

只添加 `--target x86_64-pc-windows-gnu` 不一定足够：Cargo 的构建脚本仍可能由默认 MSVC 主机工具链编译并寻找 `link.exe`。需要同时选择 GNU toolchain。

### `STATUS_ENTRYPOINT_NOT_FOUND`

某些 Windows GNU/WebView2 环境中，Cargo 生成的测试运行器可能在进入任何测试前返回：

```text
0xC0000139 STATUS_ENTRYPOINT_NOT_FOUND
```

这表示 Windows 加载测试 EXE 的某个 DLL 时找不到预期函数入口，通常与测试目录的 DLL 搜索顺序或 GNU/WebView2 运行库版本不匹配有关。它和正式 `AI Gateway.exe` 是不同的可执行文件，因此正式程序能启动并不矛盾。

排查顺序：

1. 确认运行的是预期 toolchain 生成的测试 EXE。
2. 检查 `PATH` 中是否混入其他 MinGW、WebView2Loader 或系统兼容 DLL。
3. 尝试标准 MSVC 工具链及 Visual Studio Developer PowerShell。
4. 使用依赖检查工具确认缺失入口所在 DLL。
5. 不要通过复制来源不明的 DLL 到系统目录解决问题。

在问题解决前可以报告测试目标编译结果，但不能把应用冒烟测试替代为完整单元测试。

## 网关测试原则

网关集成测试应使用绑定到 `127.0.0.1:0` 的 Mock 上游，由系统分配临时端口。重点覆盖：

- 408、429、5xx 和响应头超时。
- 模型、分组、容量与账号并发错误只重试原中转，且不累计线路熔断。
- 自动切换关闭时，即使当前中转处于 `open` 也会真实访问当前线路，备用中转调用次数为零。
- 自动切换开启时，线路级 `open` 中转被跳过，冷却到期后由真实请求完成半开试探。
- 上游 413 和本地 200 MiB 限制的区别。
- Responses SSE 分帧、合法终态、提前 EOF 和底层断流。
- 输出前可重试、输出后不重放。
- 请求及响应头过滤。
- usage 采集、去重和持久化。

不要使用真实 API Key、正式 SQLite、Credential Manager 中的正式条目或真实 `auth.json`。Windows Credential Manager 集成测试需要使用唯一测试 ID，并在结束后删除测试凭据。

## 正式 Windows 发布

发布前统一更新：

```text
package.json
package-lock.json
src-tauri/Cargo.toml
src-tauri/Cargo.lock
src-tauri/tauri.conf.json
界面版本文字
packaging/build-portable.ps1 默认版本
packaging/便携版说明.txt
CHANGELOG.md
```

当前正式构建使用显式 GNU 主机工具链、GNU target 和独立 `msvcrt-build` 目录。示例：

```powershell
$env:PATH = "<WinLibs或MinGW的bin目录>;" + $env:PATH
$env:RUSTUP_TOOLCHAIN = "stable-x86_64-pc-windows-gnu"
$env:CARGO_TARGET_DIR = Join-Path (Resolve-Path "src-tauri") "target/msvcrt-build"
npm run tauri -- build --target x86_64-pc-windows-gnu --bundles nsis
```

实际命令应在仓库根目录执行。目标目录必须使用绝对路径，避免 Tauri 切换工作目录后生成重复的 `src-tauri/src-tauri/target`。构建完成后，NSIS 产物位于类似目录：

```text
src-tauri/target/msvcrt-build/x86_64-pc-windows-gnu/release/bundle/nsis/
```

便携版必须使用同一次 Release 的主程序和 `WebView2Loader.dll`：

```powershell
powershell -ExecutionPolicy Bypass -File packaging/build-portable.ps1 -Version 0.3.10
```

脚本从以下目录读取文件：

```text
src-tauri/target/msvcrt-build/x86_64-pc-windows-gnu/release/
```

不要误用 `src-tauri/target/release/ai-gateway.exe` 中由其他工具链或旧构建产生的二进制。

## 发布验收

发布目录应包含：

```text
AI Gateway_<版本>_x64-setup.exe
AI Gateway_<版本>_portable_x64.zip
```

便携 ZIP 固定包含：

```text
AI Gateway.exe
WebView2Loader.dll
便携版说明.txt
```

验收内容：

- 安装包、便携 EXE、Cargo、npm 和 Tauri 配置版本一致。
- 便携 ZIP 内 EXE 与同一次 Release EXE 的 SHA-256 一致。
- EXE 使用 Windows GUI subsystem，不弹出控制台窗口。
- 包含预期的 Windows Credential API 和 WebView2 依赖。
- 计算安装包和 ZIP 的 SHA-256。
- `git diff --check` 通过。
- 未把发布包、数据库、日志、备份或用户配置纳入 Git。
- 构建过程不启动应用，不占用用户正在使用的 `8765`。

确需桌面冒烟测试时，使用隔离标识、数据库和临时端口。测试完成后还原正式 `identifier`，重新构建正式产物，并确认没有遗留临时进程或配置。
