# 贡献指南

感谢参与 AI Gateway。提交代码前请先阅读本文和[开发与构建](docs/development.md)，并确保改动不会泄露用户凭据、Codex 登录信息或请求内容。

## 开始开发

1. Fork 或克隆仓库。
2. 从最新主分支创建用途明确的分支。
3. 安装 Node.js、Rust、Windows C++ 构建工具和 WebView2 Runtime。
4. 执行 `npm install` 安装前端依赖。
5. 使用 `npm run tauri dev` 启动开发版。

不要使用真实 API Key、正式数据库或用户 `auth.json` 进行测试。需要模拟上游时，请使用临时端口和 Mock 服务。

## 修改原则

- 代码可读性优先于追求最少行数。
- 优先沿用现有模块、类型和 UI 组件，避免为单一场景增加不必要抽象。
- 核心业务逻辑、复杂条件、SQL 和兼容处理需要添加简洁且有意义的注释。
- 不写“给变量赋值”一类无信息量注释。
- 不手工修改生成代码；契约、类型和业务接入应保持一致。
- 修改共享路由、SSE、数据库或 Codex 配置行为时，应补充相应测试。
- 不得读取、修改或记录 Codex `auth.json` 内容。
- 不得在日志中加入 prompt、响应正文、API Key、Cookie 或完整鉴权头。

## 提交规范

Git Commit 使用 Conventional Commits：

```text
<type>(<scope>): <subject>
```

允许的 `type`：

```text
feat | fix | docs | style | refactor | perf | test | chore | build
```

要求：

- `scope` 使用实际模块或领域，例如 `gateway`、`codex`、`usage`、`backup`、`docs`。
- `subject` 不超过 72 个字符，以动词开头，不加句号。
- 一个提交只表达一种变更意图。

示例：

```text
fix(gateway): 修复输出前 SSE 错误未重试
docs(project): 补充开发和排障说明
test(usage): 增加官方用量去重测试
```

## 提交前检查

在仓库根目录执行：

```powershell
npm run build
cargo fmt --manifest-path src-tauri/Cargo.toml --check
cargo check --manifest-path src-tauri/Cargo.toml --all-targets
cargo test --manifest-path src-tauri/Cargo.toml --lib --no-run
git diff --check
```

如果本机环境能够正常运行 Rust 测试，再执行：

```powershell
cargo test --manifest-path src-tauri/Cargo.toml --lib
```

若测试运行器在 DLL 加载阶段返回 `STATUS_ENTRYPOINT_NOT_FOUND`，请在提交说明中明确写出“测试目标编译通过、测试运行器未启动”，不能声称测试全部运行通过。详细说明见[开发与构建](docs/development.md)。

## 前端要求

- 保持现有设计语言、字体、颜色和焦点状态。
- 使用现有 Lucide 图标，不手绘重复图标。
- 控件在常规窗口和最小窗口下不能重叠或溢出。
- 表单修改必须接入现有 dirty、保存和失败反馈流程。
- 新增用户界面时要覆盖空状态、加载状态、错误状态和禁用状态。

## Pull Request 或代码审查

提交评审时请说明：

- 解决的问题和用户可见行为。
- 关键设计选择和兼容性影响。
- 执行过的检查和测试。
- 未能执行的测试及原因。
- 数据库、备份格式、Codex 配置或安全边界是否变化。
- 界面改动的脱敏截图。

评审优先关注行为回归、数据安全、错误处理和测试缺口。不要在截图或日志中暴露任何真实凭据。

## 禁止提交的内容

```text
node_modules/
dist/
release/
src-tauri/target/
*.sqlite3
*.aigateway
*.log
*.exe
*.msi
*.zip
.env*
auth.json
用户截图和本地 QA 产物
```

提交前应检查 `git status`，确认没有数据库、日志、备份、发布包或本机配置进入暂存区。
