# 安全与隐私说明

AI Gateway 是本地网关，但第三方模式下请求会被发送到用户配置的中转服务。使用前请确认你信任这些服务商，并理解服务商可能看到请求内容和响应内容。

## 安全边界

- 默认只监听 `127.0.0.1`，不直接对局域网开放。
- 客户端必须提供本地访问令牌；令牌验证发生在读取请求体之前。
- 请求体超过固定 `200 MiB` 上限时由本地网关拒绝。
- 第三方 API Key 使用 Windows Credential Manager 保存。
- 转发前删除官方 Authorization、Cookie、账号、组织、项目和 `X-OpenAI-*` 身份头。
- `X-AI-Gateway-Token` 只用于本地鉴权，不转发给任何上游。
- 普通日志和诊断详情不保存 prompt、响应正文、API Key、Cookie 或完整请求头。
- 加密备份使用密码保护；密码不会保存到应用或日志。

## Codex 登录保护

AI Gateway 会只读解析 Codex 的 `auth.json`，仅判断是否存在可识别的本地登录凭据。应用不会解码、展示、记录、修改、删除、复制或备份其中的 Token 和账号信息，也不会联网验证凭据是否有效。官方登录仍由 Codex 自己维护；第三方模式只管理 `config.toml` 中的 `ai_gateway` Provider 配置，不把官方身份凭据转发给第三方中转。外部工具创建的 `custom` Provider 会保留。

如果使用 CC-Switch 或其他配置工具，请避免同时接管同一个 `ai_gateway` 或 `custom` Provider，防止配置互相覆盖。AI Gateway 不会因为发现外部 `custom` 就自动覆盖它。

## 数据处理

AI Gateway 不包含遥测或自动云同步。数据默认保存在本机：

- 中转元数据、路由配置、运行日志和用量统计：应用 SQLite。
- 中转 API Key：Windows Credential Manager。
- Codex 官方登录和会话：由 Codex 保存在其自己的目录；AI Gateway 只读判断登录凭据是否存在，并读取 Token 统计所需的结构化会话元数据。

官方用量同步只解析 `session_meta`、`turn_context` 和 `event_msg.token_count`，不保存聊天正文。用量统计来自 Codex 本地记录或上游明确返回的 usage，不等同于服务商账单。

## 不要提交的文件

以下文件可能包含敏感数据，禁止提交到代码仓库、Issue 或公开附件：

```text
auth.json
*.sqlite3
*.aigateway
运行日志
API Key
本地访问令牌
包含请求内容的调试输出
```

加密备份虽然经过加密，也应按照敏感文件保管，不要公开上传。

## 漏洞报告

请不要在公开 Issue 中发布可利用的安全漏洞、真实密钥或完整日志。建议先通过项目维护者提供的私下联系方式报告，并包含：

- 影响版本。
- Windows 版本和安装形式。
- 可复现步骤或最小复现样例。
- 影响范围和可能的缓解方式。

报告中请脱敏所有 API Key、Token、Cookie、`auth.json` 内容、数据库和请求正文。

## 免责声明

本项目是独立开源项目，与 OpenAI、Codex、CC-Switch 及第三方中转服务商不存在官方隶属、授权或背书关系。用户需要自行评估第三方中转服务的安全性、可靠性和合规性。
