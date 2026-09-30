# 配置与部署

`config.example.yaml` 是公开模板。`scripts/init_config.py` 据此生成权限为 `0600` 的私有 `config.yaml`，并生成随机 Web 访问密码。已存在的配置不会被覆盖。

| 配置 | 说明 |
| --- | --- |
| `api_id` / `api_hash` | 自己申请的 Telegram 应用凭证，启动前填写 |
| `web_login_secret` | Web 访问密码，不要留空或使用模板占位符 |
| `web_host` / `web_port` | 本机默认 `127.0.0.1:5002`；容器内使用 `0.0.0.0` |
| `save_path` | 下载目标；NAS 目录应提前正确挂载 |
| `start_paused` | 默认 `true`，确认导入与存储后再开始 |
| `max_download_task` | 同时下载文件数，模板从 5 开始 |
| `media_session_pool_size` | 媒体连接池；从模板默认值开始 |
| `nas_require_mount` | 需要真实共享挂载时开启，防止共享断开后写到本机 |
| `remote_access` | 可选 Cloudflare Tunnel，默认关闭 |
| `wxpusher` | 可选通知，默认关闭且没有 token / 用户 ID |

数据库位置可通过 `TMD_TASK_DB` 和 `TMD_HISTORY_DB` 指定。Compose 使用 `/app/dbdata/download_history.db`，目录单独持久化。

可选普通代理配置：

```yaml
proxy:
  scheme: socks5
  hostname: proxy.example.invalid
  port: 1080
  username: ""
  password: ""
```

启用公网访问前设置独立的强密码并使用 HTTPS 或可信私网。登录页旧版 AES 常量随前端公开，只用于协议兼容，不提供传输保密性，也不是用户的账户密钥。

rclone 上传需要安装 rclone 并填写自己的 `upload_drive`；本镜像包含 rclone。Cloudflare Tunnel 需要另行安装 `cloudflared` 或自行配置隧道容器。

## 极空间部署提醒

- 在极空间 Docker 管理器中使用本仓库构建的镜像或 Compose；不要用旧部署目录直接覆盖正在运行的实例。
- 每个实例使用独立配置、会话和数据库目录。
- 首次交互登录后再改成后台运行。
- 暂停传输、备份私有配置和数据库后再升级；保留旧镜像以便回滚。
- 普通升级不要重置数据库、删除下载文件或 `.temp` 断点。

## 首次运行：用四步配置

生成私有配置后可以直接启动。未填写有效 API 信息时，程序先开启 Web 控制台并等待，在“配置”页填完 API ID 和 API Hash 并保存后，回到终端输入 Telegram 登录验证码。API ID 是应用编号，频道 Chat ID 是频道编号，两者分别填写在账号设置和频道添加入口。

网络连接有三种方式：

- 直接连接：当前网络能访问 Telegram。
- 导入节点：统一入口可粘贴单节点链接、每行一个的链接列表、Base64 链接列表，或上传 Clash / Mihomo 格式的 YAML / JSON（也接受单节点对象或节点数组）。最多 256 KB、500 个节点。自动识别 SS、VLESS、VMess、Trojan、Hysteria2（hy2）、TUIC、SOCKS5、HTTP / HTTPS；选择一个节点，再测试并保存。
- SOCKS5 / HTTP：填写已有代理的地址与端口。代理在电脑、下载器在 NAS 时，应填写电脑局域网 IP 并允许代理客户端的局域网访问；127.0.0.1 指下载器所在机器。

“测试网络”只检查当前填写的候选设置，不保存配置，不切换运行中的下载连接。测试使用 Telegram HTTPS 端点，可访问该端点不等于 Telegram 账号登录成功；登录验证仍由 Telegram 客户端完成。账号或代理修改在重启下载进程后生效。

节点密码、VLESS UUID 和 REALITY 参数保存在认证加密的私有配置中；临时 sing-box 配置只通过标准输入传递，不写临时文件；Web 响应只返回节点名称与协议，不返回完整节点配置或已保存的凭据。内部 `ss_proxy` 配置键继续保留以兼容旧配置，现可存储上述各类节点。技术格式见 [Shadowsocks](https://sing-box.sagernet.org/configuration/outbound/shadowsocks/)、[VLESS](https://sing-box.sagernet.org/configuration/outbound/vless/)、[TLS / REALITY](https://sing-box.sagernet.org/configuration/shared/tls/) 和 [分享链接提案](https://github.com/XTLS/Xray-core/discussions/716)。

### 找不到 SS 链接？

先看代理客户端里节点的“类型”。类型是 VLESS 时，分享链接通常以 `vless://` 开头，不需要转换成 SS。在客户端的节点列表找到该节点，打开分享菜单并复制完整链接；然后在下载器选择“导入节点”，粘贴、测试、保存。截图、服务器地址和 UUID 单独都不能代替完整分享链接。链接包含连接凭据，应只粘贴到自己的本地或私有部署。

当前实现已用虚构的 VLESS TLS / REALITY 配置通过实际 sing-box 1.14.2 的配置检查、本地 SOCKS 启动与端口释放测试。没有使用用户真实节点完成握手；实际可用性仍需通过本地“测试网络”和 Telegram 登录验证。

## 统一节点导入

页面导入框下方提供“这个统一入口支持哪些格式？”说明，不需要用户先判断自己的格式。上传文件后自动读取；粘贴内容后点击“读取并识别”。只有一个兼容节点时自动选中；有多个时必须选择。部分节点不兼容时，保留可选节点并逐项解释失败原因。修改原始内容后，旧识别结果失效，需重新读取。

| 协议 | 分享链接 | 支持范围 |
| --- | --- | --- |
| Shadowsocks | `ss://` | SIP002 / 旧版 Base64；现有加密方式；不含额外插件 |
| VLESS | `vless://` | TCP / Vision + TLS / REALITY；普通 TLS 支持 WebSocket / gRPC |
| VMess | `vmess://` | 常见 Base64 JSON 分享；TCP / WebSocket / gRPC；TLS 可选 |
| Trojan | `trojan://` | TLS；TCP / WebSocket / gRPC |
| Hysteria2 | `hysteria2://` / `hy2://` | TLS、固定端口、可选 salamander 混淆；省略端口时为 443 |
| TUIC | `tuic://` | UUID + 密码、TLS、常见拥塞控制和 UDP 转发参数 |
| SOCKS5 | `socks5://` / `socks://` | 可选用户名和密码；普通 SOCKS5 |
| HTTP 代理 | `http://` / `https://` | 必须带明确端口；可选用户名和密码；HTTPS 保留 TLS |

YAML / JSON 使用 Clash / Mihomo 的 `proxies` 节点结构，不是任意应用的配置格式。小火箭原生 `.conf`、sing-box 完整 JSON、SSR、旧 Hysteria、SS 插件、XHTTP、VLESS Encryption、自定义证书 / ECH / 代理链 / 多路复用等附加选项目前不转换，可通过已有客户端提供的 SOCKS5 使用。不静默丢弃非空未知参数，不跳过证书校验。YAML 不接受锚点引用、重复字段、多文档或自定义对象标签，提供明确错误而不回显原始配置。

只抽取节点，忽略文件的顶层 DNS、规则和代理组；不请求 `proxy-providers` 或订阅 URL。导入预览不联网、不写配置。服务端预览仅返回名称、协议、序号和兼容状态，不返回密码、UUID、REALITY 公钥或原始节点对象。保存和测试时重新解析原始内容并验证所选序号；保存只保留选中节点，原文件不落盘。页面默认遮盖内容，识别完成后折叠输入区，离开配置页重新隐藏。

上述八类协议及 WebSocket / gRPC 使用虚构配置经过实际 sing-box 1.14.2 配置检查、本地 SOCKS 启动和退出后端口释放测试。这验证格式转换和运行组件，不代表实际远端握手成功；真实节点请通过“测试当前填写的网络”并完成 Telegram 登录验证。

格式参考：[Clash / Mihomo VLESS](https://github.com/MetaCubeX/Meta-Docs/blob/main/docs/config/proxies/vless.en.md)、[Shadowsocks](https://github.com/MetaCubeX/Meta-Docs/blob/main/docs/config/proxies/ss.en.md)、[sing-box VMess](https://sing-box.sagernet.org/configuration/outbound/vmess/)、[Hysteria2](https://sing-box.sagernet.org/configuration/outbound/hysteria2/)、[TUIC](https://sing-box.sagernet.org/configuration/outbound/tuic/)、[V2Ray transport](https://sing-box.sagernet.org/configuration/shared/v2ray-transport/)。

## 源码与 Docker 的加密密钥

打包 Mac 应用自动使用钥匙串，无需用户创建密钥。开发者运行源码 / Docker 时，必须在运行环境中配置 `TMD_SECRET_KEY`。值为随机 32 字节的 Base64；例如在私人终端中创建到当前环境：

```sh
export TMD_SECRET_KEY="$(python3 -c 'import base64,secrets; print(base64.b64encode(secrets.token_bytes(32)).decode())')"
```

首次生成后立即保存到密码管理器；后续启动继续使用同一个值，不能每次生成新的值。不要把密钥和加密配置一起公开。服务无法读取密钥时会拒绝启动。网页访问密码在加密前生成的本地配置里；迁移后保留在密码管理器，或从 Mac 应用的自动入口访问，不应公开输出解密后的配置。

Docker 使用 `python3 scripts/init_config.py --docker --directory ./private-config` 初始化目录，并将整个 `./private-config` 挂到 `/app/config`。旧安装先停止服务、备份，再把旧配置和 data.yaml 移入该目录；绝不能用空模板覆盖原配置。需要远程访问时，配置 HTTPS 反向代理，`web_trusted_proxies` 仅填写代理实际连接的 IP。容器内监听 `0.0.0.0` 仅用于端口映射，公开默认端口仍限制本机；局域网明文 HTTP 也会被拒绝。
