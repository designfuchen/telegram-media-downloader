# 隐私与安全

本仓库只发行经过清理的源码，源码 ZIP 不包含 Git 历史。运行配置、Telegram API 凭证、通知/隧道 token、代理密码、会话、数据库、真实频道清单、媒体、日志、备份和个人部署记录不随仓库发行。

`config.example.yaml` 仅含占位符。`scripts/init_config.py` 为每个实例生成独立的随机 Web 访问密码，不把密码打印到终端。请在本地配置中填写自己的 Telegram API 信息。

- 不要提交 `.session`、`config.yaml`、`.env`、代理分享链接或 rclone 配置。
- 不要把未经检查的数据库、日志、截图或 CSV 上传到 issue。
- Web 控制台可以控制下载并修改配置；默认仅本机可达，远程访问应使用强密码和 HTTPS。
- 已删除浏览器固定 AES 与自研 CBC / padding。远程 HTTP 请求被拒绝；同机回环 URL 可使用 HTTP，远程必须使用 TLS。默认不信任转发头，只有 `web_trusted_proxies` 指定的受控代理可以声明 HTTPS。
- 可选代理节点切换会操作独立的代理容器。默认发行配置没有挂载 Docker 套接字。

控制台拒绝空密码及少于 16 位的访问密码，拒绝带有跨站来源的写请求。会话签名密钥每次启动随机生成；请勿将访问密码当作 Telegram 验证码。

## 发布前检查

```sh
python scripts/check_public_files.py
gitleaks dir . --redact=100
gitleaks git . --redact=100
git ls-files
```

`.gitignore` 不能移除已经进入 Git 历史的密钥。若发生误提交，请先吊销或替换相关凭证，再清理历史。报告安全问题时只提供已脱敏的复现步骤。

## 本地凭证存储

原生 macOS 应用使用系统钥匙串密钥，对配置、账号 bootstrap 和完整 Telegram `.session` 做 AES-GCM 认证加密。会话不在磁盘生成明文 SQLite；临时节点配置通过标准输入传给 sing-box。缺钥匙串、错误密钥或密文损坏时保留原文件并停止。

源码 / Docker 启动必须设置 `TMD_SECRET_KEY`（32 字节随机密钥的 Base64），首启自动迁移现有配置。把密钥放在系统秘密管理器、受限环境文件或容器秘密管理设施中，与配置和会话分开备份。不要提交到 Git。丢失密钥后只能重新配置和重新登录，不能从安装包中恢复。运行目录与原路径也应保留。

配置与断点文件采用同目录临时文件、fsync、原子替换和 0600 权限；Docker 必须挂载整个私有配置目录，不可单独挂载一个配置文件。历史备份、之前生成的明文日志和旧副本不属于自动清理范围。

日志脱敏会屏蔽已加载配置中的敏感值及常见手机号、节点 URI、hash 和 token 格式；它不能保证识别所有第三方日志中的未知格式，因此反馈问题前仍要检查日志。

## 报告安全漏洞

请使用 GitHub 的 [私密漏洞报告入口](https://github.com/designfuchen/telegram-media-downloader/security/advisories/new)，提供受影响版本、脱敏的重现步骤与影响范围。不要在公开 Issue 中提交凭证、会话文件或可直接滥用的细节。我们会先确认问题，再协调修复与披露。

当前维护版本为 0.1.5；旧版本应更新到包含相应修复的版本。项目没有承诺未经验证的响应时限。
