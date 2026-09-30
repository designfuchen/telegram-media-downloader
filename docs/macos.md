# 原生 macOS 应用

界面使用 SwiftUI 和 AppKit，下载引擎使用应用内置的 Python 运行时。用户不需要安装 Python、Docker 或打开终端。目标为 Apple Silicon、macOS 14 或更新版本；目前只在开发机系统实际运行验收，最低系统兼容性尚待独立验证。

## 安装与首次使用

打开 DMG，将 Telegram Downloader 拖到 Applications。打开应用后依次完成：

1. 通过引导中的 Telegram 官方链接申请 API ID / API Hash，保存凭证。
2. 选择直连，或粘贴节点链接、导入 YAML / JSON 并选择节点。保存后测试网络。
3. 在应用中输入手机号、验证码；开启两步验证的账号还需要密码。
4. 添加频道链接、用户名或 ID。批量导入页提供 CSV 模板和确认前的访问权限检查。
5. 使用系统文件夹选择器设置目录，点击开始下载。

首次启动为空配置，不包含任何开发者账号、节点、频道、会话或下载历史。默认下载目录是当前用户的 `~/Downloads/Telegram`。设置、数据库和登录会话放在 `~/Library/Application Support/Telegram Downloader Native`，与旧版网页应用独立。退出原生应用会通知引擎停止并保存进度。

## 构建

需要 Xcode Command Line Tools、Python 3.11 构建环境和对应架构的 sing-box。构建工具需要 PyInstaller 6.16.0；运行依赖见 requirements-lock.txt。

```sh
python scripts/build_native_macos.py --output /path/to/new-build-folder --sing-box /path/to/sing-box
```

脚本冻结 Python 引擎，编译 SwiftUI 与钥匙串组件，生成 `.app`、带 Applications 快捷入口的 DMG 和 SHA-256 摘要。版本统一读取 `utils/__init__.py`。每次使用新的输出目录，避免覆盖正在运行的应用。

## 认证与发布边界

原生界面通过回环地址连接内置引擎，使用短时、单次令牌换取会话。握手文件只记录端口，权限为 0600，不记录令牌。引擎仍保留认证，不能通过局域网直接访问。

省略证书参数时，构建使用 ad-hoc 签名，供本机验证。面向公众分发前应使用 Developer ID Application 正式签名、完成 Apple 公证并装订票据；另需尽可能在干净 Mac 上验收首次安装、真实账号登录和真实下载，并注明尚未验证的环境。Apple Development 证书不能代替 Developer ID Application。

原生界面当前覆盖首次引导、节点导入、登录、频道队列、暂停/开始、分页和 CSV 批量导入。旧网页控制台的逐文件历史、失败任务细分管理、频道高级筛选尚未完整迁移到原生界面；功能范围以实际界面为准。性能收益尚未基准测试，下载速度仍取决于 Telegram、节点和磁盘。

### 多账号与退出登录

在窗口右上角的账号菜单，或“设置 → 账号”中，可以添加另一个账号、切换账号或退出当前登录。切换前会暂停并保存当前下载进度。切回后需点击“开始下载”继续。

每个账号有独立的 Telegram 会话、频道库、任务数据库和本地控制台凭证。旧安装的数据保留在原目录；新账号保存在同一应用数据目录的 `accounts/<随机标识>` 子目录。新账号会继承本机 API、网络和下载偏好；会话、频道、任务和其他服务凭证不会复制。新账号默认使用独立输出子目录，也可以在保存位置中更换。

退出登录会通过 Telegram 撤销此应用在本机的会话，其他设备不受影响。文件和下载记录保留。再次使用此账号需要手机号、验证码和可能的两步验证密码。所有账号元信息仅存于本机私有目录，不包含在开源源码或应用安装包里。

## 凭证保护与升级

原生应用通过内置组件使用 macOS 钥匙串保存随机 256 位密钥。完整配置、账号初始化信息和 Telegram 会话文件使用 AES-GCM 认证加密；会话 SQLite 只在内存中解密。旧配置与旧会话首次打开时迁移，重启继续使用原有登录。读取不到钥匙串或密文校验失败时停止，不覆盖原文件，也不退回明文。

密钥与文件绑定当前安装路径；移动应用数据、换电脑或恢复备份前，必须同时保留钥匙串和原有路径。备份中旧版的明文副本不会被自动删除。验证码与两步验证密码不写入配置；日志屏蔽已注册的凭证和常见敏感格式。加密保护不代替设备密码、磁盘加密与对本机程序的信任。

## 正式签名、公证与 DMG

在 Apple Developer 账户中创建并安装 **Developer ID Application** 证书，使用 `xcrun notarytool store-credentials` 将公证凭证保存在钥匙串。不要将私钥、Apple 密码或 API key 放入源码。随后执行：

```sh
python scripts/build_native_macos.py \
  --output /path/to/fresh-release-folder \
  --sing-box /path/to/sing-box \
  --codesign-identity "Developer ID Application: YOUR_NAME (TEAM_ID)" \
  --notary-profile YOUR_KEYCHAIN_PROFILE \
  --release
```

`--release` 在缺少正式证书或公证配置时直接拒绝构建。脚本从内到外签名 Mach-O 与 framework，启用 hardened runtime，提交应用公证、装订票据并检查 Gatekeeper；然后创建 DMG、签名、公证并装订 DMG。只有全部成功才写入 `signed-and-notarized` 构建状态。Python 的原生回调需要允许动态可执行内存，见 `native/engine.entitlements`；没有放开 library validation。

也可把已签名应用的 macOS 归档加入 Xcode Organizer，使用已登录的 Apple 账户选择 Direct Distribution 进行公证并导出。发布验收应核对 `stapler validate`、`codesign --verify --deep --strict` 与 `spctl --assess` 的实际结果。不要将“创建证书成功”写成“公证成功”。

### 0.1.5 的实际分发方式

0.1.5 使用 Xcode Direct Distribution 公证并导出应用，应用票据已装订。分发附件为包含该应用的 DMG 和保留元数据的应用 ZIP；DMG 本身未单独签名或公证。应用票据、公证后的严格签名、系统分发预检查以及 DMG 挂载和拷贝后检查均通过。该方式与上面的 `--release` 全容器自动公证流程不同。Apple 的 [Xcode 分发演示](https://developer.apple.com/videos/play/wwdc2019/235/?time=2243) 说明了导出已公证应用后用 ZIP 或磁盘映像分发的方式；后续自动构建可使用上述全容器流程。
