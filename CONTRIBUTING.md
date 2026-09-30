# 一起改进 Telegram 下载器

欢迎修复问题、改进中文文案、优化原生交互和完善文档。小而清晰的改动更容易评审。

## 先确定要解决的问题

Bug 请使用反馈模板，说明操作步骤、预期与实际结果。较大的功能先开 Issue，讨论使用场景、界面和实现范围；避免写完后才发现方向不一致。

公开 Issue、截图、日志和 PR 中请勿放入手机号、API Hash、节点链接、Token、登录会话或私人频道内容。安全漏洞使用 [SECURITY.md](SECURITY.md) 中的私密报告方式。

## 准备开发环境

需要 Python 3.11；原生 SwiftUI 还需要 macOS 14+ 与 Xcode Command Line Tools。

```sh
git clone https://github.com/designfuchen/telegram-media-downloader.git
cd telegram-media-downloader
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements-lock.txt
```

运行程序前按 [配置文档](docs/configuration.md) 保存自己的加密密钥并初始化私有配置。测试使用临时数据与模拟客户端，无需填写真实 Telegram 凭证。

## 提交前检查

```sh
python -m unittest discover -s tests -t .
python -m py_compile media_downloader.py module/*.py utils/*.py scripts/*.py
python scripts/check_public_files.py
git diff --check
```

设置 `TMD_SINGBOX_BINARY` 后可运行真实 sing-box 配置校验；macOS 钥匙串测试需要构建辅助组件。未提供这些组件时，相关可选测试会跳过，应在 PR 中说明。原生修改需要编译并说明实际界面验证结果。

## 代码与体验约定

- Python 使用四空格缩进，沿用周围的命名与结构；Swift 沿用项目现有 SwiftUI / Observation 模式。
- 一次 PR 解决一组相关问题。错误提示说明发生了什么、接下来做什么；等待操作要显示状态，失败要能重试。
- 对用户统一使用“文件”“GIF 动图”“未登录”“开始下载”等现有概念；五步引导命名保持一致。
- 用有意义的回归测试验证缺陷，不为纯文案或间距修改添加镜像实现的测试。
- 保持默认本机监听、加密配置、权限检查和日志脱敏。不要将真实凭证写进示例、测试或构建产物。
- 修改行为时同步更新说明。公开配置只编辑 `config.example.yaml`；版本号统一来自 `utils/__init__.py`。

## 发起 Pull Request

从最新 `main` 建立分支，提交到自己的 Fork，再提出 PR。描述问题、改后的行为和验证方式；界面改动附使用示例数据的截图。说明未覆盖的情况与新增依赖许可证。

通过贡献，你同意自己有权提交这些内容，并按本项目 MIT 许可证提供项目代码；第三方内容须保留其原有许可与来源。感谢每一次认真改进。
