# Telegram 下载器：品牌与界面文案

产品名称为 **Telegram 下载器**。

## 最终图标

文件：`module/static/brand/telegram-downloader-icon.svg`。1024 × 1024 画布，纯矢量；单色蓝 `#2874F0`，白色聊天气泡，下载箭头和托盘线以蓝色镂空表达。圆角方形，保留清楚的外轮廓；无字母、文字、小锁徽标、渐变、阴影或纹理。

用户在查看生成图后要求更简约，因此最终交付采用手写 SVG；Image 生成的探索版本保留在仓库之外，没有作为发行素材。侧栏、登录页、空页面和浏览器图标都引用同一个本地图标，没有外部图片请求。

## 文案原则

主文案：**把 Telegram 内容，收进自己的空间。**

辅助文案：**连接账号，选择保存位置，添加你关心的频道。图片、视频和文件，按你的规则归档。**

| 场景 | 最终用语与意图 |
| --- | --- |
| 品牌短句 | 好内容，留在自己手里。 |
| 配置按钮 | 保存设置；明确保存行为，不暗示立即启动下载 |
| 初次配置 | 账号信息 → 网络连接 → 保存位置 → 添加频道 |
| 空队列 | 第一份归档，从一个频道开始。提供添加、批量导入两种入口 |
| 下载空页面 | 现在没有文件在下载；提供查看频道队列入口 |
| 历史空页面 | 你的归档记录，会出现在这里。说明完成后能查看哪些信息 |
| 异常空页面 | 目前没有需要处理的下载；说明重试达到上限的文件会保留 |
| 批量添加 | 先预览，再确认。重复项和无法访问的频道会单独提示 |
| 文件夹检测 | 检查文件夹；按钮附近显示实际可写结果 |

不把网络检测写成 Telegram 登录成功，不把加入频道写成下载完成，不承诺绕过 Telegram 的权限限制，不使用“全球最好”等无依据的产品声明。

## 本轮交互

- API Hash 和节点导入内容可以切换显示；切换页面后自动恢复隐藏，不读取服务端已保存的密钥原值。节点识别后折叠原始内容，把操作集中到节点选择。
- 配置步骤点击后定位字段并交给键盘焦点，保留减少动态效果偏好。
- 空页面提供下一步；没有异常任务时禁用“重试全部”。筛选无结果和首次空队列使用不同说明。
- 清除顶部剩余的并发调度实现术语，保留实际速度与时间更新。

本地虚构数据演示已经验证密钥显示、切页隐藏、空队列入口和添加对话框。116 项 Python 回归测试通过；这不代表真实 Telegram 登录与传输已验收。

## Image 生成探索提示词

使用内置 Image 工具生成探索图，最终 SVG 按后续简约要求重新设计。原始提示词如下：

```text
App icon for a download utility called "Telegram Downloader". A rounded chat bubble in white, with a bold downward arrow in the center landing on a tray line, and a small padlock badge at the bottom-right corner to suggest private content. Flat design with subtle soft gradients, cyan-to-indigo gradient background, rounded-square iOS icon shape, clean edges, high contrast, minimal and modern, legible at small sizes. Centered composition with balanced padding, no text, no watermark, 1024x1024.
```

本轮在实际小屏浏览区域复查六个页面的 203 个可见文字对比度候选，未发现低于相应门槛的候选。该检查不涵盖全部状态或透明叠加，不等同于完整 WCAG 认证。
