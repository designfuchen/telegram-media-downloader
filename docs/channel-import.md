# 频道导入

## 从 Web 界面添加

“添加频道”接受数字 Chat ID、`@public_channel` 或 Telegram 链接。可以设置起始消息 ID、分组、优先级、过滤条件与是否启用。

批量使用“频道批量导入”：粘贴文本或选择 CSV，点击“解析并校验”，核对结果，再点击“确认入库”。默认暂停时，确认入库不会替你启动全部下载。

普通文本格式为：

```text
@public_channel,0,学习,normal
```

CSV 使用字段名：

```text
chat_id,start_message_id,group_name,priority,download_filter
@public_channel,0,学习,normal,
```

链接访问权限由自己的 Telegram 账号决定。私有频道请先在 Telegram 中手动加入，再校验；未加入的邀请链接可能只显示预览，无法取得可下载的数字 Chat ID。

## 提取与解析工具

以下操作应在已登录的下载器环境中执行。运行中的账户会话应先做只读备份，避免多个客户端同时写同一个 `.session` 文件。工具内部已有会话备份逻辑。

```sh
# 从某个频道消息里提取引用，保存为私有 CSV。
python scripts/extract_channel_chat_ids.py @source_channel --output output/channel_chat_ids

# 解析邀请链接；不会自动加入频道。
python scripts/resolve_channel_invites.py \
  --input output/channel_chat_ids.csv \
  --output output/resolved.csv

# 重新解析并将结果送入 Web 的导入批次，仍需在 UI 确认。
python scripts/sync_and_import.py --input output/resolved.csv
```

按实际提取结果选择输入文件；各工具的具体参数可通过 `--help` 查看。也可以直接把解析后的 CSV 上传到 Web 界面。

Docker 环境可使用 `docker compose exec downloader python scripts/<工具名>.py ...`。CSV 必须放到容器能访问的目录，例如挂载的 `downloads/`，并使用容器内路径 `/app/downloads/...`。

输出目录、CSV 和会话都被 Git 忽略。它们可能包含私人频道名称、Chat ID 或邀请链接，请不要作为 issue 附件直接上传。

## 固定批次手动加入流程

在控制台的“频道导入”页签中查看固定批次清单，逐个手动加入 Telegram，再点“我已手动加入，验证当前批次”。系统校验可访问频道并入库；失败可以重新验证或跳过。当前批次全部处理完后才允许进入下一批，下载完成需要自己确认。

首次准备批次 CSV，可使用 `order,invite_link,title,chat_id` 字段，将文件保存在私有目录，并在 `config.yaml` 中指定 `batch_csv_path`。Docker 默认读取 `/app/dbdata/channels.csv`。已有队列不会被清空，重复邀请链接会跳过。

通用 CSV 导入与手动加入批次保留各自的校验语义，共用控制台、账户和持久化数据库。旧 `/batch` 收藏链接会自动进入相同的导入页签。

## 可下载模板

导入窗口先选择“已加入的频道”或“待手动加入的邀请链接”，再点“下载 CSV 模板”。模板采用 UTF-8 BOM，便于 Excel 显示中文。仓库也提供 [频道模板](templates/channels.csv) 和 [邀请链接模板](templates/invites.csv)。

保留第一行，替换示例行。频道模板的 chat_id 可以填 @用户名、频道链接或数字 Chat ID；start_message_id=0 从起点开始，group_name 自定义分组，priority 为 normal/high/low，download_filter 可留空。邀请链接模板的 order 是排序，invite_link 填完整私有邀请链接，title 可填频道名称，chat_id 可留空。

邀请链接导入只添加到手动加入清单。随后在 Telegram 手动加入，再验证当前批次；不会自动加群，也不会清空已有导入进度。
