import AppKit
import SwiftUI

struct ImportsView: View {
    @Bindable var model: AppModel
    @State private var content = ""
    @State private var batches: [ImportBatch] = []
    @State private var details = ""
    @State private var showDetails = false

    var body: some View {
        VStack(alignment: .leading, spacing: 24) {
            Text("一次导入，慢慢收藏。").font(.largeTitle.bold())
            Text("粘贴多个频道，每行一个；也可以选择 CSV 文件。导入前会检查账号访问权限。")
                .foregroundStyle(.secondary)
            GroupBox {
                VStack(alignment: .leading, spacing: 16) {
                    TextEditor(text: $content).frame(height: 150)
                        .accessibilityLabel("频道列表或 CSV 内容")
                    HStack {
                        Button("选择 CSV…", systemImage: "doc") { Task { await chooseFile() } }
                        Button("下载 CSV 模板") { Task { await saveTemplate() } }
                        Spacer()
                        Button("检查并预览", systemImage: "checkmark.magnifyingglass") { Task { await createBatch() } }
                            .buttonStyle(.borderedProminent).disabled(content.isEmpty || !model.ready || model.busy)
                    }
                }.padding(16)
            }
            if !model.ready { Label("先在“开始使用”中登录 Telegram，才能检查频道。", systemImage: "info.circle").foregroundStyle(.secondary) }
            ForEach(batches) { batch in
                GroupBox {
                    VStack(alignment: .leading, spacing: 12) {
                        HStack {
                            Text(batch.name.isEmpty ? "频道导入 #\(batch.id)" : batch.name).font(.headline)
                            Spacer()
                            if batch.status == "imported" { Label("已导入", systemImage: "checkmark.circle") }
                            else { Text(batch.pending > 0 ? "正在检查" : "检查完成").foregroundStyle(.secondary) }
                        }
                        Text("共 \(batch.total) 条 · 可导入 \(batch.valid) · 待检查 \(batch.pending) · 无效 \(batch.invalid)")
                            .font(.callout).foregroundStyle(.secondary)
                        HStack {
                            Button("查看检查结果") { Task { await inspect(batch.id) } }
                            Spacer()
                            Button("确认导入") { Task { await confirm(batch.id) } }
                                .disabled(batch.pending > 0 || batch.valid == 0 || batch.status == "imported" || model.busy)
                        }
                    }.padding(12)
                }
            }
        }
        .sheet(isPresented: $showDetails) {
            VStack(alignment: .leading, spacing: 16) {
                Text("频道检查结果").font(.title2.bold())
                ScrollView { Text(details).textSelection(.enabled).frame(maxWidth: .infinity, alignment: .leading) }
                Button("完成") { showDetails = false }.keyboardShortcut(.defaultAction)
            }.padding(24).frame(width: 620, height: 420)
        }
        .task {
            while !Task.isCancelled {
                do {
                    try await refresh()
                    try await Task.sleep(for: .seconds(3))
                } catch is CancellationError { return }
                catch { model.error = error.localizedDescription; return }
            }
        }
    }

    private func refresh() async throws {
        let response = try await Engine.shared.request("/api/channel_imports")
        batches = (response["batches"] as? [[String: Any]] ?? []).map(ImportBatch.init)
    }
    private func createBatch() async {
        await model.perform {
            _ = try await Engine.shared.request("/api/channel_imports", body: ["content": content, "source_name": "本地频道导入"])
            content = ""
            try await refresh()
        }
    }
    private func confirm(_ id: Int) async {
        await model.perform {
            let response = try await Engine.shared.request("/api/channel_imports/\(id)/confirm", body: [:])
            model.message = response["message"] as? String ?? "已导入"
            try await refresh()
            try await model.refresh()
        }
    }
    private func inspect(_ id: Int) async {
        await model.perform {
            let response = try await Engine.shared.request("/api/channel_imports/\(id)?limit=500")
            details = (response["items"] as? [[String: Any]] ?? []).map { row in
                let channel = row["chat_id"] as? String ?? ""
                let status = row["status"] as? String ?? ""
                let reason = row["error"] as? String ?? ""
                return "\(channel) · \(status) \(reason)"
            }.joined(separator: "\n\n")
            if (response["count"] as? Int ?? 0) > 500 { details += "\n\n仅显示前 500 条。" }
            showDetails = true
        }
    }
    private func chooseFile() async {
        let panel = NSOpenPanel()
        panel.canChooseDirectories = false
        if await panel.begin() == .OK, let url = panel.url {
            await model.perform {
                let size = try url.resourceValues(forKeys: [.fileSizeKey]).fileSize ?? 0
                guard size <= 262144 else { throw CocoaError(.fileReadTooLarge) }
                content = try String(contentsOf: url, encoding: .utf8)
            }
        }
    }
    private func saveTemplate() async {
        let panel = NSSavePanel()
        panel.nameFieldStringValue = "频道导入模板.csv"
        if await panel.begin() == .OK, let url = panel.url {
            await model.perform {
                try "\u{FEFF}chat_id,start_message_id,group_name,priority,download_filter\n@replace_with_your_channel,0,学习,normal,\n".write(to: url, atomically: true, encoding: .utf8)
                model.message = "模板已保存。将示例频道替换为你自己的频道后导入。"
            }
        }
    }
}
