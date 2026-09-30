import SwiftUI

struct ChannelPreviewView: View {
    let channel: Channel
    let savePath: String
    @State private var previews: [Data] = []
    @State private var loading = false

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            if previews.isEmpty {
                HStack(spacing: 10) {
                    Image(systemName: "photo.on.rectangle.angled").font(.title2)
                    Text(loading ? "正在生成本地预览…" : "暂无图片或视频预览")
                        .font(.caption)
                }.foregroundStyle(.secondary).frame(maxWidth: .infinity, minHeight: 72)
                    .background(.quaternary.opacity(0.3), in: .rect(cornerRadius: 10))
            } else {
                HStack(spacing: 10) {
                    ForEach(previews.indices, id: \.self) { index in
                        if let image = NSImage(data: previews[index]) {
                            Image(nsImage: image).resizable().scaledToFill()
                                .frame(width: 150, height: 95).clipped().clipShape(.rect(cornerRadius: 10))
                                .accessibilityLabel("该频道已下载内容的本地缩略图")
                        }
                    }
                    Spacer(minLength: 0)
                }
                Text("本地已下载内容 · 视频显示静态首帧").font(.caption).foregroundStyle(.secondary)
            }
        }
        .task(id: "\(channel.id)-\(channel.completed == 0 ? -1 : channel.completed / 5)-\(savePath)") {
            await load()
        }
    }

    private func load() async {
        guard channel.completed > 0 else { return }
        loading = true
        defer { loading = false }
        do {
            var query = URLComponents()
            query.path = "/api/channel_files"
            query.queryItems = [URLQueryItem(name: "chat_id", value: channel.id),
                                URLQueryItem(name: "status", value: "completed"),
                                URLQueryItem(name: "limit", value: "30")]
            guard let path = query.string else { return }
            let response = try await Engine.shared.request(path)
            let files = response["data"] as? [[String: Any]] ?? []
            var next: [Data] = []
            for file in files {
                try Task.checkCancellation()
                if let path = file["save_path"] as? String,
                   let data = await LocalThumbnail.shared.data(path: path, root: savePath) {
                    next.append(data)
                }
                if next.count == 3 { break }
            }
            if !Task.isCancelled { previews = next }
        } catch { /* A missing preview must not interrupt downloads. */ }
    }
}
