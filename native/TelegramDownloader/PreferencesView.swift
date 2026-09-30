import SwiftUI

/// Day-to-day preferences use task names; onboarding remains a separate guide.
struct PreferencesView: View {
    @Bindable var model: AppModel
    @Environment(\.accessibilityReduceMotion) private var reduceMotion
    private let sections = ["下载内容", "文件整理", "保存位置", "网络", "账号"]

    var body: some View {
        VStack(alignment: .leading, spacing: InterfaceStyle.sectionSpacing) {
            PageHeading(title: "设置", subtitle: "选择下载什么、保存在哪里，或管理你的账号。")
            Picker("设置分类", selection: $model.settingsSection) {
                ForEach(sections, id: \.self) { Text($0).tag($0) }
            }.pickerStyle(.segmented).controlSize(.large).labelsHidden()
            VStack(alignment: .leading, spacing: InterfaceStyle.sectionSpacing) {
                switch model.settingsSection {
                case "文件整理": organization
                case "保存位置": storage
                case "网络":
                    SettingsCard("网络连接", subtitle: "选择连接方式，保存后测试能否访问 Telegram。") {
                        SetupView(model: model, isSettings: true).network
                    }
                case "账号": AccountView(model: model)
                default: downloads
                }
            }.id(model.settingsSection).transition(.opacity)
            if model.busy {
                HStack(spacing: 10) { ProgressView().controlSize(.small); Text(model.busyMessage).foregroundStyle(.secondary) }
            }
        }.disabled(model.busy)
            .animation(reduceMotion ? nil : .easeInOut(duration: 0.18), value: model.settingsSection)
    }

    private var downloads: some View {
        VStack(alignment: .leading, spacing: InterfaceStyle.sectionSpacing) {
            SettingsCard("下载哪些内容", subtitle: "按需勾选。默认保留图片、视频、音频和文件。") {
                LazyVGrid(columns: [GridItem(.adaptive(minimum: 250), spacing: 12)], spacing: 12) {
                    MediaChoice(title: "图片", detail: "照片与频道图片", symbol: "photo", selected: $model.downloadPhotos)
                    MediaChoice(title: "视频", detail: "频道发布的视频", symbol: "play.rectangle", selected: $model.downloadVideos)
                    MediaChoice(title: "音频", detail: "音乐与音频文件", symbol: "waveform", selected: $model.downloadAudio)
                    MediaChoice(title: "文件", detail: "文档、压缩包等附件", symbol: "doc", selected: $model.downloadDocuments)
                    MediaChoice(title: "语音消息", detail: "聊天中的语音录音", symbol: "mic", selected: $model.downloadVoice)
                    MediaChoice(title: "圆形视频", detail: "Telegram 视频消息", symbol: "video.circle", selected: $model.downloadVideoNotes)
                    MediaChoice(title: "GIF 动图", detail: "GIF 与动态图片", symbol: "sparkles.rectangle.stack", selected: $model.downloadAnimations)
                    MediaChoice(title: "文字", detail: "保存纯文字消息，可单独选择", symbol: "text.alignleft", selected: $model.downloadText)
                }
                Text("更改会用于后续扫描。已有文件和已排队任务会保留。")
                    .font(.callout).foregroundStyle(.secondary)
                saveButton
            }
            SettingsCard("更多下载选项") {
                DisclosureGroup("筛选文件格式") {
                    VStack(alignment: .leading, spacing: 14) {
                        Text("all 表示全部；多个扩展名用英文逗号分隔，例如 mp4,mkv。")
                            .font(.callout).foregroundStyle(.secondary)
                        LabeledContent("视频格式") { TextField("all 或 mp4,mkv", text: $model.videoFormats) }
                        LabeledContent("音频格式") { TextField("all 或 mp3,flac", text: $model.audioFormats) }
                        LabeledContent("文件格式") { TextField("all 或 pdf,zip", text: $model.documentFormats) }
                    }.textFieldStyle(.roundedBorder).padding(.top, 12)
                }
                DisclosureGroup("同时下载数量") {
                    VStack(alignment: .leading, spacing: 12) {
                        Stepper("同时下载 \(model.concurrentDownloads) 个文件", value: $model.concurrentDownloads, in: 1...64)
                        Text("建议保持默认 5。调整后需重新连接，登录会话会保留。")
                            .font(.callout).foregroundStyle(.secondary)
                    }.padding(.top, 12)
                }
                saveButton
            }
        }
    }

    private var organization: some View {
        VStack(alignment: .leading, spacing: InterfaceStyle.sectionSpacing) {
            SettingsCard("文件夹分类", subtitle: "用熟悉的目录结构，找到下载的内容。") {
                Toggle("按频道建立文件夹", isOn: $model.foldersByChannel)
                Toggle("再按年月分类", isOn: $model.foldersByDate)
                Toggle("再按媒体类型分类", isOn: $model.foldersByType)
            }
            SettingsCard("文件名称", subtitle: "选择文件名里保留的信息。") {
                Toggle("消息编号", isOn: $model.namesWithID)
                Toggle("原始文件名", isOn: $model.namesWithOriginal)
                Toggle("消息说明文字", isOn: $model.namesWithCaption)
                Text("推荐保留消息编号，避免同名文件混淆。只影响新文件。")
                    .font(.callout).foregroundStyle(.secondary)
                saveButton
            }
        }
    }

    private var storage: some View {
        SettingsCard("保存到哪里", subtitle: "这台 Mac、外置磁盘或已挂载的网络磁盘都可以。") {
            Label(URL(filePath: model.savePath).lastPathComponent, systemImage: "folder.fill")
                .font(.title3.bold()).foregroundStyle(.indigo)
            Text(model.savePath).font(.callout.monospaced()).textSelection(.enabled)
                .padding(14).frame(maxWidth: .infinity, alignment: .leading)
                .background(.quaternary.opacity(0.3), in: .rect(cornerRadius: 10))
            HStack {
                Button("更换文件夹…", systemImage: "folder.badge.gearshape") { Task { await model.chooseFolder() } }
                Button("在 Finder 中打开", systemImage: "arrow.up.right.square") { NSWorkspace.shared.open(URL(filePath: model.savePath)) }
            }
            Text("无需重新登录。新任务使用新位置，已有任务继续保存在原位置。")
                .font(.callout).foregroundStyle(.secondary)
        }
    }

    private var saveButton: some View {
        Button("保存更改", systemImage: "checkmark") { Task { await model.saveDownloadPreferences() } }
            .buttonStyle(.borderedProminent).controlSize(.large)
            .disabled(![model.downloadPhotos, model.downloadVideos, model.downloadAudio, model.downloadDocuments,
                         model.downloadVoice, model.downloadVideoNotes, model.downloadAnimations, model.downloadText].contains(true))
    }
}
