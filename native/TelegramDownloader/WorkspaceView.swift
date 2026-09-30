import SwiftUI

struct WorkspaceView: View {
    @Bindable var model: AppModel
    @AppStorage("appearance") private var appearance = "system"
    @State private var selection = "开始使用"
    @Environment(\.accessibilityReduceMotion) private var reduceMotion

    var body: some View {
        NavigationSplitView {
            VStack(alignment: .leading, spacing: 24) {
                Label {
                    VStack(alignment: .leading, spacing: 4) {
                        Text("Telegram").font(.title3.bold())
                        Text("下载器").foregroundStyle(.secondary)
                    }
                } icon: {
                    Image(systemName: "arrow.down.to.line.compact")
                        .font(.title).foregroundStyle(.white)
                        .frame(width: 42, height: 42)
                        .background(.indigo.gradient, in: .rect(cornerRadius: 13))
                }.padding(.horizontal, 18).padding(.top, 30)
                List(selection: $selection) {
                    Section("工作空间") {
                        Label(model.ready ? "使用指南" : "开始使用", systemImage: "sparkles").tag("开始使用")
                        Label("频道与下载", systemImage: "arrow.down.circle").tag("频道与下载")
                        Label("批量导入", systemImage: "square.and.arrow.down").tag("批量导入")
                        Label("设置", systemImage: "slider.horizontal.3").tag("设置")
                    }
                }.listStyle(.sidebar)
                VStack(alignment: .leading, spacing: 8) {
                    Label(model.connected ? "已就绪" : "正在启动…",
                          systemImage: model.connected ? "checkmark.circle.fill" : "clock")
                        .font(.caption).foregroundStyle(.secondary)
                    Text("文件保存在你的设备里。")
                        .font(.callout).foregroundStyle(.secondary)
                }.padding(20)
            }.navigationSplitViewColumnWidth(min: 200, ideal: 220, max: 260)
        } detail: {
            VStack(spacing: 0) {
                HStack {
                    Text(selection).font(.headline)
                    Spacer()
                    AccountMenu(model: model, compact: true)
                        .menuStyle(.borderlessButton).fixedSize()
                    Menu("外观", systemImage: "circle.lefthalf.filled") {
                        Picker("外观", selection: $appearance) {
                            Text("跟随系统").tag("system")
                            Text("浅色").tag("light")
                            Text("深色").tag("dark")
                        }
                    }.menuStyle(.borderlessButton).fixedSize()
                    if model.busy { ProgressView().controlSize(.small).accessibilityLabel("正在处理") }
                }.padding(.horizontal, InterfaceStyle.pagePadding).padding(.vertical, 18)
                Divider()
                if !model.connected {
                    ContentUnavailableView {
                        Label("正在准备你的下载空间", systemImage: "arrow.down.circle")
                    } description: {
                        Text(model.error ?? "首次启动可能需要片刻，无需安装 Python 或其他组件。")
                    } actions: {
                        if model.error != nil { Button("重新连接") { Task { await model.restart() } } }
                        else { ProgressView() }
                    }
                } else {
                    ScrollView {
                        VStack(alignment: .leading, spacing: 24) {
                            if model.restartRequired {
                                SettingsCard("") {
                                    HStack {
                                        Label("设置已更新，重新连接后生效", systemImage: "arrow.clockwise")
                                        Spacer()
                                        Button("重新连接") { Task { await model.restart() } }
                                    }.padding(8)
                                }
                            }
                            if selection == "频道与下载" { downloads }
                            else if selection == "批量导入" { ImportsView(model: model) }
                            else if selection == "设置" { PreferencesView(model: model) }
                            else { SetupView(model: model, isSettings: false) }
                        }.padding(InterfaceStyle.pagePadding).frame(maxWidth: InterfaceStyle.contentWidth).frame(maxWidth: .infinity)
                    }.background(Color(nsColor: .windowBackgroundColor))
                }
                if let error = model.error {
                    feedback(error, symbol: "exclamationmark.triangle", isError: true)
                } else if !model.message.isEmpty {
                    feedback(model.message, symbol: "checkmark.circle", isError: false)
                }
            }
            .animation(reduceMotion ? nil : .easeInOut(duration: 0.2), value: selection)
        }
        .preferredColorScheme(appearance == "dark" ? .dark : appearance == "light" ? .light : nil)
        .onChange(of: model.accountRevision) {
            selection = model.ready ? "频道与下载" : "设置"
            model.settingsSection = "账号"
        }
        .task {
            await model.connect()
            if model.ready { selection = "频道与下载" }
            while !Task.isCancelled {
                do {
                    try await Task.sleep(for: .seconds(3))
                    if model.connected && !model.busy { try await model.refresh() }
                } catch is CancellationError { return }
                catch { if !model.busy { model.connected = false; model.error = "引擎连接中断，请点击重新连接。" } }
            }
        }
    }

    private func feedback(_ text: String, symbol: String, isError: Bool) -> some View {
        HStack(alignment: .top) {
            Label(text, systemImage: symbol).textSelection(.enabled)
            Spacer()
            Button("关闭提示", systemImage: "xmark") { model.error = nil; model.message = "" }
                .labelStyle(.iconOnly).buttonStyle(.plain)
        }.font(.callout).padding(16)
            .background(isError ? Color.orange.opacity(0.12) : Color.indigo.opacity(0.08))
    }

    private var downloads: some View {
        VStack(alignment: .leading, spacing: 24) {
            PageHeading(title: "频道与下载", subtitle: "添加频道，查看进度，把内容保存在自己的设备里。")
            SettingsCard("") {
                HStack {
                    TextField("粘贴频道链接、@用户名或频道 ID", text: $model.channelInput)
                        .textFieldStyle(.roundedBorder).onSubmit { Task { await model.addChannel() } }
                    Button("添加频道", systemImage: "plus") { Task { await model.addChannel() } }
                        .buttonStyle(.borderedProminent).disabled(model.channelInput.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty || model.busy)
                }
            }
            HStack {
                Text("\(model.count) 个频道").font(.headline)
                Spacer()
                Button("暂停全部", systemImage: "pause") { Task { await model.channelAction("pause") } }
                    .disabled(model.count == 0 || model.busy)
                Button("开始下载", systemImage: "arrow.down") { Task { await model.channelAction("start") } }
                    .buttonStyle(.borderedProminent).disabled(!model.ready || model.restartRequired || model.count == 0 || model.busy)
            }
            if model.channels.isEmpty {
                ContentUnavailableView("还没有频道", systemImage: "rectangle.stack.badge.plus",
                    description: Text("在上方粘贴第一个频道。私密频道需要先用当前账号加入。"))
                    .frame(minHeight: 260)
            } else {
                ForEach(model.channels) { channel in
                    SettingsCard("") {
                        VStack(alignment: .leading, spacing: 14) {
                            HStack(alignment: .top) {
                                Image(systemName: "antenna.radiowaves.left.and.right")
                                    .font(.title2).foregroundStyle(.indigo).frame(width: 36, height: 36)
                                VStack(alignment: .leading, spacing: 5) {
                                    Text(channel.title).font(.headline).textSelection(.enabled)
                                    Text(channel.reason).font(.callout).foregroundStyle(.secondary).fixedSize(horizontal: false, vertical: true)
                                }
                                Spacer()
                                if let action = channel.action {
                                    Button(action == "pause" ? "暂停" : "开始下载", systemImage: action == "pause" ? "pause" : "play") {
                                        Task { await model.channelAction(action, id: channel.id) }
                                    }.disabled(model.busy || (action != "pause" && channel.blocked))
                                }
                            }
                            ChannelPreviewView(channel: channel, savePath: model.savePath)
                            if channel.activeFiles > 0 {
                                VStack(alignment: .leading, spacing: 8) {
                                    HStack {
                                        Label("当前传输", systemImage: "arrow.down.circle.fill")
                                        Spacer()
                                        Text(channel.speed).monospacedDigit()
                                    }
                                    if let progress = channel.activeProgress {
                                        ProgressView(value: progress).accessibilityLabel("当前文件下载进度")
                                    } else {
                                        ProgressView().controlSize(.small).accessibilityLabel("等待文件传输进度")
                                    }
                                    Text("\(channel.transferred) / \(channel.activeTotal) · \(channel.eta)")
                                        .font(.caption).foregroundStyle(.secondary).monospacedDigit()
                                }
                            }
                            if channel.total > 0 {
                                ProgressView(value: channel.progress).accessibilityLabel("下载进度")
                                Text("已完成 \(channel.completed) / \(channel.total) 个文件")
                                    .font(.caption).foregroundStyle(.secondary).monospacedDigit()
                            }
                        }
                    }
                }
                HStack {
                    Button("上一页") { model.page -= 1; Task { await model.perform { try await model.refresh() } } }.disabled(model.page == 1 || model.busy)
                    Spacer()
                    Text("第 \(model.page) 页").foregroundStyle(.secondary)
                    Spacer()
                    Button("下一页") { model.page += 1; Task { await model.perform { try await model.refresh() } } }.disabled(model.page * 50 >= model.count || model.busy)
                }
            }
        }
    }
}
