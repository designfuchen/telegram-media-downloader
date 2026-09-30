import SwiftUI

struct SetupView: View {
    @Bindable var model: AppModel
    let isSettings: Bool
    @State private var step = 0
    @Environment(\.accessibilityReduceMotion) private var reduceMotion
    private let titles = ["API 凭证", "网络连接", "登录账号", "添加频道", "保存位置与开始"]
    private let icons = ["key", "network", "person.crop.circle", "plus.rectangle.on.rectangle", "folder"]

    var body: some View {
        VStack(alignment: .leading, spacing: 26) {
            PageHeading(title: "开始第一次下载", subtitle: "准备 API 凭证，连接 Telegram，再选择频道和保存位置。")
            HStack(spacing: 8) {
                ForEach(titles.indices, id: \.self) { index in
                    Button {
                        withAnimation(reduceMotion ? nil : .smooth(duration: 0.25)) { step = index }
                    } label: {
                        VStack(spacing: 8) {
                            Image(systemName: icons[index]).font(.title3)
                            Text(titles[index]).font(.caption)
                        }.frame(maxWidth: .infinity).padding(.vertical, 14)
                            .background(step == index ? Color.indigo.opacity(0.12) : Color.clear, in: .rect(cornerRadius: 12))
                            .foregroundStyle(step == index ? Color.indigo : Color.secondary)
                    }.buttonStyle(.plain)
                        .disabled(!isSettings && index >= 2 && !model.networkVerified)
                        .accessibilityAddTraits(step == index ? [.isSelected] : [])
                }
            }
            SettingsCard("") {
                VStack(alignment: .leading, spacing: 22) {
                    HStack(spacing: 12) {
                        Text("0\(step + 1)").font(.title2.bold()).foregroundStyle(.indigo).monospacedDigit()
                        Text(titles[step]).font(.title2.bold())
                        Spacer()
                    }
                    switch step {
                    case 0: account
                    case 1: network
                    case 2: login
                    case 3: channel
                    default: storage
                    }
                    if model.busy {
                        HStack(spacing: 10) {
                            ProgressView().controlSize(.small)
                            Text(model.busyMessage).foregroundStyle(.secondary)
                        }.accessibilityElement(children: .combine)
                    } else if let error = model.error {
                        Label(error, systemImage: "exclamationmark.triangle.fill")
                            .foregroundStyle(.orange).textSelection(.enabled)
                    } else if !model.message.isEmpty {
                        Label(model.message, systemImage: "checkmark.circle.fill")
                            .foregroundStyle(.secondary).textSelection(.enabled)
                    }
                }.frame(maxWidth: .infinity, alignment: .leading)
            }.id(step).transition(reduceMotion ? .opacity : .opacity.combined(with: .offset(y: 6)))
            HStack {
                if step > 0 { Button("上一步") { step -= 1 } }
                Spacer()
                if step < 4 {
                    Button("下一步", systemImage: "arrow.right") {
                        withAnimation(reduceMotion ? nil : .smooth(duration: 0.25)) { step += 1 }
                    }.buttonStyle(.borderedProminent).controlSize(.large)
                        .disabled((step == 0 && !model.credentialsSaved) || (step == 1 && !model.networkVerified) || (step == 2 && !model.ready) || (step == 3 && model.count == 0))
                }
            }
        }.disabled(model.busy)
            .onAppear { step = initialStep }
            .onChange(of: model.accountRevision) { step = initialStep }
    }

    private var initialStep: Int {
        model.ready ? (model.count > 0 ? 4 : 3) : model.credentialsSaved ? 1 : 0
    }

    var account: some View {
        VStack(alignment: .leading, spacing: 18) {
            Text("在 Telegram 官方网站创建应用，复制 API ID 和 API Hash。")
                .foregroundStyle(.secondary)
            Link(destination: URL(string: "https://my.telegram.org/apps")!) {
                Label("打开 Telegram 官方申请页面", systemImage: "arrow.up.right.square")
            }
            DisclosureGroup("不知道怎么申请？查看分步教程") {
              VStack(alignment: .leading, spacing: 8) {
                Text("1. 用你的 Telegram 手机号登录官方网站。")
                Text("2. 选择 API development tools，创建一个应用。")
                Text("3. App title 填 MyDownloader，Short name 填 mydownloader，Platform 选 Desktop。")
                Text("4. 把页面上的 App api_id 和 App api_hash 复制到下方。")
            }.font(.callout).foregroundStyle(.secondary).textSelection(.enabled).padding(.top, 10)
            }
            LabeledContent("API ID") { TextField("填写 App api_id 中的数字", text: $model.apiID).textFieldStyle(.roundedBorder) }
            LabeledContent("API Hash") { SecureField(model.credentialsSaved ? "已保存；留空即可保留" : "32 位字符", text: $model.apiHash).textFieldStyle(.roundedBorder) }
            HStack {
                Button("保存 API 凭证") { Task { await model.saveAccount() } }.buttonStyle(.borderedProminent)
                    .disabled(model.apiID.isEmpty || (!model.credentialsSaved && model.apiHash.isEmpty))
                if model.credentialsSaved { Label("凭证已保存", systemImage: "checkmark.circle").foregroundStyle(.secondary) }
            }
            Text("这是 Telegram 的 API 凭证，不是代理节点 UUID，也不是验证码。")
                .font(.caption).foregroundStyle(.secondary)
        }
    }

    var network: some View {
        VStack(alignment: .leading, spacing: 18) {
            Picker("连接方式", selection: $model.networkMode) {
                Text("直接连接").tag("direct")
                Text("导入节点").tag("link")
            }.pickerStyle(.segmented)
            if model.networkMode == "link" {
                Text("一个入口，自动识别节点链接、Clash YAML 和 JSON 配置。")
                Text("支持 SS、VLESS、VMess、Trojan、Hysteria2、TUIC、SOCKS5、HTTP 等格式；具体参数会在导入时校验。")
                    .font(.caption).foregroundStyle(.secondary)
                TextEditor(text: $model.nodeContent).font(.body.monospaced())
                    .frame(height: 120).padding(6).background(.background, in: .rect(cornerRadius: 8))
                    .overlay(RoundedRectangle(cornerRadius: 8).stroke(.quaternary))
                    .accessibilityLabel("节点链接或 YAML / JSON 配置")
                HStack {
                    Button("选择配置文件", systemImage: "doc") { Task { await model.importNodeFile() } }
                    Button("识别节点") { Task { await model.previewNodes() } }.disabled(model.nodeContent.isEmpty)
                }
                if !model.nodes.isEmpty {
                    Picker("使用节点", selection: $model.nodeIndex) {
                        ForEach(model.nodes.indices, id: \.self) { index in Text(model.nodes[index]).tag(index) }
                    }
                }
            } else {
                Label("适合这台 Mac 可以直接连接 Telegram 的网络。", systemImage: "network")
                    .foregroundStyle(.secondary)
            }
            HStack {
                Button(model.networkTesting ? "正在测试连接…" : "保存并测试连接") { Task { await model.saveNetwork() } }.buttonStyle(.borderedProminent)

            }
            Label(model.networkVerified ? "连接测试通过。" : "保存并测试后，确认 Telegram 可以连接。",
                  systemImage: model.networkVerified ? "checkmark.circle.fill" : "network")
                .foregroundStyle(model.networkVerified ? Color.green : Color.secondary)
            Text("配置内容只在本机处理。导入不会自动拉取远程订阅地址。")
                .font(.caption).foregroundStyle(.secondary)
        }
    }

    var login: some View {
        VStack(alignment: .leading, spacing: 18) {
            Label(model.ready ? "Telegram 已登录" : model.loginMessage,
                  systemImage: model.ready ? "checkmark.shield" : "lock.shield")
                .font(.headline).fixedSize(horizontal: false, vertical: true)
            if ["connecting", "finishing"].contains(model.loginStage) && !model.busy {
                HStack(spacing: 10) {
                    ProgressView().controlSize(.small)
                    Text(model.loginStage == "connecting" ? "正在连接 Telegram，请稍候…" : "正在完成登录，请稍候…")
                        .foregroundStyle(.secondary)
                }
            }
            switch model.loginStage {
            case "phone":
                TextField("手机号，含国家区号，例如 +86…", text: $model.phone).textFieldStyle(.roundedBorder)
            case "code":
                Text("请查看 Telegram 官方消息中的验证码。")
                SecureField("验证码", text: $model.code).textFieldStyle(.roundedBorder)
            case "password":
                SecureField("Telegram 两步验证密码", text: $model.password).textFieldStyle(.roundedBorder)
            default: EmptyView()
            }
            if ["phone", "code", "password"].contains(model.loginStage) {
                Button(model.loginStage == "phone" ? "获取验证码" : "继续登录") { Task { await model.login() } }
                    .buttonStyle(.borderedProminent)
                    .disabled(!model.networkVerified || Date() < model.loginRetryAt)
            }
            if model.loginStage == "code" {
                TimelineView(.periodic(from: .now, by: 1)) { context in
                    let remaining = max(0, Int(ceil(model.resendAvailableAt.timeIntervalSince(context.date))))
                    Button(remaining > 0 ? "\(remaining) 秒后可重新发送" : "没收到验证码？重新发送") {
                        Task { await model.resendCode() }
                    }.disabled(remaining > 0 || !model.networkVerified)
                }
                Text("发送方式由 Telegram 决定，不保证通过短信发送。请优先查看其他已登录设备中的 Telegram 官方消息。")
                    .font(.caption).foregroundStyle(.secondary)
            }
            if !model.networkVerified && !model.ready {
                if isSettings {
                    Button("前往网络设置并测试连接", systemImage: "network") { model.settingsSection = "网络" }
                } else { Text("请在“网络连接”中测试成功后继续登录。").foregroundStyle(.secondary) }
            }
            Text("验证码和两步验证密码不会写入配置文件。登录会话保存在本机。")
                .font(.caption).foregroundStyle(.secondary)
        }
    }

    private var channel: some View {
        VStack(alignment: .leading, spacing: 18) {
            Text("粘贴频道链接、@用户名，或以 -100 开头的频道 ID。")
                .foregroundStyle(.secondary)
            TextField("例如 https://t.me/channel_name", text: $model.channelInput).textFieldStyle(.roundedBorder)
            Button("添加频道", systemImage: "plus") { Task { await model.addChannel() } }
                .buttonStyle(.borderedProminent).disabled(model.channelInput.isEmpty)
            Label("已保存 \(model.count) 个频道", systemImage: "rectangle.stack")
            Text("私密频道请先在 Telegram 中加入。你只能下载当前账号有权访问的内容。")
                .font(.caption).foregroundStyle(.secondary)
        }
    }

    var storage: some View {
        VStack(alignment: .leading, spacing: 18) {
            Label("保存在这台 Mac，或你已挂载的外置磁盘。", systemImage: "internaldrive")
            Text(model.savePath).font(.callout.monospaced()).textSelection(.enabled)
                .padding(14).frame(maxWidth: .infinity, alignment: .leading)
                .background(.quaternary.opacity(0.3), in: .rect(cornerRadius: 10))
            Button("选择保存文件夹…", systemImage: "folder") { Task { await model.chooseFolder() } }
            Divider()
            Label(model.ready ? "账号已连接" : "请先完成 Telegram 登录",
                  systemImage: model.ready ? "checkmark.circle" : "circle")
            Label(model.count > 0 ? "频道已添加" : "请先添加一个频道",
                  systemImage: model.count > 0 ? "checkmark.circle" : "circle")
            Button("开始下载", systemImage: "arrow.down.circle.fill") { Task { await model.channelAction("start") } }
                .buttonStyle(.borderedProminent).controlSize(.large).disabled(!model.ready || model.restartRequired || model.count == 0)
            Text("开始后，在侧栏“频道与下载”查看扫描结果、下载进度或暂停任务。")
                .font(.caption).foregroundStyle(.secondary)
        }
    }
}
