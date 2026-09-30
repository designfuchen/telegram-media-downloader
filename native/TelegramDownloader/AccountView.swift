import SwiftUI

struct AccountMenu: View {
    @Bindable var model: AppModel
    var compact = false
    @State private var target: AccountProfile?
    @State private var adding = false
    @State private var signingOut = false
    @State private var confirming = false

    var body: some View {
        Menu {
            ForEach(model.profiles) { profile in
                Button {
                    target = profile; adding = false; signingOut = false; confirming = true
                } label: {
                    Label(profile.name, systemImage: profile.id == model.activeProfileID ? "checkmark" : "person.crop.circle")
                }.disabled(profile.id == model.activeProfileID)
            }
            Divider()
            Button("添加另一个账号", systemImage: "person.badge.plus") {
                target = nil; adding = true; signingOut = false; confirming = true
            }
            if model.hasAccount {
                Button("退出当前账号", systemImage: "rectangle.portrait.and.arrow.right", role: .destructive) {
                    target = nil; adding = false; signingOut = true; confirming = true
                }
            }
        } label: {
            Label(compact ? model.accountName : "切换账号", systemImage: "person.crop.circle")
                .lineLimit(1)
        }.disabled(model.busy || !model.connected)
            .confirmationDialog(signingOut ? "退出当前账号？" : adding ? "添加另一个 Telegram 账号？" : "切换到 \(target?.name ?? "另一个账号")？",
                                isPresented: $confirming, titleVisibility: .visible) {
                if signingOut {
                    Button("退出登录", role: .destructive) { Task { await model.logoutAccount() } }
                } else if adding {
                    Button("添加账号") { Task { await model.addAccount() } }
                } else if let target {
                    Button("切换账号") { Task { await model.switchAccount(to: target.id) } }
                }
                Button("取消", role: .cancel) { }
            } message: {
                Text(signingOut ? "将暂停下载并撤销本机登录。已下载文件和记录会保留，再使用此账号需要重新登录。" :
                    "当前下载会暂停。每个账号有独立的登录、频道和下载记录；切回后可继续。")
            }
    }
}

struct AccountView: View {
    @Bindable var model: AppModel
    @State private var confirmingLogout = false
    var body: some View {
        VStack(alignment: .leading, spacing: InterfaceStyle.sectionSpacing) {
            SettingsCard("当前账号") {
                HStack(spacing: 16) {
                    Image(systemName: model.ready ? "person.crop.circle.fill" : "person.crop.circle")
                        .font(.system(size: 44)).foregroundStyle(.indigo)
                    VStack(alignment: .leading, spacing: 6) {
                        Text(model.accountName).font(.title3.bold()).textSelection(.enabled)
                        Text(model.hasAccount ? model.accountDetail : "登录后显示 Telegram 账号信息").foregroundStyle(.secondary)
                    }
                    Spacer()
                    StatusLabel(text: model.ready ? "已登录" : model.hasAccount ? "正在连接账号" : "未登录", active: model.ready)
                }
                Divider()
                HStack {
                    Text("账号在本机管理，切换后下载保持暂停。")
                        .font(.callout).foregroundStyle(.secondary)
                    Spacer()
                    AccountMenu(model: model)
                    if model.hasAccount {
                        Button("退出登录", role: .destructive) { confirmingLogout = true }
                            .confirmationDialog("退出当前账号？", isPresented: $confirmingLogout, titleVisibility: .visible) {
                                Button("退出登录", role: .destructive) { Task { await model.logoutAccount() } }
                                Button("取消", role: .cancel) { }
                            } message: {
                                Text("将暂停下载并撤销本机登录。文件和下载记录会保留；其他设备不受影响。")
                            }
                    }
                }
            }
            if !model.credentialsSaved { credentials }
            if !model.hasAccount {
                SettingsCard("登录 Telegram", subtitle: "使用你的 Telegram 手机号，连接已有账号。") {
                    SetupView(model: model, isSettings: true).login
                }
            }
            if model.credentialsSaved { credentials }
            SettingsCard("账号与文件的边界") {
                Label("仅下载当前账号有权访问的频道内容。", systemImage: "checkmark.shield")
                Label("退出账号不会删除已下载文件，也不会退出其他设备。", systemImage: "internaldrive")
            }
        }
    }

    private var credentials: some View {
        SettingsCard("Telegram API 凭证", subtitle: "连接 Telegram 所需的 API ID 和 API Hash，只需填写一次。") {
            if model.credentialsSaved {
                DisclosureGroup("凭证已保存 · 查看或修改") {
                    SetupView(model: model, isSettings: true).account.padding(.top, 16)
                }
            } else { SetupView(model: model, isSettings: true).account }
        }
    }
}
