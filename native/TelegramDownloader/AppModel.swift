import AppKit
import Observation

@MainActor @Observable
final class AppModel {
    var connected = false
    var busy = false
    var busyMessage = "正在处理…"
    var message = ""
    var error: String?
    var channels: [Channel] = []
    var count = 0
    var page = 1
    var credentialsSaved = false
    var ready = false
    var restartRequired = false
    var loginStage = "waiting"
    var resendAvailableAt = Date.distantPast
    var loginRetryAt = Date.distantPast
    var networkVerified = false
    var networkTesting = false
    var loginMessage = ""
    var profiles: [AccountProfile] = []
    var activeProfileID = "legacy"
    var hasAccount = false
    private var connectionRevision = 0
    var accountName = "尚未登录"
    var accountDetail = "连接 Telegram 后即可开始下载"
    var settingsSection = "下载内容"
    var accountRevision = 0
    var apiID = ""
    var apiHash = ""
    var phone = ""
    var code = ""
    var password = ""
    var savePath = ""
    var networkMode = "direct" { didSet { if oldValue != networkMode { networkVerified = false } } }
    var nodeContent = "" { didSet { if oldValue != nodeContent { networkVerified = false } } }
    var nodeIndex = 0 { didSet { if oldValue != nodeIndex { networkVerified = false } } }
    var nodes: [String] = []
    var channelInput = ""
    var downloadPhotos = true
    var downloadVideos = true
    var downloadAudio = true
    var downloadDocuments = true
    var downloadVoice = false
    var downloadVideoNotes = false
    var downloadAnimations = false
    var downloadText = false
    var videoFormats = "all"
    var audioFormats = "all"
    var documentFormats = "all"
    var foldersByChannel = true
    var foldersByDate = true
    var foldersByType = false
    var namesWithID = true
    var namesWithOriginal = true
    var namesWithCaption = false
    var concurrentDownloads = 5

    private var config: [String: Any] = [:]

    func connect() async {
        await perform {
            try AccountProfiles.shared.load()
            self.syncProfiles()
            try await Engine.shared.start()
            self.connected = true
            try await self.loadConfig()
            try await self.refresh()
        }
    }

    func loadConfig() async throws {
        config = try await Engine.shared.request("/api/config")["config"] as? [String: Any] ?? [:]
        let media = Set(config["media_types"] as? [String] ?? [])
        downloadPhotos = media.contains("photo"); downloadVideos = media.contains("video")
        downloadAudio = media.contains("audio"); downloadDocuments = media.contains("document")
        downloadVoice = media.contains("voice"); downloadVideoNotes = media.contains("video_note")
        downloadAnimations = media.contains("animation")
        downloadText = config["enable_download_txt"] as? Bool ?? false
        let formats = config["file_formats"] as? [String: [String]] ?? [:]
        videoFormats = (formats["video"] ?? ["all"]).joined(separator: ",")
        audioFormats = (formats["audio"] ?? ["all"]).joined(separator: ",")
        documentFormats = (formats["document"] ?? ["all"]).joined(separator: ",")
        let folders = Set(config["file_path_prefix"] as? [String] ?? [])
        foldersByChannel = folders.contains("chat_title"); foldersByDate = folders.contains("media_datetime")
        foldersByType = folders.contains("media_type")
        let names = Set(config["file_name_prefix"] as? [String] ?? [])
        namesWithID = names.contains("message_id"); namesWithOriginal = names.contains("file_name")
        namesWithCaption = names.contains("caption")
        concurrentDownloads = config["max_download_task"] as? Int ?? 5
        apiID = String(describing: config["api_id"] ?? "")
        if apiID == "0" { apiID = "" }
        savePath = config["save_path"] as? String ?? ""
        let network = config["network_status"] as? [String: Any] ?? [:]
        networkMode = network["mode"] as? String ?? "direct"
        if networkMode == "ss" { networkMode = "link" }
    }

    func refresh() async throws {
        let revision = connectionRevision
        let status = try await Engine.shared.request("/api/setup/status")
        let account = try await Engine.shared.request("/api/account")
        let login = try await Engine.shared.request("/api/setup/login")
        let queue = try await Engine.shared.request("/api/channels?limit=50&page=\(page)")
        // A response from the outgoing account must never populate the new view.
        guard revision == connectionRevision else { return }
        credentialsSaved = status["credentials_saved"] as? Bool ?? false
        ready = status["telegram_ready"] as? Bool ?? false
        restartRequired = status["restart_required"] as? Bool ?? false
        let identity = account["account"] as? [String: Any]
        hasAccount = identity != nil
        try AccountProfiles.shared.update(identity, signedIn: hasAccount)
        syncProfiles()
        accountName = identity?["display_name"] as? String ?? profiles.first(where: { $0.id == activeProfileID })?.name ?? "尚未登录"
        accountDetail = profiles.first(where: { $0.id == activeProfileID })?.detail ?? "尚未登录"
        loginStage = login["stage"] as? String ?? "waiting"
        loginMessage = login["message"] as? String ?? ""
        resendAvailableAt = Date().addingTimeInterval(Double(login["resend_after"] as? Int ?? 0))
        loginRetryAt = Date().addingTimeInterval(Double(login["retry_after"] as? Int ?? 0))
        channels = (queue["data"] as? [[String: Any]] ?? []).map(Channel.init)
        count = status["channel_count"] as? Int ?? queue["count"] as? Int ?? 0
    }

    func save(_ fields: [String: Any]) async throws {
        var payload = config
        if let formats = config["file_formats"] as? [String: Any] {
            payload["file_formats"] = formats.mapValues { value in
                if let parts = value as? [String] { return parts.joined(separator: ",") }
                return String(describing: value)
            }
        }
        for (key, value) in fields { payload[key] = value }
        let result = try await Engine.shared.request("/api/config", body: payload)
        config = result["config"] as? [String: Any] ?? config
        message = result["message"] as? String ?? "已保存"
        try await refresh()
    }

    func saveDownloadPreferences() async {
        await perform("正在保存下载偏好…") {
            let media = [("photo", self.downloadPhotos), ("video", self.downloadVideos),
                         ("audio", self.downloadAudio), ("document", self.downloadDocuments),
                         ("voice", self.downloadVoice), ("video_note", self.downloadVideoNotes),
                         ("animation", self.downloadAnimations)].filter { $0.1 }.map { $0.0 }
            let folders = [("chat_title", self.foldersByChannel), ("media_datetime", self.foldersByDate),
                           ("media_type", self.foldersByType)].filter { $0.1 }.map { $0.0 }
            let names = [("message_id", self.namesWithID), ("file_name", self.namesWithOriginal),
                         ("caption", self.namesWithCaption)].filter { $0.1 }.map { $0.0 }
            let response = try await Engine.shared.request("/api/download/preferences", body: [
                "media_types": media, "file_formats": ["video": self.videoFormats, "audio": self.audioFormats, "document": self.documentFormats],
                "enable_download_txt": self.downloadText, "file_path_prefix": folders,
                "file_name_prefix": names, "max_download_task": self.concurrentDownloads])
            self.message = response["message"] as? String ?? "下载偏好已保存。"
            try await self.loadConfig()
            try await self.refresh()
        }
    }

    func saveAccount() async {
        await perform {
            try await self.save(["api_id": self.apiID, "api_hash": self.apiHash])
            self.apiHash = ""
        }
    }

    func previewNodes() async {
        await perform {
            let result = try await Engine.shared.request("/api/network/import", body: ["content": self.nodeContent])
            let entries = result["nodes"] as? [[String: Any]] ?? []
            self.nodes = entries.enumerated().map { index, node in
                (node["name"] as? String ?? "节点 \(index + 1)") + (node["supported"] as? Bool == false ? "（不支持）" : "")
            }
            self.nodeIndex = 0
            self.message = "识别到 \(self.nodes.count) 个节点，请选择并保存。"
        }
    }

    func saveNetwork() async {
        await perform("正在保存并测试网络连接…") {
            self.networkVerified = false
            self.networkTesting = true
            defer { self.networkTesting = false }
            var fields: [String: Any] = ["network_mode": self.networkMode]
            if self.networkMode == "link", !self.nodeContent.isEmpty {
                fields["node_content"] = self.nodeContent
                fields["node_index"] = self.nodeIndex
            }
            try await self.save(fields)
            self.nodeContent = ""
            self.nodes = []
            let response = try await Engine.shared.request("/api/network/test", body: [:])
            self.networkVerified = true
            self.message = response["message"] as? String ?? "连接测试通过，可以继续。"
        }
    }

    func resendCode() async {
        guard networkVerified else { error = "请先完成网络连接测试。"; return }
        await perform("正在重新发送验证码…") {
            defer { self.code = "" }
            do {
                _ = try await Engine.shared.request("/api/setup/login", body: ["action": "resend"])
            } catch {
                try? await self.refresh()
                throw error
            }
            try await self.refresh()
        }
    }

    func login() async {
        guard networkVerified else { error = "请先完成网络连接测试。"; return }
        await perform("正在提交登录信息，请稍候…") {
            let action = self.loginStage
            let value = action == "phone" ? self.phone : action == "code" ? self.code : self.password
            do {
                _ = try await Engine.shared.request("/api/setup/login", body: ["action": action, "value": value])
            } catch {
                try? await self.refresh()
                throw error
            }
            self.code = ""; self.password = ""
            try await self.refresh()
        }
    }

    func addChannel() async {
        await perform("正在添加频道…") {
            let result = try await Engine.shared.request("/api/channel_library", body: ["chat_id": self.channelInput])
            self.channelInput = ""
            self.count = result["channel_count"] as? Int ?? self.count
            self.message = "频道已保存。完成登录和保存位置设置后，点击开始下载。"
            try await self.refresh()
        }
    }

    func channelAction(_ action: String, id: String? = nil) async {
        await perform {
            var body: [String: Any] = ["action": action]
            if let id { body["chat_id"] = id } else { body["all"] = true }
            _ = try await Engine.shared.request("/api/channel_state", body: body)
            try await self.refresh()
        }
    }

    func chooseFolder() async {
        let panel = NSOpenPanel()
        panel.directoryURL = URL(filePath: savePath)
        panel.canChooseDirectories = true; panel.canChooseFiles = false
        panel.canCreateDirectories = true; panel.prompt = "保存到这里"
        if await panel.begin() == .OK, let url = panel.url {
            await perform("正在检查并保存文件夹…") {
                let result = try await Engine.shared.request("/api/storage/path", body: ["path": url.path])
                self.savePath = result["path"] as? String ?? url.path
                self.config["save_path"] = self.savePath
                self.message = result["message"] as? String ?? "保存位置已更新，无需重新登录。"
                try await self.refresh()
            }
        }
    }

    func importNodeFile() async {
        let panel = NSOpenPanel()
        panel.canChooseDirectories = false; panel.allowsMultipleSelection = false
        if await panel.begin() == .OK, let url = panel.url {
            await perform {
                let size = try url.resourceValues(forKeys: [.fileSizeKey]).fileSize ?? 0
                guard size <= 262144 else { throw CocoaError(.fileReadTooLarge) }
                self.nodeContent = try String(contentsOf: url, encoding: .utf8)
            }
            if error == nil { await previewNodes() }
        }
    }

    func restart() async {
        await perform("正在重新连接，请稍候…") {
            self.connected = false
            try await Engine.shared.restart()
            self.connected = true
            try await self.loadConfig()
            try await self.refresh()
        }
    }

    private func syncProfiles() {
        profiles = AccountProfiles.shared.record.profiles
        activeProfileID = AccountProfiles.shared.record.active
    }

    private func pauseForAccountChange() async throws {
        if count > 0 {
            _ = try await Engine.shared.request("/api/channel_state", body: ["action": "pause", "all": true])
        }
    }

    func addAccount() async {
        await perform("正在创建独立账号空间…") {
            let id = UUID().uuidString
            _ = try await Engine.shared.request("/api/account/profile", body: ["id": id])
            try AccountProfiles.shared.add(id)
            self.syncProfiles()
            try await self.changeAccount(to: id)
            self.settingsSection = "账号"
            self.message = self.credentialsSaved ? "账号空间已创建。API 和网络设置已带入，登录新的 Telegram 账号即可。" : "账号空间已创建。请填写 API 凭证并测试网络，再登录 Telegram。"
        }
    }

    func switchAccount(to id: String) async {
        guard id != activeProfileID else { return }
        await perform("正在保存进度并切换账号…") {
            try await self.changeAccount(to: id)
            self.message = "账号已切换。下载保持暂停，点击开始下载即可继续。"
        }
    }

    private func changeAccount(to id: String) async throws {
        let previous = activeProfileID
        connectionRevision += 1
        try await pauseForAccountChange()
        connected = false
        do {
            try await Engine.shared.restart(profileID: id)
            try AccountProfiles.shared.activate(id)
        } catch {
            try? await Engine.shared.restart(profileID: previous)
            connected = true
            try? await loadConfig()
            try? await refresh()
            throw error
        }
        resetAccountFields()
        connected = true
        try await loadConfig()
        try await refresh()
        accountRevision += 1
    }

    func logoutAccount() async {
        await perform("正在暂停下载并退出账号…") {
            self.connectionRevision += 1
            try await self.pauseForAccountChange()
            _ = try await Engine.shared.request("/api/account/logout", body: [:])
            try AccountProfiles.shared.update(nil, signedIn: false)
            self.resetAccountFields()
            self.connected = false
            try await Engine.shared.restart()
            self.connected = true
            try await self.loadConfig()
            try await self.refresh()
            self.accountRevision += 1
            self.message = "已退出登录。文件和下载记录已保留，需要重新登录才能继续下载。"
        }
    }

    private func resetAccountFields() {
        ready = false; hasAccount = false; networkVerified = false; restartRequired = false
        apiHash = ""; phone = ""; code = ""; password = ""; nodeContent = ""; nodes = []
        channelInput = ""; channels = []; count = 0; page = 1
        loginStage = "waiting"; loginMessage = "正在读取账号状态…"
        accountName = "尚未登录"; accountDetail = "尚未登录"
    }

    func perform(_ progress: String = "正在处理…", _ operation: () async throws -> Void) async {
        guard !busy else { return }
        busyMessage = progress
        busy = true; error = nil; message = ""
        defer { busy = false }
        do { try await operation() } catch { self.error = error.localizedDescription }
    }
}
