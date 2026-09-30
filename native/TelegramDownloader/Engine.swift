import AppKit
import Foundation

/// Owns the bundled engine. No shell, external interpreter, or browser is required.
@MainActor
final class Engine {
    static let shared = Engine()
    private var process: Process?
    private var handshake: URL?
    private var session = URLSession(configuration: .ephemeral)
    private var base: URL?
    private var authenticated = false

    func start(profileID: String? = nil) async throws {
        if process?.isRunning == true {
            if authenticated { return }
            throw failure("引擎仍在准备，请退出应用后重新打开。")
        }
        let directory = try AccountProfiles.shared.directory(for: profileID ?? AccountProfiles.shared.record.active)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true,
            attributes: [.posixPermissions: 0o700])
        let handoff = directory.appending(path: "bridge-\(UUID().uuidString).json")
        handshake = handoff
        let token = UUID().uuidString + UUID().uuidString
        guard let resources = Bundle.main.resourceURL else { throw failure("应用资源缺失，请重新安装。") }
        let child = Process()
        child.executableURL = resources.appending(path: "Engine/Telegram Downloader")
        var environment = ProcessInfo.processInfo.environment
        environment["TMD_NO_BROWSER"] = "1"
        environment["TMD_REQUIRE_KEYCHAIN"] = "1"
        environment["TMD_DESKTOP_DATA_DIR"] = directory.path
        environment["TMD_DESKTOP_ROOT_DIR"] = AccountProfiles.root.path
        environment["TMD_PARENT_LAUNCH_TOKEN"] = token
        environment["TMD_DESKTOP_HANDSHAKE"] = handoff.path
        child.environment = environment
        child.standardOutput = FileHandle.nullDevice
        child.standardError = FileHandle.nullDevice
        try child.run()
        process = child
        for _ in 0..<120 {
            try await Task.sleep(for: .milliseconds(250))
            guard child.isRunning else { throw failure("下载引擎未能启动。请退出后重新打开应用。") }
            if let data = try? Data(contentsOf: handoff),
               let record = try? JSONSerialization.jsonObject(with: data) as? [String: Int],
               let port = record["port"] {
                base = URL(string: "http://127.0.0.1:\(port)")
                do {
                    _ = try await request("/api/local-launch", body: ["token": token])
                    try? FileManager.default.removeItem(at: handoff)
                    authenticated = true
                    return
                } catch { /* The socket may not yet be listening. */ }
            }
        }
        stop()
        throw failure("启动超时。请重新打开应用后重试。")
    }

    func request(_ path: String, body: [String: Any]? = nil) async throws -> [String: Any] {
        guard let base, let url = URL(string: path, relativeTo: base) else { throw failure("下载引擎尚未连接。") }
        var request = URLRequest(url: url)
        request.timeoutInterval = path == "/api/account/logout" ? 45 : 35
        if let body {
            request.httpMethod = "POST"
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            request.httpBody = try JSONSerialization.data(withJSONObject: body)
        }
        let (data, response) = try await session.data(for: request)
        guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            throw failure("无法读取下载引擎的响应。")
        }
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode),
              object["ok"] as? Bool != false else {
            throw failure(object["message"] as? String ?? "操作未完成，请重试。")
        }
        return object
    }

    func restart(profileID: String? = nil) async throws {
        let old = process
        stop()
        for _ in 0..<450 {
            if old?.isRunning != true { break }
            try await Task.sleep(for: .milliseconds(100))
        }
        guard old?.isRunning != true else { throw failure("引擎仍在保存进度，请稍后重试。") }
        session.invalidateAndCancel()
        session = URLSession(configuration: .ephemeral)
        try await start(profileID: profileID)
    }

    func stop() {
        if process?.isRunning == true { process?.terminate() }
        base = nil
        authenticated = false
        if let handshake { try? FileManager.default.removeItem(at: handshake) }
    }

    private func failure(_ message: String) -> NSError {
        NSError(domain: "TelegramDownloader", code: 1, userInfo: [NSLocalizedDescriptionKey: message])
    }
}
