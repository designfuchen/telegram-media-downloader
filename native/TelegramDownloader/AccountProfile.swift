import Foundation

struct AccountProfile: Codable, Identifiable, Equatable {
    let id: String
    var name: String
    var detail = "尚未登录"
    var signedIn = false
}

/// Existing installations remain in place. New accounts get independent state.
@MainActor
final class AccountProfiles {
    static let shared = AccountProfiles()
    struct Record: Codable {
        var active: String
        var profiles: [AccountProfile]
    }
    static var root: URL {
        ProcessInfo.processInfo.environment["TMD_DESKTOP_DATA_DIR"].map { URL(filePath: $0) } ??
        FileManager.default.homeDirectoryForCurrentUser.appending(path: "Library/Application Support/Telegram Downloader Native")
    }
    private(set) var record = Record(active: "legacy", profiles: [AccountProfile(id: "legacy", name: "账号 1")])
    private var file: URL { Self.root.appending(path: "account-profiles.json") }

    func load() throws {
        if FileManager.default.fileExists(atPath: file.path) {
            let decoded = try JSONDecoder().decode(Record.self, from: Data(contentsOf: file))
            guard !decoded.profiles.isEmpty, Set(decoded.profiles.map(\.id)).count == decoded.profiles.count,
                  decoded.profiles.allSatisfy({ $0.id == "legacy" || UUID(uuidString: $0.id) != nil }),
                  decoded.profiles.contains(where: { $0.id == decoded.active }) else {
                throw CocoaError(.fileReadCorruptFile)
            }
            record = decoded
        } else { try save() }
    }

    func directory(for id: String) throws -> URL {
        guard id == "legacy" || UUID(uuidString: id) != nil else { throw CocoaError(.fileReadInvalidFileName) }
        let root = Self.root.resolvingSymlinksInPath()
        let directory = id == "legacy" ? root : root.appending(path: "accounts/\(id)")
        guard directory.resolvingSymlinksInPath().path == directory.standardizedFileURL.path else {
            throw CocoaError(.fileReadNoPermission)
        }
        return directory
    }

    func add(_ id: String) throws {
        guard UUID(uuidString: id) != nil, !record.profiles.contains(where: { $0.id == id }) else { throw CocoaError(.fileWriteInvalidFileName) }
        var updated = record
        updated.profiles.append(AccountProfile(id: id, name: "账号 \(record.profiles.count + 1)"))
        try save(updated)
    }

    func activate(_ id: String) throws {
        guard record.profiles.contains(where: { $0.id == id }) else { throw CocoaError(.fileReadCorruptFile) }
        var updated = record
        updated.active = id
        try save(updated)
    }

    func update(_ identity: [String: Any]?, signedIn: Bool) throws {
        guard let index = record.profiles.firstIndex(where: { $0.id == record.active }) else { return }
        let previous = record.profiles[index]
        var updated = record
        if let identity {
            updated.profiles[index].name = identity["display_name"] as? String ?? previous.name
            let username = identity["username"] as? String ?? ""
            updated.profiles[index].detail = username.isEmpty ? identity["phone_hint"] as? String ?? "Telegram 账号" : "@" + username
        }
        updated.profiles[index].signedIn = signedIn
        if !signedIn && identity == nil { updated.profiles[index].detail = "尚未登录" }
        if previous != updated.profiles[index] { try save(updated) }
    }

    private func save(_ updated: Record? = nil) throws {
        let candidate = updated ?? record
        try FileManager.default.createDirectory(at: Self.root, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        try JSONEncoder().encode(candidate).write(to: file, options: .atomic)
        try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: file.path)
        record = candidate
    }
}
