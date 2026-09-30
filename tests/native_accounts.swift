import Foundation

@main
struct ProfileChecks {
    @MainActor static func main() throws {
        let root = AccountProfiles.root
        try FileManager.default.createDirectory(at: root.appending(path: "sessions"), withIntermediateDirectories: true)
        let sentinel = root.appending(path: "sessions/existing.session")
        try Data("keep-existing-login".utf8).write(to: sentinel)
        let store = AccountProfiles.shared
        try store.load()
        precondition(store.record.active == "legacy")
        let legacyDirectory = try store.directory(for: "legacy")
        precondition(legacyDirectory == root.resolvingSymlinksInPath())
        let second = "11111111-2222-4333-8444-555555555555"
        try store.add(second)
        try store.activate(second)
        try store.update(["display_name": "Example", "username": "sample"], signedIn: true)
        try store.load()
        precondition(store.record.active == second)
        precondition(store.record.profiles.last?.name == "Example")
        precondition(store.record.profiles.last?.detail == "@sample")
        try store.activate("legacy")
        let originalSession = try Data(contentsOf: sentinel)
        precondition(originalSession == Data("keep-existing-login".utf8))
        let permissions = try FileManager.default.attributesOfItem(atPath: root.appending(path: "account-profiles.json").path)[.posixPermissions] as? NSNumber
        precondition(permissions?.intValue == 0o600)
        do { _ = try store.directory(for: "../../bad"); preconditionFailure("Path traversal allowed") }
        catch { }
        print("Native account profiles: migration, separation, persistence and permissions passed.")
    }
}
