import Foundation

struct Channel: Identifiable {
    let id: String
    let title: String
    let reason: String
    let action: String?
    let progress: Double
    let completed: Int
    let total: Int
    let blocked: Bool
    let activeFiles: Int
    let activeProgress: Double?
    let speed: String
    let transferred: String
    let activeTotal: String
    let eta: String

    init(_ row: [String: Any]) {
        id = String(describing: row["chat"] ?? "")
        let name = row["chat_title"] as? String ?? ""
        title = name.isEmpty ? id : name
        reason = row["state_reason"] as? String ?? "等待扫描"
        action = row["channel_action"] as? String
        progress = min(1, max(0, (row["channel_progress"] as? Double ?? 0) / 100))
        completed = row["channel_completed_count"] as? Int ?? 0
        total = row["channel_total_count"] as? Int ?? 0
        activeFiles = row["active_files"] as? Int ?? 0
        activeProgress = (row["active_progress"] as? Double).map { min(1, max(0, $0 / 100)) }
        speed = row["active_speed"] as? String ?? "0 B/s"
        transferred = row["active_downloaded"] as? String ?? "0 B"
        activeTotal = row["active_total"] as? String ?? "0 B"
        eta = row["active_eta"] as? String ?? "等待进度"
        blocked = row["start_blocked"] as? Bool ?? true
    }
}
