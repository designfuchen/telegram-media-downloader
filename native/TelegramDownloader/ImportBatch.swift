import Foundation

struct ImportBatch: Identifiable {
    let id: Int
    let name: String
    let status: String
    let total: Int
    let valid: Int
    let pending: Int
    let invalid: Int
    let imported: Int
    init(_ row: [String: Any]) {
        id = row["id"] as? Int ?? 0
        name = row["source_name"] as? String ?? "频道导入"
        status = row["status"] as? String ?? "validating"
        total = row["total_count"] as? Int ?? 0
        valid = row["valid_count"] as? Int ?? 0
        pending = row["pending_count"] as? Int ?? 0
        invalid = row["invalid_count"] as? Int ?? 0
        imported = row["imported_count"] as? Int ?? 0
    }
}
