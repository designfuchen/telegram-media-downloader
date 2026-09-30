import Foundation
import Security

// Commands and keys travel through pipes, never process arguments or logs.
let input = FileHandle.standardInput.readDataToEndOfFile()
guard let request = try? JSONSerialization.jsonObject(with: input) as? [String: Any],
      let account = request["account"] as? String,
      account.hasPrefix("vault-"), account.count == 70 else { exit(2) }
let query: [String: Any] = [
    kSecClass as String: kSecClassGenericPassword,
    kSecAttrService as String: "org.telegramdownloader.native.vault",
    kSecAttrAccount as String: account,
    kSecReturnData as String: true,
    kSecMatchLimit as String: kSecMatchLimitOne
]
var item: CFTypeRef?
var status = SecItemCopyMatching(query as CFDictionary, &item)
if status == errSecItemNotFound && request["create"] as? Bool == true {
    var bytes = [UInt8](repeating: 0, count: 32)
    guard SecRandomCopyBytes(kSecRandomDefault, bytes.count, &bytes) == errSecSuccess else { exit(3) }
    var attributes = query
    attributes.removeValue(forKey: kSecReturnData as String)
    attributes.removeValue(forKey: kSecMatchLimit as String)
    attributes[kSecValueData as String] = Data(bytes)
    attributes[kSecAttrAccessible as String] = kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
    let added = SecItemAdd(attributes as CFDictionary, nil)
    guard added == errSecSuccess || added == errSecDuplicateItem else { exit(4) }
    status = SecItemCopyMatching(query as CFDictionary, &item)
}
guard status == errSecSuccess, let key = item as? Data, key.count == 32 else { exit(5) }
FileHandle.standardOutput.write(Data(key.base64EncodedString().utf8))
