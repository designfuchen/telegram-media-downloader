import SwiftUI
import AppKit

@main
struct TelegramDownloaderApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) private var delegate
    @State private var model = AppModel()

    var body: some Scene {
        Window("Telegram 下载器", id: "main") {
            WorkspaceView(model: model)
                .frame(minWidth: 880, minHeight: 640)
                .tint(.indigo)
        }
        .defaultSize(width: 1080, height: 760)
        .windowStyle(.hiddenTitleBar)
        .commands {
            CommandGroup(replacing: .newItem) { }
            CommandGroup(after: .appInfo) {
                Button("显示保存文件夹") {
                    NSWorkspace.shared.open(URL(filePath: model.savePath))
                }.keyboardShortcut("o", modifiers: [.command, .shift])
            }
        }
    }
}
