import Foundation

@main struct NetworkGateTests {
    @MainActor static func main() async {
        let model = AppModel()
        assert(!model.networkVerified)
        await model.login()
        assert(model.error == "请先完成网络连接测试。")
        model.error = nil
        await model.resendCode()
        assert(model.error == "请先完成网络连接测试。")
        model.networkVerified = true
        model.networkMode = "link"
        assert(!model.networkVerified)
        model.networkVerified = true
        model.nodeContent = "changed test content"
        assert(!model.networkVerified)
        model.networkVerified = true
        model.nodeIndex = 1
        assert(!model.networkVerified)
        // An unchanged field should not discard a successful test.
        model.networkVerified = true
        model.nodeIndex = 1
        assert(model.networkVerified)
        print("Native network gate tests passed")
    }
}
