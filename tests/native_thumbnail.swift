import Foundation
import CoreGraphics
import ImageIO
import UniformTypeIdentifiers

@main struct ThumbnailTests {
    static func main() async throws {
        let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let allowed = root.appending(path: "downloads")
        try FileManager.default.createDirectory(at: allowed, withIntermediateDirectories: true)
        let file = allowed.appending(path: "sample.png")
        let context = CGContext(data: nil, width: 1000, height: 800, bitsPerComponent: 8, bytesPerRow: 0,
                                space: CGColorSpaceCreateDeviceRGB(), bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue)!
        context.setFillColor(CGColor(red: 0.2, green: 0.3, blue: 0.8, alpha: 1))
        context.fill(CGRect(x: 0, y: 0, width: 1000, height: 800))
        let writer = CGImageDestinationCreateWithURL(file as CFURL, UTType.png.identifier as CFString, 1, nil)!
        CGImageDestinationAddImage(writer, context.makeImage()!, nil)
        assert(CGImageDestinationFinalize(writer))
        let result = await LocalThumbnail.shared.data(path: file.path, root: allowed.path)
        assert(result != nil)
        let decoded = CGImageSourceCreateWithData(result! as CFData, nil)!
        let image = CGImageSourceCreateImageAtIndex(decoded, 0, nil)!
        assert(image.width <= 400 && image.height <= 400)
        let forbidden = root.appending(path: "private.png")
        try FileManager.default.copyItem(at: file, to: forbidden)
        let link = allowed.appending(path: "link.png")
        try FileManager.default.createSymbolicLink(at: link, withDestinationURL: forbidden)
        let outside = await LocalThumbnail.shared.data(path: forbidden.path, root: allowed.path)
        let symlink = await LocalThumbnail.shared.data(path: link.path, root: allowed.path)
        assert(outside == nil && symlink == nil)
        print("Native thumbnail downsampling and path boundary tests passed")
    }
}
