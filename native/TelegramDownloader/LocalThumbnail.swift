import AVFoundation
import ImageIO
import UniformTypeIdentifiers

actor LocalThumbnail {
    static let shared = LocalThumbnail()

    func data(path: String, root: String) async -> Data? {
        let url = URL(filePath: path).resolvingSymlinksInPath()
        let directory = URL(filePath: root).resolvingSymlinksInPath().path
        guard !root.isEmpty, url.path.hasPrefix(directory + "/"),
              FileManager.default.fileExists(atPath: url.path) else { return nil }
        let ext = url.pathExtension.lowercased()
        var frame: CGImage?
        if ["jpg", "jpeg", "png", "webp", "heic", "gif"].contains(ext),
           let source = CGImageSourceCreateWithURL(url as CFURL, nil) {
            let options: [CFString: Any] = [kCGImageSourceCreateThumbnailFromImageAlways: true,
                                          kCGImageSourceThumbnailMaxPixelSize: 400,
                                          kCGImageSourceCreateThumbnailWithTransform: true]
            frame = CGImageSourceCreateThumbnailAtIndex(source, 0, options as CFDictionary)
        } else if ["mp4", "mov", "m4v"].contains(ext) {
            let generator = AVAssetImageGenerator(asset: AVURLAsset(url: url))
            generator.maximumSize = CGSize(width: 400, height: 240)
            generator.appliesPreferredTrackTransform = true
            frame = try? await generator.image(at: CMTime(seconds: 0, preferredTimescale: 600)).image
        }
        guard let frame else { return nil }
        let bytes = NSMutableData()
        guard let destination = CGImageDestinationCreateWithData(bytes, UTType.png.identifier as CFString, 1, nil) else { return nil }
        CGImageDestinationAddImage(destination, frame, nil)
        guard CGImageDestinationFinalize(destination) else { return nil }
        return bytes as Data
    }
}
