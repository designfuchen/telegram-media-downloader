import AppKit

// Vector artwork, rasterized at the sizes required by an ICNS bundle.
let directory = URL(fileURLWithPath: CommandLine.arguments[1], isDirectory: true)
try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
for points in [16, 32, 128, 256, 512] {
    for scale in [1, 2] {
        let size = points * scale
        let image = NSImage(size: NSSize(width: size, height: size))
        image.lockFocus()
        let side = CGFloat(size)
        let rect = NSRect(x: side * 0.08, y: side * 0.08, width: side * 0.84, height: side * 0.84)
        let shape = NSBezierPath(roundedRect: rect, xRadius: side * 0.19, yRadius: side * 0.19)
        NSColor(calibratedRed: 0.24, green: 0.31, blue: 0.82, alpha: 1).setFill()
        shape.fill()
        let stroke = NSBezierPath()
        stroke.lineWidth = side * 0.065
        stroke.lineCapStyle = .round
        stroke.lineJoinStyle = .round
        stroke.move(to: NSPoint(x: side * 0.5, y: side * 0.72))
        stroke.line(to: NSPoint(x: side * 0.5, y: side * 0.40))
        stroke.move(to: NSPoint(x: side * 0.36, y: side * 0.53))
        stroke.line(to: NSPoint(x: side * 0.5, y: side * 0.39))
        stroke.line(to: NSPoint(x: side * 0.64, y: side * 0.53))
        stroke.move(to: NSPoint(x: side * 0.32, y: side * 0.29))
        stroke.line(to: NSPoint(x: side * 0.68, y: side * 0.29))
        NSColor.white.setStroke()
        stroke.stroke()
        image.unlockFocus()
        let bitmap = NSBitmapImageRep(data: image.tiffRepresentation!)!
        let suffix = scale == 2 ? "@2x" : ""
        try bitmap.representation(using: .png, properties: [:])!.write(to: directory.appendingPathComponent("icon_\(points)x\(points)\(suffix).png"))
    }
}
