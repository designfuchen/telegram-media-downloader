import SwiftUI

enum InterfaceStyle {
    static let pagePadding: CGFloat = 28
    static let sectionSpacing: CGFloat = 22
    static let cardPadding: CGFloat = 22
    static let cornerRadius: CGFloat = 16
    static let contentWidth: CGFloat = 920
}

struct PageHeading: View {
    let title: String
    let subtitle: String
    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(title).font(.title.bold()).accessibilityAddTraits(.isHeader)
            Text(subtitle).foregroundStyle(.secondary).fixedSize(horizontal: false, vertical: true)
        }
    }
}

struct SettingsCard<Content: View>: View {
    let title: String
    let subtitle: String
    @ViewBuilder let content: () -> Content
    init(_ title: String, subtitle: String = "", @ViewBuilder content: @escaping () -> Content) {
        self.title = title; self.subtitle = subtitle; self.content = content
    }
    var body: some View {
        VStack(alignment: .leading, spacing: 18) {
            if !title.isEmpty {
                VStack(alignment: .leading, spacing: 5) {
                    Text(title).font(.headline).accessibilityAddTraits(.isHeader)
                    if !subtitle.isEmpty { Text(subtitle).font(.callout).foregroundStyle(.secondary) }
                }
            }
            content()
        }
        .padding(InterfaceStyle.cardPadding).frame(maxWidth: .infinity, alignment: .leading)
        .background(.background, in: .rect(cornerRadius: InterfaceStyle.cornerRadius))
        .overlay(RoundedRectangle(cornerRadius: InterfaceStyle.cornerRadius).stroke(.quaternary.opacity(0.55), lineWidth: 1))
    }
}

struct StatusLabel: View {
    let text: String
    let active: Bool
    var body: some View {
        Label(text, systemImage: active ? "checkmark.circle.fill" : "circle.dotted")
            .font(.callout).foregroundStyle(active ? Color.green : Color.secondary)
            .padding(.horizontal, 10).padding(.vertical, 6)
            .background(active ? Color.green.opacity(0.08) : Color.secondary.opacity(0.07), in: .capsule)
    }
}

struct MediaChoice: View {
    let title: String
    let detail: String
    let symbol: String
    @Binding var selected: Bool
    var body: some View {
        Toggle(isOn: $selected) {
            HStack(spacing: 12) {
                Image(systemName: symbol).font(.title3).foregroundStyle(selected ? Color.indigo : Color.secondary)
                    .frame(width: 30)
                VStack(alignment: .leading, spacing: 4) {
                    Text(title).font(.body)
                    Text(detail).font(.callout).foregroundStyle(.secondary)
                }
            }
        }.toggleStyle(.checkbox)
            .padding(14).frame(maxWidth: .infinity, alignment: .leading)
            .background(selected ? Color.indigo.opacity(0.045) : Color.secondary.opacity(0.035), in: .rect(cornerRadius: 10))
    }
}
