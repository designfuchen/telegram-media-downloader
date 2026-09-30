# Telegram Downloader

**Bring your channel content home to your Mac.**

A native SwiftUI interface for organizing Telegram photos, videos, audio, files and text on your own device. Based on [tangyoha/telegram_media_downloader](https://github.com/tangyoha/telegram_media_downloader).

[![Checks](https://github.com/designfuchen/telegram-media-downloader/actions/workflows/checks.yml/badge.svg)](https://github.com/designfuchen/telegram-media-downloader/actions/workflows/checks.yml)
[![MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

![Workflow demo](docs/assets/workflow.gif)

The 15-second interface walkthrough uses synthetic channel and file data; it is not a speed benchmark. The current UI and detailed documentation are in Chinese.

## Highlights

- Five-step setup: API credentials, tested network connection, account login, channel, and destination folder.
- Separate media choices, including text-only downloads; organize folders by channel, month and media type.
- Channel links, usernames, numeric IDs and CSV import with a template and review step.
- Unified common node-link, Clash / Mihomo YAML, JSON and Base64 import; test before continuing.
- Progress, pause/resume, local thumbnails, persistent queue and retry/backoff.
- Independent account profiles; local Keychain-backed encrypted configuration and sessions on macOS.

## Availability

Native builds target Apple Silicon and macOS 14+. The bundled application includes Python and sing-box. **[Download the Mac DMG](https://github.com/designfuchen/telegram-media-downloader/releases/download/v0.1.5/Telegram-Downloader-0.1.5-arm64.dmg)** and drag the app into Applications.

The application is Developer ID signed, notarized by Apple, and has a validated stapled ticket. The DMG is an unsigned distribution wrapper containing that approved application; an app ZIP is also available in [Releases](https://github.com/designfuchen/telegram-media-downloader/releases/tag/v0.1.5). Container integrity, mounting, copying, strict app signatures, the ticket and system pre-distribution checks passed locally. See the [release audit](docs/release-audit.md) for environment limitations.

Source and Docker instructions are in the [main README](README.md#安装与运行). Python 3.11 and a securely saved `TMD_SECRET_KEY` are required for source execution. Native build instructions: [docs/macos.md](docs/macos.md).

184 local regression tests passed. CI runs tests, publication checks and Gitleaks scans. See [release audit](docs/release-audit.md) for evidence and limitations.

Only archive content your account is authorized to access and save. No developer accounts, sessions, proxy nodes, private channels or downloaded content are included. Report vulnerabilities according to [SECURITY.md](SECURITY.md), and contribute using [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Project code is MIT; upstream notices are retained. Bundled dependencies have their own licenses, including LGPL and GPL components: [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md). This project is not affiliated with Telegram.
