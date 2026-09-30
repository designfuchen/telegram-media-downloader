# Third-party notices

This project is a customized derivative of
[tangyoha/telegram_media_downloader](https://github.com/tangyoha/telegram_media_downloader).
The upstream MIT copyright and permission notice is preserved in `LICENSE`.
Existing upstream authorship remains in source and packaging metadata.

Bundled browser resources:

- [CryptoJS](https://github.com/brix/crypto-js), MIT; its original notice is preserved in `module/static/aes/crypto-js-master/LICENSE`.
- [Layui](https://github.com/layui/layui), MIT; its notice is preserved in `module/static/layui/LICENSE`.
- [Lucide](https://github.com/lucide-icons/lucide) line icons are embedded locally in the navigation. Its ISC license and inherited Feather MIT notice are preserved in `docs/lucide-LICENSE.txt`.

Python packages and optional external binaries are separate dependencies with
their own licenses. In particular, the customized
[Pyrogram](https://github.com/tangyoha/pyrogram) dependency has its own LGPL license;
the source revision used by this fork is recorded in `requirements.txt`.

The native macOS application and Docker image include the unmodified [sing-box v1.14.2](https://github.com/SagerNet/sing-box/tree/v1.14.2) executable from its official image as a separate process. Its license notice is preserved in `docs/sing-box-LICENSE.txt`; the corresponding upstream source is available at the versioned link above. It is governed by its own GPLv3-or-later terms and additional naming notice, rather than this project's MIT license.

Documentation artwork in `docs/assets/social-preview.png` was generated for this project. Interface captures use synthetic demonstration data and do not contain third-party channel media.
