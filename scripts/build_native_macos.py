"""Build, sign, notarize and package the native macOS application."""
import argparse
import hashlib
from pathlib import Path
import plistlib
import runpy
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
MACHO = {b"\xfe\xed\xfa\xce", b"\xce\xfa\xed\xfe", b"\xfe\xed\xfa\xcf", b"\xcf\xfa\xed\xfe", b"\xca\xfe\xba\xbe", b"\xbe\xba\xfe\xca"}

def run(arguments):
    subprocess.run(list(map(str, arguments)), check=True)

def signing_command(path, identity, engine=False):
    command = ['codesign', '--force', '--sign', identity]
    if identity != '-':
        command += ['--timestamp', '--options', 'runtime']
        if engine:
            command += ['--entitlements', ROOT/'native/engine.entitlements']
    return command + [path]

def sign_bundle(app, identity):
    # Sign code from the inside out. --deep is used for verification only.
    for file in sorted(app.rglob('*'), key=lambda p: len(p.parts), reverse=True):
        if file.is_symlink() or not file.is_file():
            continue
        with file.open('rb') as handle:
            magic = handle.read(4)
        if magic in MACHO:
            run(signing_command(file, identity, file.name == 'Telegram Downloader'))
    for framework in sorted(app.rglob('*.framework'), key=lambda p: len(p.parts), reverse=True):
        if not framework.is_symlink():
            run(signing_command(framework, identity))
    run(signing_command(app, identity))
    run(['codesign', '--verify', '--deep', '--strict', app])

def notarize(path, profile):
    run(['xcrun', 'notarytool', 'submit', path, '--keychain-profile', profile, '--wait'])

def build(args):
    if args.release and (not args.codesign_identity or not args.codesign_identity.startswith('Developer ID Application:') or not args.notary_profile):
        raise ValueError('正式发布需要 Developer ID Application 证书和钥匙串中的 notarytool 凭证。')
    if args.notary_profile and (not args.codesign_identity or not args.codesign_identity.startswith('Developer ID Application:')):
        raise ValueError('公证必须指定 Developer ID Application 签名证书。')
    out = args.output.resolve()
    if out.exists():
        raise ValueError('输出目录已存在，请选择新目录。')
    version = runpy.run_path(str(ROOT/'utils/__init__.py'))
    out.mkdir(parents=True)
    identity = args.codesign_identity or '-'
    backend = [sys.executable, ROOT/'scripts/build_macos.py', '--output', out/'backend', '--sing-box', args.sing_box.resolve()]
    if identity != '-':
        backend += ['--codesign-identity', identity]
    run(backend)
    app = out/'Telegram Downloader.app'
    mac, resources = app/'Contents/MacOS', app/'Contents/Resources'
    mac.mkdir(parents=True)
    resources.mkdir()
    shutil.copytree(out/'backend/dist/Telegram Downloader', resources/'Engine', symlinks=True)
    run(['xcrun', 'swiftc', '-swift-version', '6', '-O', '-parse-as-library', '-file-prefix-map', str(ROOT)+'=/source', '-target', 'arm64-apple-macosx14.0', *sorted((ROOT/'native/TelegramDownloader').glob('*.swift')), '-o', mac/'TelegramDownloader'])
    info = {'CFBundleName':'Telegram Downloader', 'CFBundleDisplayName':'Telegram 下载器', 'CFBundleIdentifier':'org.telegramdownloader.native', 'CFBundleExecutable':'TelegramDownloader', 'CFBundlePackageType':'APPL', 'CFBundleShortVersionString':version['__version__'], 'CFBundleVersion':version['__build__'], 'LSMinimumSystemVersion':'14.0', 'NSHighResolutionCapable':True, 'NSPrincipalClass':'NSApplication', 'CFBundleIconFile':'AppIcon'}
    (app/'Contents/Info.plist').write_bytes(plistlib.dumps(info))
    iconset = out/'AppIcon.iconset'
    run(['xcrun', 'swift', ROOT/'scripts/make_macos_icon.swift', iconset])
    run(['iconutil', '-c', 'icns', iconset, '-o', resources/'AppIcon.icns'])
    for name in ['LICENSE','THIRD_PARTY_NOTICES.md']:
        shutil.copyfile(ROOT/name, resources/name)
    sign_bundle(app, identity)
    if args.notary_profile:
        archive = out/'app-for-notarization.zip'
        run(['ditto', '-c', '-k', '--keepParent', app, archive])
        notarize(archive, args.notary_profile)
        run(['xcrun', 'stapler', 'staple', app])
        run(['xcrun', 'stapler', 'validate', app])
        run(['spctl', '--assess', '--type', 'execute', '--verbose', app])
    stage = out/'dmg-stage'
    stage.mkdir()
    run(['ditto', app, stage/app.name])
    (stage/'Applications').symlink_to('/Applications')
    dmg = out/f"Telegram-Downloader-{version['__version__']}-arm64.dmg"
    run(['hdiutil', 'create', '-volname', 'Telegram Downloader', '-srcfolder', stage, '-format', 'UDZO', dmg])
    if identity != '-':
        run(['codesign', '--timestamp', '--sign', identity, dmg])
    if args.notary_profile:
        notarize(dmg, args.notary_profile)
        run(['xcrun','stapler','staple',dmg])
        run(['xcrun','stapler','validate',dmg])
    dmg.with_suffix('.dmg.sha256').write_text(hashlib.sha256(dmg.read_bytes()).hexdigest()+'  '+dmg.name+'\n')
    status = 'signed-and-notarized' if args.notary_profile else 'not-notarized'
    (out/'BUILD-STATUS.txt').write_text(f"Version: {version['__version__']}\nDistribution: {status}\n")
    print(app)
    print(dmg)
    if status != 'signed-and-notarized':
        print('此包尚未完成正式签名与公证，不满足面向公众分发的验收条件。')

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--sing-box', type=Path, required=True)
    parser.add_argument('--codesign-identity')
    parser.add_argument('--notary-profile', help='notarytool credential profile stored in Keychain')
    parser.add_argument('--release', action='store_true', help='require distribution signing and notarization')
    args = parser.parse_args()
    try:
        build(args)
    except ValueError as error:
        parser.error(str(error))

if __name__ == '__main__':
    main()
