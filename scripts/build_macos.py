"""Build a relocatable Apple Silicon app with Python and optional node runtime."""
import argparse
import importlib.metadata
import shutil
from pathlib import Path
import subprocess
import sys

root=Path(__file__).resolve().parent.parent
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output',type=Path,required=True)
parser.add_argument('--sing-box',type=Path,required=True)
parser.add_argument('--codesign-identity',help='Developer ID Application identity; omit for local test builds')
args=parser.parse_args()
out=args.output.resolve();out.mkdir(parents=True,exist_ok=True)
helper = out / 'keychain-helper'
subprocess.run(['xcrun', 'swiftc', '-swift-version', '6', '-O', '-file-prefix-map', str(root)+'=/source', '-target', 'arm64-apple-macosx14.0', str(root/'native/KeychainHelper/main.swift'), '-o', str(helper)], check=True)
command=[sys.executable,'-m','PyInstaller','--noconfirm','--windowed','--onedir','--name','Telegram Downloader','--osx-bundle-identifier','org.telegramdownloader.desktop','--distpath',str(out/'dist'),'--workpath',str(out/'work'),'--specpath',str(out),'--collect-all','pyrogram','--collect-all','rich','--hidden-import','tgcrypto']
for source,target in [('config.example.yaml','.'),('module/templates','module/templates'),('module/static','module/static'),('LICENSE','.'),('THIRD_PARTY_NOTICES.md','.'),('docs/sing-box-LICENSE.txt','.')]:
 command+=['--add-data',str(root/source)+':'+target]
if args.codesign_identity:command+=['--codesign-identity',args.codesign_identity]
licenses=out/'licenses';licenses.mkdir(exist_ok=True)
for dist in importlib.metadata.distributions():
    name=dist.metadata.get('Name','dependency')
    target=licenses/name;target.mkdir(exist_ok=True)
    for item in dist.files or []:
        if 'license' in item.name.lower() or item.name.lower().startswith('copying'):
            source=Path(dist.locate_file(item))
            if source.is_file():shutil.copyfile(source,target/item.name)
    (target/'NOTICE.txt').write_text(dist.read_text('METADATA') or name)
for source in [Path(sys.base_prefix)/'LICENSE',Path(sys.base_prefix)/'LICENSE.txt',Path(sys.base_prefix)/'lib/python3.11/LICENSE.txt']:
    if source.is_file():shutil.copyfile(source,licenses/'Python-LICENSE.txt');break
command+=['--add-data',str(licenses)+':licenses','--add-data',str(root/'docs/lucide-LICENSE.txt')+':licenses']
command+=['--add-binary',str(args.sing_box.resolve())+':.', '--add-binary', str(helper)+':.', str(root/'desktop.py')]
subprocess.run(command,cwd=root,check=True)
