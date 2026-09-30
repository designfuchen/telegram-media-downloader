"""Build a source-only release. Never include Git history or runtime data."""
import argparse
import hashlib
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import zipfile

ROOT = Path(__file__).resolve().parent.parent
ROOT_FILES = {'.gitignore', '.dockerignore', 'Dockerfile', 'LICENSE', 'README.md',
              'README_CN.md', 'README_EN.md', 'CONTRIBUTING.md', 'CHANGELOG.md', 'SECURITY.md', 'THIRD_PARTY_NOTICES.md',
              'config.example.yaml', 'docker-compose.yaml', 'requirements.txt',
              'requirements-lock.txt', 'setup.py', 'desktop.py', 'media_downloader.py', 'start.command'}
SOURCE_DIRS = {'native', 'module', 'utils', 'scripts', 'tests', 'docs', '.github'}
SKIP_PARTS = {'__pycache__', '.pytest_cache', '.venv', 'node_modules', '.git'}
SKIP_SUFFIXES = {'.pyc', '.pyo', '.DS_Store'}


def build(output):
    output = output.resolve()
    if output.exists():
        raise ValueError('输出已存在，请选择新文件名，避免覆盖。')
    with tempfile.TemporaryDirectory(prefix='tmd-source-release-') as folder:
        target = Path(folder) / 'telegram-downloader-source'
        target.mkdir()
        for source in sorted(ROOT.rglob('*')):
            relative = source.relative_to(ROOT)
            if relative.parts[0] not in SOURCE_DIRS and str(relative) not in ROOT_FILES:
                continue
            if any(part in SKIP_PARTS for part in relative.parts) or source.suffix in SKIP_SUFFIXES or source.name == '.DS_Store':
                continue
            if source.is_symlink():
                raise ValueError('源码中存在符号链接：' + str(relative))
            if not source.is_file():
                continue
            destination = target / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
            destination.chmod(0o755 if relative.name == 'start.command' else 0o644)
        subprocess.run([sys.executable, str(target/'scripts/check_public_files.py'), '--directory'], cwd=target, check=True)
        rows = []
        for file in sorted(target.rglob('*')):
            if file.is_file():
                rows.append(hashlib.sha256(file.read_bytes()).hexdigest() + '  ' + file.relative_to(target).as_posix())
        (target/'MANIFEST.sha256').write_text('\n'.join(rows)+'\n')
        output.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(output, 'x', compression=zipfile.ZIP_DEFLATED) as archive:
            for file in sorted(target.rglob('*')):
                if file.is_file():
                    archive.write(file, file.relative_to(target.parent))
    checksum = hashlib.sha256(output.read_bytes()).hexdigest()
    output.with_suffix(output.suffix+'.sha256').write_text(checksum+'  '+output.name+'\n')
    print('源码包已生成：'+str(output))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    build(args.output)
