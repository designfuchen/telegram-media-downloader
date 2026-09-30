"""Reject private runtime files and high-confidence credential patterns."""
import argparse
import fnmatch
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parent.parent
DENIED = ('config.yaml', 'data.yaml', '.env', '.env.*', '*.session*', '*.db*',
          '*.sqlite*', '*.csv', '*.log', '*.key', '*.pem', '*.p12', '*.pfx',
          '*.pyc', '.DS_Store', '*.bak*', '*.zip', '*.tar*', 'CLAUDE*.md', 'FIX-*.md')
PUBLIC_TEMPLATES = {"docs/templates/channels.csv", "docs/templates/invites.csv"}
PRIVATE_DIRS = {'sessions', 'dbdata', 'downloads', 'output', 'log', 'logs',
                'temp', 'tmp', 'private-config', '.private-publication', '.claude', '.venv', '__pycache__'}
PATTERNS = {
    'private-key': r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----',
    'github-token': r'\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,})\b',
    'telegram-bot-token': r'\b\d{7,12}:[A-Za-z0-9_-]{30,}\b',
    'private-invite': r'https?://t\.me/(?:\+|joinchat/)[A-Za-z0-9_-]{10,}',
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', action='store_true', help='Check extracted source without Git')
    args = parser.parse_args()
    if args.directory:
        paths = [p.relative_to(ROOT) for p in ROOT.rglob('*') if p.is_file() or p.is_symlink()]
    else:
        result = subprocess.run(['git', 'ls-files', '--cached', '--others', '--exclude-standard', '-z'], cwd=ROOT, capture_output=True)
        if result.returncode or not result.stdout:
            raise SystemExit('未找到可检查的源码文件。')
        paths = [Path(p) for p in result.stdout.decode().split('\0') if p]
    failures = []
    for relative in paths:
        if relative.as_posix() not in PUBLIC_TEMPLATES and (any(part in PRIVATE_DIRS for part in relative.parts) or any(
            fnmatch.fnmatch(relative.name, pattern) for pattern in DENIED
        )):
            failures.append((relative, 'private-runtime-file'))
        p = ROOT / relative
        if not p.exists() or p.is_symlink():
            failures.append((relative, 'missing-or-symlink'))
            continue
        data = p.read_bytes()
        if b'\0' in data:
            continue
        text = data.decode('utf-8', errors='replace')
        for name, pattern in PATTERNS.items():
            if re.search(pattern, text):
                failures.append((relative, name))
    for path, kind in failures:
        print(f'{path}: {kind}')  # Never print credential values.
    if failures:
        raise SystemExit(1)
    print(f'公开文件检查通过：{len(paths)} 个文件。')


if __name__ == '__main__':
    main()
