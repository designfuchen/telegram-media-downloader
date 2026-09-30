"""Open the local console automatically, without exposing the access password."""
from pathlib import Path
import os
import secrets
import subprocess
import sys
import threading
import time
import urllib.request
import webbrowser
import yaml


def open_when_ready(host, port, token):
    address = '127.0.0.1' if host in {'127.0.0.1', 'localhost', '0.0.0.0'} else host
    url = f'http://{address}:{port}'
    for _ in range(150):
        try:
            with urllib.request.urlopen(url + '/login', timeout=1) as response:
                if response.status == 200:
                    webbrowser.open(url + '/login#launch=' + token)
                    return
        except OSError:
            time.sleep(.2)


def main():
    root = Path(__file__).resolve().parent.parent
    os.chdir(root)
    if not (root/'config.yaml').exists():
        subprocess.run([sys.executable, str(root/'scripts/init_config.py')], check=True)
    sys.path.insert(0, str(root))
    from module import config_io
    cfg = config_io.load(root/'config.yaml', yaml)
    local = cfg.get('web_host') in {'127.0.0.1','localhost'}
    token = secrets.token_urlsafe(32) if local else ''
    env = dict(os.environ)
    if local:
        env['TMD_LOCAL_LAUNCH_TOKEN'] = token
    child = subprocess.Popen([sys.executable, str(root/'media_downloader.py')], env=env)
    if local:
        threading.Thread(target=open_when_ready, args=(cfg['web_host'],cfg.get('web_port',5002),token),daemon=True).start()
    try:
        child.wait()
    except KeyboardInterrupt:
        try:child.wait(timeout=15)
        except subprocess.TimeoutExpired:child.terminate()


if __name__ == '__main__':main()
