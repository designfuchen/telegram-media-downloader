"""Self-contained macOS launcher; user data lives outside the application bundle."""
from pathlib import Path
import fcntl
import json
import os
import secrets
import signal
import socket
import sys
import threading
import time
import urllib.request
import webbrowser
import yaml


def open_console(port, token=''):
    url=f'http://127.0.0.1:{port}'
    for _ in range(200):
        try:
            with urllib.request.urlopen(url+'/login',timeout=1) as response:
                if response.status==200:
                    webbrowser.open(url+'/login#launch='+token if token else url)
                    return
        except OSError:time.sleep(.2)


def write_handshake(path, port):
    """Publish only the loopback port; never persist the launch token."""
    if not path:
        return
    descriptor=os.open(Path(path),os.O_WRONLY|os.O_CREAT|os.O_TRUNC|os.O_NOFOLLOW,0o600)
    os.fchmod(descriptor,0o600)
    with os.fdopen(descriptor,"w") as output:
        json.dump({"port":port},output)


def main():
    os.umask(0o077)
    bundle=Path(getattr(sys,'_MEIPASS',Path(__file__).resolve().parent))
    helper = bundle / 'keychain-helper'
    if helper.is_file():
        os.environ['TMD_KEYCHAIN_HELPER'] = str(helper)
    elif os.environ.get('TMD_REQUIRE_KEYCHAIN') == '1':
        raise RuntimeError('系统钥匙串组件缺失，请重新安装完整应用。')
    from module import config_io
    # The override exists for isolated smoke tests; no personal defaults ship.
    state=Path(os.environ.get('TMD_DESKTOP_DATA_DIR',Path.home()/'Library/Application Support/Telegram Downloader'))
    state.mkdir(parents=True,exist_ok=True,mode=0o700)
    (state/'log').mkdir(exist_ok=True,mode=0o700)
    # A windowed Python bundle has no standard streams. Initialize them before
    # importing account/bootstrap modules, which can initialize logging.
    if sys.stdout is None:sys.stdout=(state/'log/launcher.log').open('a',buffering=1)
    if sys.stderr is None:sys.stderr=sys.stdout
    lock=(state/'app.lock').open('a')
    cfg_path=state/'config.yaml'
    try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:
        cfg=config_io.load(cfg_path, yaml)
        if os.environ.get("TMD_NO_BROWSER") != "1":
            webbrowser.open(f"http://127.0.0.1:{cfg.get('web_port',5002)}")
        return
    if not cfg_path.exists():
        cfg=yaml.safe_load((bundle/'config.example.yaml').read_text())
        bootstrap=state/'bootstrap.json'
        if bootstrap.exists():
            from module.account_management import INHERITED_SETTINGS
            inherited=json.loads(config_io.decode(bootstrap.read_bytes(), bootstrap))
            for key in INHERITED_SETTINGS | {'save_path', 'start_paused'}:
                if key in inherited:cfg[key]=inherited[key]
        cfg['web_login_secret']=secrets.token_urlsafe(32)
        if not bootstrap.exists():cfg['save_path']=str(Path.home()/'Downloads/Telegram')
        cfg['web_host']='127.0.0.1'
        config_io.dump(cfg_path, cfg, yaml)
        if bootstrap.exists():bootstrap.unlink()
    cfg=config_io.load(cfg_path, yaml)
    cfg['web_host']='127.0.0.1'
    with socket.socket() as probe:
        probe.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        try:probe.bind(('127.0.0.1',int(cfg.get('web_port',5002))))
        except OSError:probe.bind(('127.0.0.1',0))
        cfg['web_port']=probe.getsockname()[1]
    config_io.dump(cfg_path, cfg, yaml)
    for name in ['sessions','log','temp','dbdata','output']:(state/name).mkdir(exist_ok=True,mode=0o700)
    Path(cfg['save_path']).expanduser().mkdir(parents=True,exist_ok=True)
    os.environ['TMD_TASK_DB']=str(state/'dbdata/downloads.db')
    os.environ['TMD_HISTORY_DB']=os.environ['TMD_TASK_DB']
    os.environ['TMD_DESKTOP']='1'
    token=os.environ.get("TMD_PARENT_LAUNCH_TOKEN") or secrets.token_urlsafe(32)
    if len(token) < 32:raise ValueError("Invalid local launch token")
    write_handshake(os.environ.get("TMD_DESKTOP_HANDSHAKE"),cfg["web_port"])
    os.environ['TMD_LOCAL_LAUNCH_TOKEN']=token
    if (bundle/'sing-box').exists():os.environ['TMD_SINGBOX_BINARY']=str(bundle/'sing-box')
    os.chdir(state)
    def interrupt(*_):raise KeyboardInterrupt
    signal.signal(signal.SIGTERM,interrupt)
    if os.environ.get('TMD_NO_BROWSER')!='1':
        threading.Thread(target=open_console,args=(cfg['web_port'],token),daemon=True).start()
    from module.secure_session import install
    install()
    import media_downloader
    if media_downloader._check_config():media_downloader.main()
    lock.close()
    if getattr(media_downloader.app,"desktop_restart_requested",False):
        os.execv(sys.executable,[sys.executable]+sys.argv[1:] if getattr(sys,"frozen",False) else [sys.executable]+sys.argv)


if __name__=='__main__':main()
