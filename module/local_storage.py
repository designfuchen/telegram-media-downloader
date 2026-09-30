"""Native folder selection for a downloader running on the same Mac."""
import os
from pathlib import Path
import subprocess
import sys
import threading

_picker_lock = threading.Lock()


def native_picker_available():
    return sys.platform == 'darwin' and Path('/usr/bin/osascript').is_file()


def choose_folder():
    """Only return a user-selected existing directory; never save configuration."""
    if not native_picker_available():
        raise ValueError('下载器未在本机 Mac 上运行，请填写运行设备上的保存路径。')
    if not _picker_lock.acquire(blocking=False):
        raise ValueError('文件夹选择窗口已经打开，请先完成或取消。')
    try:
        default = Path.home() / 'Downloads'
        if not default.is_dir():
            default = Path.home()
        script = '''on run argv
activate
set chosenFolder to choose folder with prompt "选择 Telegram 下载保存文件夹" default location (POSIX file (item 1 of argv))
return POSIX path of chosenFolder
end run'''
        result = subprocess.run(
            ['/usr/bin/osascript', '-e', script, str(default)],
            capture_output=True, text=True, timeout=120, check=False,
        )
        if result.returncode:
            if '(-128)' in result.stderr:
                return None
            raise ValueError('无法打开文件夹选择窗口，请手动填写路径。')
        path = Path(result.stdout.strip()).expanduser()
        if not path.is_absolute() or not path.is_dir():
            raise ValueError('请选择一个已存在的文件夹。')
        return str(path.resolve())
    except (OSError, subprocess.SubprocessError) as error:
        raise ValueError('选择窗口已超时或不可用，请重试或手动填写路径。') from error
    finally:
        _picker_lock.release()
