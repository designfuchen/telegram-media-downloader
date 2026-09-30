"""for package download"""

import platform
import shutil
import subprocess
import sys


def reveal_path(target: str) -> tuple:
    """Open a folder in the OS file manager, cross-platform.

    Returns (ok, message). On a headless server (no GUI / no opener) this
    fails gracefully instead of raising, so Linux/Docker deployments do not
    crash on the macOS-only ``open`` command.
    """
    if not target:
        return False, "缺少路径"
    try:
        if sys.platform == "darwin":
            subprocess.Popen(["open", target], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return True, "已在访达中打开"
        if sys.platform.startswith("win"):
            import os  # pylint: disable=import-outside-toplevel

            os.startfile(target)  # type: ignore[attr-defined]  # noqa
            return True, "已在资源管理器中打开"
        opener = shutil.which("xdg-open")
        if opener:
            subprocess.Popen([opener, target], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return True, "已在文件管理器中打开"
        return False, "无头环境无法打开目录（Docker/NAS 请直接在文件管理器访问共享）"
    except OSError as exc:
        return False, f"无法打开目录：{exc}"

# def get_platform() -> str:
#     """Get platform title
#     Returns
#     -------
#     str
#         window amd64 return "windows-amd64"
#     """
#     sys_platform = platform.system().lower()
#     platform_str: str = sys_platform
#     if "macos" in sys_platform:
#         platform_str = "osx"

#     machine = platform.machine().lower()

#     if "i386" in machine:
#         platform_str += "-386"
#     else:
#         platform_str += "-" + machine

#     return platform_str


def get_exe_ext() -> str:
    """Get exe ext
    Returns
    str
        if in window then return "exe" other return ""
    """
    if "windows" in platform.system().lower():
        return ".exe"
    return ""
