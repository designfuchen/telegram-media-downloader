import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
from desktop import write_handshake


class DesktopBridgeTests(unittest.TestCase):
    def test_private_port_only_handshake(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'bridge.json'
            write_handshake(path, 50123)
            self.assertEqual(json.loads(path.read_text()), {'port': 50123})
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            path.chmod(0o644)
            write_handshake(path, 50124)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

    def test_symlink_is_rejected_without_overwriting_target(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)/'keep.txt'
            target.write_text('keep')
            link = Path(directory)/'bridge.json'
            link.symlink_to(target)
            with self.assertRaises(OSError):
                write_handshake(link, 50123)
            self.assertEqual(target.read_text(), 'keep')

    def test_no_handshake_is_optional(self):
        write_handshake(None, 50123)
