"""Pyrogram SQLite lives in RAM; complete session files are authenticated ciphertext."""
import sqlite3
import time
from pyrogram.storage.file_storage import FileStorage
from module import config_io, secret_storage
from utils.crypto import PREFIX

class ProtectedSessionStorage(FileStorage):
    async def open(self):
        self.conn = sqlite3.connect(":memory:", check_same_thread=False)
        try:
            if self.database.exists():
                data = self.database.read_bytes()
                if data.startswith(PREFIX):
                    snapshot = bytearray(secret_storage.unseal(data, self.database))
                    # SQLite deserialize requires a non-WAL image. Authentication
                    # has already succeeded; only the journal-mode header changes.
                    snapshot[18:20] = b"\x01\x01"
                    self.conn.deserialize(bytes(snapshot))
                elif data.startswith(b"SQLite format 3\x00"):
                    source = sqlite3.connect(self.database.resolve().as_uri() + "?mode=ro", uri=True)
                    try:
                        source.backup(self.conn)
                    finally:
                        source.close()
                else:
                    raise ValueError("登录会话格式无效，请保留原文件并检查安装。")
                self.update()
            else:
                self.create()
            self.conn.execute("PRAGMA journal_mode=MEMORY")
            self._persist()
            for suffix in ("-wal", "-shm", "-journal"):
                self.database.with_name(self.database.name + suffix).unlink(missing_ok=True)
        except BaseException:
            self.conn.close()
            raise

    def _persist(self):
        self.conn.commit()
        config_io.atomic_write(self.database, secret_storage.seal(self.conn.serialize(), self.database))

    def _accessor(self, value=object):
        if value == object:
            return self._get()
        self._set(value)
        self._persist()

    async def save(self):
        await self.date(int(time.time()))

    async def close(self):
        self._persist()
        self.conn.close()

    async def update_peers(self, peers):
        await super().update_peers(peers)
        self._persist()

    async def update_state(self, value=object):
        result = await super().update_state(value)
        if value != object:
            self._persist()
        return result

def install():
    if secret_storage.enabled():
        import pyrogram.client
        pyrogram.client.FileStorage = ProtectedSessionStorage
