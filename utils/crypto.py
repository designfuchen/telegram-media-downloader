"""Authenticated local encryption. Transport security is provided by TLS."""
import secrets
from Crypto.Cipher import AES

PREFIX = b"TMD-GCM1\x00"

def encrypt(data: bytes, key: bytes, context: bytes) -> bytes:
    cipher = AES.new(key, AES.MODE_GCM, nonce=secrets.token_bytes(12), mac_len=16)
    cipher.update(context)
    ciphertext, tag = cipher.encrypt_and_digest(data)
    return PREFIX + cipher.nonce + tag + ciphertext

def decrypt(data: bytes, key: bytes, context: bytes) -> bytes:
    if not data.startswith(PREFIX) or len(data) < len(PREFIX) + 28:
        raise ValueError("受保护数据格式不正确。")
    offset = len(PREFIX)
    cipher = AES.new(key, AES.MODE_GCM, nonce=data[offset:offset + 12], mac_len=16)
    cipher.update(context)
    return cipher.decrypt_and_verify(data[offset + 28:], data[offset + 12:offset + 28])
