"""Telegram authorization on the client's event loop; codes never reach disk."""
import asyncio
import math
import re
import time
from pyrogram import errors, types


class BrowserLogin:
    def __init__(self, client):
        self.client = client
        self.stage = 'connecting'
        self.message = '正在连接 Telegram…'
        self.phone = None
        self.code_hash = None
        self.result = None
        self.lock = asyncio.Lock()
        self.retry_at = 0
        self.resend_at = 0

    def status(self):
        return {'stage': self.stage, 'message': self.message,
                'retry_after': max(0, math.ceil(self.retry_at - time.monotonic())),
                'resend_after': max(0, math.ceil(max(self.retry_at, self.resend_at) - time.monotonic()))}

    async def authorize(self):
        self.result = asyncio.get_running_loop().create_future()
        self.stage, self.message = 'phone', '输入 Telegram 账号的手机号，包含国家区号。'
        return await self.result

    async def submit(self, action, value):
        if self.lock.locked():
            return {'ok': False, 'message': '上一项操作仍在进行，请稍等。'}
        async with self.lock:
            if time.monotonic() < self.retry_at:
                return {'ok': False, 'message': 'Telegram 要求稍后重试，请等待倒计时结束。', **self.status()}
            if self.result is None or self.result.done():
                return {'ok': False, 'message': '登录尚未准备好，请稍等。'}
            try:
                if action == 'resend' and self.stage == 'code':
                    if time.monotonic() < self.resend_at:
                        return {'ok': False, **self.status(), 'message': '请等待倒计时结束后再重新发送。'}
                    sent = await self.client.resend_code(self.phone, self.code_hash)
                    self.code_hash = sent.phone_code_hash
                    self.resend_at = time.monotonic() + max(int(getattr(sent, 'timeout', None) or 60), 1)
                    self.message = '已重新发送，请查看 Telegram 官方消息或 Telegram 指定的接收方式，并填写最新验证码。'
                    return {'ok': True, **self.status()}
                if action == 'phone' and self.stage in {'phone', 'code'}:
                    if time.monotonic() < self.resend_at:
                        return {'ok': False, **self.status(), 'message': '请等待倒计时结束后再获取验证码。'}
                    phone = re.sub(r'[\s()-]', '', value)
                    if not re.fullmatch(r'\+[1-9]\d{6,14}', phone):
                        return {'ok': False, 'message': '请填写含国家区号的完整手机号，例如 +86 后接手机号。'}
                    sent = await self.client.send_code(phone)
                    self.phone, self.code_hash = phone, sent.phone_code_hash
                    self.resend_at = time.monotonic() + max(int(getattr(sent, "timeout", None) or 60), 1)
                    self.stage, self.message = 'code', '验证码已发送，请先查看 Telegram 官方消息，也可能通过短信收到。'
                    return {'ok': True, **self.status()}
                if action == 'code' and self.stage == 'code':
                    code = re.sub(r'\s', '', value)
                    if not re.fullmatch(r'\d{3,10}', code):
                        return {'ok': False, 'message': '请输入刚收到的数字验证码。'}
                    user = await self.client.sign_in(self.phone, self.code_hash, code)
                elif action == 'password' and self.stage == 'password':
                    user = await self.client.check_password(value)
                else:
                    return {'ok': False, 'message': '请按页面提示完成当前步骤。'}
                if not isinstance(user, types.User):
                    self.stage, self.message = 'phone', '该手机号尚未完成注册，请先在 Telegram 官方客户端注册。'
                    self.phone = self.code_hash = None
                    return {'ok': False, **self.status()}
                self.stage, self.message = 'finishing', '验证通过，正在准备下载服务…'
                self.phone = self.code_hash = None
                self.result.set_result(user)
                return {'ok': True, **self.status()}
            except errors.SessionPasswordNeeded:
                self.stage, self.message = 'password', '此账号开启了两步验证，请输入你在 Telegram 设置的密码。'
                return {'ok': True, **self.status()}
            except errors.FloodWait as error:
                self.retry_at = time.monotonic() + max(int(error.value), 1)
                self.message = '操作较频繁，请稍后再试。'
            except errors.PhoneCodeExpired:
                self.stage, self.message = 'phone', '验证码已过期，请重新输入手机号获取。'
                self.phone = self.code_hash = None
            except errors.PhoneCodeInvalid:
                self.message = '验证码不正确，请查看最新收到的验证码。'
            except errors.PasswordHashInvalid:
                self.message = '两步验证密码不正确，请重新输入。'
            except errors.ApiIdInvalid:
                self.message = '应用编号或密钥不正确。请核对第一步信息，保存后重新打开下载器。'
            except errors.PhoneNumberInvalid:
                self.message = "手机号格式不正确，请检查国家区号和号码。"
            except errors.PhoneNumberBanned:
                self.message = "此账号被 Telegram 限制登录，请在官方客户端检查账号状态。"
            except errors.RPCError:
                self.message = 'Telegram 未接受此次登录，请检查账号状态后重试。'
            except (OSError, TimeoutError, ConnectionError):
                self.message = '暂时无法连接 Telegram，请检查网络后重试。'
            return {'ok': False, **self.status()}
