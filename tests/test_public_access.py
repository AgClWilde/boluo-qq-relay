"""Regression tests: QQ reminder commands are not gated by an allowlist."""
import asyncio
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import AsyncMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from relay import HELP, Relay, Store, load_config


class ConfigurationTests(unittest.TestCase):
    def test_config_without_allowed_users(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text('{}', encoding='utf-8')
            config = load_config(path)
            self.assertNotIn('allowed_users', config)

    def test_legacy_config_ignores_allowlist(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(json.dumps({'allowed_users': ['123456789']}), encoding='utf-8')
            config = load_config(path)
            self.assertNotIn('allowed_users', config)

    def test_legacy_empty_allowlist_is_accepted(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(json.dumps({'allowed_users': []}), encoding='utf-8')
            self.assertNotIn('allowed_users', load_config(path))


class QQEventTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.relay = Relay.__new__(Relay)
        self.relay.config = {}  # no allowlist in new installations
        self.relay.store = Store(Path(self.temp.name) / 'relay.sqlite3')
        self.relay.command_lock = asyncio.Lock()
        self.relay.send = AsyncMock()

    async def asyncTearDown(self):
        self.relay.store.db.close()
        self.temp.cleanup()

    async def test_arbitrary_user_can_request_private_help(self):
        event = {'post_type': 'message', 'message_type': 'private',
                 'self_id': 123456, 'user_id': 987654321, 'message_id': 101,
                 'raw_message': '.boluo提醒'}
        await self.relay.on_event(event)
        self.relay.send.assert_awaited_once_with('private', '987654321', HELP)

    async def test_arbitrary_member_can_request_group_status(self):
        event = {'post_type': 'message', 'message_type': 'group',
                 'self_id': 123456, 'user_id': 876543210, 'group_id': 112233,
                 'message_id': 102,
                 'message': [{'type': 'text', 'data': {'text': '.boluo提醒 状态'}}]}
        await self.relay.on_event(event)
        self.relay.send.assert_awaited_once_with('group', '112233', '本窗口尚未绑定菠萝聊天。')

    async def test_duplicate_message_not_handled_twice(self):
        event = {'post_type': 'message', 'message_type': 'private',
                 'self_id': 123456, 'user_id': 987654321, 'message_id': 103,
                 'raw_message': '.boluo提醒'}
        await self.relay.on_event(event)
        await self.relay.on_event(event)
        self.assertEqual(self.relay.send.await_count, 1)

    async def test_self_messages_and_unrelated_text_are_ignored(self):
        await self.relay.on_event({'post_type': 'message', 'message_type': 'private',
                                   'self_id': 123456, 'user_id': 123456,
                                   'message_id': 104, 'raw_message': '.boluo提醒'})
        await self.relay.on_event({'post_type': 'message', 'message_type': 'private',
                                   'self_id': 123456, 'user_id': 987654321,
                                   'message_id': 105, 'raw_message': 'ordinary conversation'})
        self.relay.send.assert_not_awaited()


if __name__ == '__main__':
    unittest.main()
