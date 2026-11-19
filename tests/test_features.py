"""Account reuse, whole-space discovery, exclusions and migration contracts."""
import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from boluo import Boluo, BoluoError
from commands import command
from relay import Relay, Store, instance_lock

SPACE = '00000000-0000-4000-8000-000000000001'
OTHER_SPACE = '00000000-0000-4000-8000-000000000002'
CHANNEL = '00000000-0000-4000-8000-000000000011'
NEW_CHANNEL = '00000000-0000-4000-8000-000000000012'
OTHER_CHANNEL = '00000000-0000-4000-8000-000000000013'
URL = f'https://old.boluochat.com/#/chat/{SPACE}/{CHANNEL}'
OTHER_URL = f'https://old.boluochat.com/#/chat/{OTHER_SPACE}/{OTHER_CHANNEL}'


def channel(ident=CHANNEL, space=SPACE, name='Main Channel', public=True):
    return {'id': ident, 'spaceId': space, 'name': name, 'isPublic': public}


class ParserTests(unittest.TestCase):
    def test_registration_retains_password_spaces(self):
        self.assertEqual(command('。boluo提醒 登记'), {'action': 'register'})
        self.assertEqual(command('.boluo提醒 登记\nfixture-user\npassword with spaces'),
                         {'action': 'register', 'username': 'fixture-user', 'password': 'password with spaces'})

    def test_all_flag_and_mode_order(self):
        for text in (f'.boluo提醒 全频道 全部 {URL}', f'.boluo提醒 全部 {URL} 全频道',
                     f'.boluo提醒 {URL} 全频道 全部'):
            parsed = command(text)
            self.assertEqual((parsed['action'], parsed['all_channels'], parsed['mode']), ('attach', True, '全部'))
        parsed = command(f'.boluo提醒 全频道 {URL} fixture-user password 全部 with spaces')
        self.assertEqual(parsed['password'], 'password 全部 with spaces')
        self.assertNotIn('mode', parsed)

    def test_names_include_spaces_and_qualified_unicode(self):
        self.assertEqual(command('.boluo提醒 关闭 Main Channel')['name'], 'Main Channel')
        self.assertEqual(command('.boluo提醒 关闭 位面 名/频道 名')['name'], '位面 名/频道 名')
        self.assertEqual(command(f'.boluo提醒 关闭 {URL}')['url'], URL)

    def test_legacy_modes_and_credentials(self):
        self.assertEqual(command('.boluo提醒 全部'), {'action': 'mode', 'mode': '全部', 'url': ''})
        self.assertEqual(command(f'.boluo提醒 场内 {URL}')['action'], 'mode')
        self.assertEqual(command(f'{URL}\nfixture-user\npassword with spaces')['password'], 'password with spaces')


class APITests(unittest.IsolatedAsyncioTestCase):
    async def test_listing_filters_private_and_verifies_ownership(self):
        api = Boluo(None)
        api.get = AsyncMock(return_value=[{'channel': channel()},
                                         {'channel': channel(NEW_CHANNEL, public=False)}])
        self.assertEqual(await api.channels(SPACE), [channel()])
        api.get.assert_awaited_once_with('/channels/by_space', {'id': SPACE})
        api.get.return_value = [{'channel': channel(space=OTHER_SPACE)}]
        with self.assertRaises(BoluoError):
            await api.channels(SPACE)


class FakeBoluo:
    channel_map = {}
    login_calls = []
    rejected_tokens = set()

    def __init__(self, session, token=''):
        self.token = token

    async def login(self, username, password):
        self.login_calls.append((username, password))
        self.token = 'fixture-token'
        return {'token': self.token, 'user_id': 'fixture-account'}

    async def latest(self, space, ident, limit=256):
        if self.token in self.rejected_tokens:
            raise BoluoError('UNAUTHENTICATED')
        if self.channel_map[ident]['spaceId'] != space:
            raise BoluoError('NO_PERMISSION')
        return []

    async def channel(self, ident):
        return self.channel_map[ident]

    async def space(self, ident):
        return {'id': ident, 'name': 'Fixture Plane' if ident == SPACE else 'Other Plane'}

    async def channels(self, ident):
        return [c for c in self.channel_map.values() if c['spaceId'] == ident and c['isPublic']]


class FeatureTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'state.sqlite3'
        self.relay = Relay.__new__(Relay)
        self.relay.config = {}
        self.relay.store = Store(self.path)
        self.relay.command_lock = asyncio.Lock()
        self.relay.auth_lock = asyncio.Lock()
        self.relay.discovery_lock = asyncio.Lock()
        self.relay.pending_registration = {}
        self.relay.workers = {}
        self.relay.source_states = {}
        self.relay.session = None
        self.relay.send = AsyncMock()
        self.relay.reconcile = AsyncMock()
        FakeBoluo.channel_map = {CHANNEL: channel(), OTHER_CHANNEL: channel(OTHER_CHANNEL, OTHER_SPACE)}
        FakeBoluo.login_calls = []
        FakeBoluo.rejected_tokens = set()
        self.api_patch = patch('relay.Boluo', FakeBoluo)
        self.api_patch.start()

    async def asyncTearDown(self):
        self.api_patch.stop()
        self.relay.store.db.close()
        self.temp.cleanup()

    async def register(self, owner='101'):
        await self.relay.handle(command('.boluo提醒 登记 fixture-user fixture-password'), owner, 'private', owner)

    async def all(self, owner='101', group='201'):
        await self.relay.handle(command(f'.boluo提醒 全频道 {URL}'), owner, 'group', group)
        return self.relay.store.spaces('group', group)[0]

    async def test_private_registration_then_new_space_in_group(self):
        await self.register()
        await self.relay.handle(command(f'.boluo提醒 {OTHER_URL}'), '101', 'group', '201')
        binding = self.relay.store.active()[0]
        self.assertEqual((binding['space'], binding['owner'], binding['target_type']), (OTHER_SPACE, '101', 'group'))
        self.assertEqual(len(FakeBoluo.login_calls), 1)
        self.assertNotIn('fixture-password', str(self.relay.send.await_args_list))
        self.relay.store.db.close()
        self.relay.store = Store(self.path)
        self.assertEqual(self.relay.store.credential('101')['username'], 'fixture-user')
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)

    async def test_no_args_private_registration_followup_and_duplicate(self):
        def event(mid, text, kind='private'):
            return {'post_type': 'message', 'message_type': kind, 'self_id': '999',
                    'user_id': '101', 'group_id': '201', 'message_id': mid, 'raw_message': text}
        await self.relay.on_event(event(1, '。boluo提醒 登记'))
        credentials = event(2, 'fixture-user\nfixture-password')
        await self.relay.on_event(credentials)
        await self.relay.on_event(credentials)
        self.assertEqual(len(FakeBoluo.login_calls), 1)
        self.assertEqual(self.relay.send.await_count, 2)

    async def test_group_registration_rejected_without_login(self):
        await self.relay.handle(command('.boluo提醒 登记 fixture-user fixture-password'), '101', 'group', '201')
        self.assertFalse(FakeBoluo.login_calls)
        self.assertIsNone(self.relay.store.credential('101'))

    async def test_new_public_channel_uses_original_boundary_and_enters_outbox(self):
        await self.register()
        subscription = await self.all()
        FakeBoluo.channel_map[NEW_CHANNEL] = channel(NEW_CHANNEL, name='New Channel')
        await self.relay.discover_space(subscription)
        binding = next(b for b in self.relay.store.active() if b['channel'] == NEW_CHANNEL)
        self.assertEqual(binding['started'], subscription['started'])
        message = {'id': 'fixture-message', 'channelId': NEW_CHANNEL, 'created': datetime.fromtimestamp(time.time() + 1, timezone.utc).isoformat(),
                   'inGame': True, 'name': 'Fixture Speaker', 'text': 'fixture-body'}
        self.relay.store.ingest(binding, [message])
        queued = self.relay.store.rows('SELECT * FROM outbox WHERE binding=?', (binding['id'],))
        self.assertEqual(len(queued), 1)
        self.assertIn('New Channel', queued[0]['text'])
        self.assertNotIn('fixture-body', queued[0]['text'])

    async def test_name_exclusion_persists_and_explicit_url_restores(self):
        await self.register()
        subscription = await self.all()
        await self.relay.handle(command('.boluo提醒 关闭 Main Channel'), '102', 'group', '201')
        self.assertFalse(self.relay.store.active())
        self.relay.store.db.close()
        self.relay.store = Store(self.path)
        await self.relay.discover_space(subscription)
        self.assertFalse(self.relay.store.active())
        await self.relay.handle(command(f'.boluo提醒 {URL}'), '102', 'group', '201')
        self.assertEqual(len(self.relay.store.active()), 1)
        self.assertFalse(self.relay.store.rows('SELECT * FROM exclusions'))

    async def test_every_group_member_can_add_existing_space_but_not_other_group(self):
        await self.register()
        await self.all()
        FakeBoluo.channel_map[NEW_CHANNEL] = channel(NEW_CHANNEL, name='New Channel')
        url = f'https://old.boluochat.com/#/chat/{SPACE}/{NEW_CHANNEL}'
        await self.relay.handle(command(f'.boluo提醒 {url}'), '102', 'group', '201')
        self.assertEqual(len(self.relay.store.active()), 2)
        await self.relay.handle(command(f'.boluo提醒 {url}'), '102', 'group', '202')
        self.assertEqual(len(self.relay.store.active()), 2)
        self.assertIn('登记', self.relay.send.await_args.args[2])

    async def test_group_identity_precedes_unrelated_member_registration(self):
        await self.register()
        await self.all()
        self.relay.store.remember('102', 'other-user', 'other-password',
                                  {'token': 'unrelated-token', 'user_id': 'other-account'})
        FakeBoluo.rejected_tokens = {'unrelated-token'}
        FakeBoluo.channel_map[NEW_CHANNEL] = channel(NEW_CHANNEL, name='New Channel')
        url = f'https://old.boluochat.com/#/chat/{SPACE}/{NEW_CHANNEL}'
        await self.relay.handle(command(f'.boluo提醒 {url}'), '102', 'group', '201')
        binding = next(b for b in self.relay.store.active() if b['channel'] == NEW_CHANNEL)
        self.assertEqual((binding['owner'], binding['token']), ('101', 'fixture-token'))

    async def test_ambiguous_name_does_not_close_then_qualified_name_does(self):
        await self.register()
        await self.relay.handle(command(f'.boluo提醒 {URL}'), '101', 'group', '201')
        await self.relay.handle(command(f'.boluo提醒 {OTHER_URL}'), '101', 'group', '201')
        await self.relay.handle(command('.boluo提醒 关闭 Main Channel'), '102', 'group', '201')
        self.assertEqual(len(self.relay.store.active()), 2)
        await self.relay.handle(command('.boluo提醒 关闭 Fixture Plane/Main Channel'), '102', 'group', '201')
        self.assertEqual([b['space'] for b in self.relay.store.active()], [OTHER_SPACE])

    async def test_repeat_all_preserves_cursor_mode_and_exclusion(self):
        await self.register()
        subscription = await self.all()
        binding = self.relay.store.active()[0]
        self.relay.store.set_mode([binding['id']], '全部')
        with self.relay.store.db:
            self.relay.store.db.execute("UPDATE bindings SET cursor='fixture-cursor',notification_state=? WHERE id=?",
                                        (json.dumps({'phase': 'muted'}), binding['id']))
        await self.all()
        current = self.relay.store.get(binding['id'])
        self.assertEqual((current['started'], current['cursor'], current['mode'], current['notification_state']),
                         (binding['started'], 'fixture-cursor', '全部', json.dumps({'phase': 'muted'})))
        await self.relay.handle(command('.boluo提醒 关闭 Main Channel'), '102', 'group', '201')
        await self.all()
        self.assertFalse(self.relay.store.active())
        self.assertEqual(self.relay.store.spaces()[0]['id'], subscription['id'])

    async def test_close_all_disables_discovery(self):
        await self.register()
        await self.all()
        await self.relay.handle(command('.boluo提醒 关闭'), '102', 'group', '201')
        self.assertFalse(self.relay.store.active())
        self.assertFalse(self.relay.store.spaces())

    async def test_status_lists_plane_channel_and_canonical_url(self):
        await self.register()
        await self.all()
        await self.relay.handle(command('.boluo提醒 状态'), '102', 'group', '201')
        status = self.relay.send.await_args.args[2]
        self.assertIn('位面「Fixture Plane」', status)
        self.assertIn('频道「Main Channel」', status)
        self.assertIn(URL, status)
        self.assertIn('全频道', status)

    async def test_expired_token_renews_without_reentering_password(self):
        await self.register()
        with self.relay.store.db:
            self.relay.store.db.execute("UPDATE credentials SET token='expired'")
        FakeBoluo.rejected_tokens = {'expired'}
        await self.relay.handle(command(f'.boluo提醒 {OTHER_URL}'), '101', 'group', '201')
        self.assertEqual(len(FakeBoluo.login_calls), 2)
        self.assertEqual(self.relay.store.active()[0]['token'], 'fixture-token')

    async def test_duplicate_invalid_command_sends_one_response(self):
        event = {'post_type': 'message', 'message_type': 'private', 'self_id': '999',
                 'user_id': '101', 'message_id': 3, 'raw_message': '.boluo提醒 测试 invalid'}
        await self.relay.on_event(event)
        await self.relay.on_event(event)
        self.assertEqual(self.relay.send.await_count, 1)

    async def test_disappeared_public_channel_stops_automatic_binding(self):
        await self.register()
        subscription = await self.all()
        FakeBoluo.channel_map[CHANNEL]['isPublic'] = False
        await self.relay.discover_space(subscription)
        self.assertFalse(self.relay.store.active())
        self.assertFalse(self.relay.store.rows('SELECT * FROM exclusions'))


class MigrationTests(unittest.TestCase):
    def test_schema_upgrade_keeps_existing_binding_and_progress(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'legacy.sqlite3'
            db = sqlite3.connect(path)
            db.executescript('''CREATE TABLE bindings (
              id TEXT PRIMARY KEY, owner TEXT NOT NULL, target_type TEXT NOT NULL,
              target_id TEXT NOT NULL, space TEXT NOT NULL, channel TEXT NOT NULL,
              title TEXT NOT NULL, token TEXT NOT NULL, user_id TEXT NOT NULL,
              started REAL NOT NULL, active INTEGER NOT NULL DEFAULT 1,
              cursor TEXT, last_error TEXT, last_event REAL, UNIQUE(target_type,target_id,channel));''')
            db.execute('INSERT INTO bindings VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                       ('fixture-binding', '101', 'group', '201', SPACE, CHANNEL, 'Legacy Title',
                        'legacy-token', 'legacy-account', 123.0, 1, 'legacy-cursor', None, 456.0))
            db.commit()
            db.close()
            store = Store(path)
            binding = store.active()[0]
            self.assertEqual((binding['id'], binding['token'], binding['cursor'], binding['started']),
                             ('fixture-binding', 'legacy-token', 'legacy-cursor', 123.0))
            self.assertIsNone(binding['space_subscription'])
            store.db.close()

    def test_canonical_database_cannot_be_locked_twice(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'state.sqlite3'
            with instance_lock(path):
                with self.assertRaises(RuntimeError):
                    with instance_lock(path.parent / '.' / path.name):
                        pass
            with instance_lock(path):
                pass


if __name__ == '__main__':
    unittest.main()
