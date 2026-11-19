#!/usr/bin/env python3
"""Persistent Boluo -> NapCat relay. No inbound HTTP server is necessary."""
import argparse
import asyncio
import contextlib
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
import time
from urllib.parse import urlsplit
import uuid

import aiohttp
from boluo import Boluo, BoluoError
from onebot import OneBot
from commands import HELP, command, parse_url
from notifications import epoch, render, resume, transition

LOG = logging.getLogger("boluo-qq")

@contextlib.contextmanager
def instance_lock(database):
    """A canonical database has one relay process, including symlink aliases."""
    path = Path(database).expanduser().resolve()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with path.with_name(path.name + ".lock").open("a") as handle:
        os.chmod(handle.name, 0o600)
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("已有服务正在使用此数据库。") from None
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def load_config(filename):
    """Resolve user-supplied paths without embedding any deployment identity."""
    path = Path(filename).expanduser().resolve()
    try:
        config = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raise ValueError("无法读取有效的配置文件。") from None
    if not isinstance(config, dict):
        raise ValueError("配置必须是 JSON 对象。")
    # Legacy configurations may contain allowed_users. It no longer restricts
    # QQ commands and is ignored so existing installations keep working.
    config.pop("allowed_users", None)
    config.pop("allowed_user", None)
    database = config.get("database", "state/relay.sqlite3")
    if not isinstance(database, str) or not database.strip():
        raise ValueError("database 须为有效路径。")
    for key, value in (("database", database), ("napcat_config", config.get("napcat_config"))):
        if value is not None:
            if not isinstance(value, str) or not value.strip():
                raise ValueError("配置路径不能为空。")
            resolved = Path(value).expanduser()
            config[key] = str((path.parent / resolved).resolve() if not resolved.is_absolute() else resolved.resolve())
    url = config.get("onebot_url", "ws://127.0.0.1:3001")
    try:
        if not isinstance(url, str) or any(character.isspace() for character in url):
            raise ValueError
        parsed = urlsplit(url)
        valid = (parsed.scheme in ("ws", "wss") and parsed.hostname
                 and parsed.username is None and parsed.password is None
                 and not parsed.query and not parsed.fragment
                 and (parsed.port is None or 1 <= parsed.port <= 65535))
    except (TypeError, ValueError):
        valid = False
    if not valid:
        raise ValueError("onebot_url 须为不含用户名、密码或查询参数的正向 WebSocket 地址。")
    config["onebot_url"] = url
    if "napcat_ws_port" in config:
        port = config["napcat_ws_port"]
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise ValueError("napcat_ws_port 须为 1–65535 的整数。")
    env_name = config.get("onebot_token_env")
    if env_name is not None and (not isinstance(env_name, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", env_name)):
        raise ValueError("onebot_token_env 须为有效的环境变量名称。")
    for key, default in (("rescan_seconds", 300), ("send_interval", 1.2)):
        value = config.get(key, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 < value < float("inf"):
            raise ValueError("轮询及发送间隔须为有限正数。")
        config[key] = value
    return config


def resolve_onebot_token(config):
    """Read credentials at runtime; never include them in distributed files."""
    env_name = config.get("onebot_token_env")
    if env_name and env_name in os.environ:
        token = os.environ[env_name]
    elif config.get("napcat_config"):
        try:
            napcat = json.loads(Path(config["napcat_config"]).read_text(encoding="utf-8"))
            servers = napcat["network"]["websocketServers"]
            if not isinstance(servers, list):
                raise ValueError
            selected = [server for server in servers if isinstance(server, dict) and server.get("enable") is True
                        and ("napcat_ws_port" not in config or server.get("port") == config["napcat_ws_port"]
                             or str(server.get("port")) == str(config["napcat_ws_port"]))]
            if len(selected) != 1:
                raise ValueError
            token = selected[0].get("token", "")
        except (OSError, ValueError, KeyError, TypeError):
            raise ValueError("无法唯一选择 NapCat 正向 WebSocket；请检查配置路径及 napcat_ws_port。") from None
    else:
        raise ValueError("请提供 NapCat 配置，或设置指定的 OneBot 令牌环境变量。")
    if not isinstance(token, str) or "\r" in token or "\n" in token:
        raise ValueError("OneBot 令牌格式无效。")
    return token


def hidden(message):
    return (message.get("whisperToUsers") is not None and not message.get("text")
            and not message.get("entities") and not message.get("mediaId"))



class Store:
    def __init__(self, path):
        path = Path(path)
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        os.chmod(path, 0o600)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
          PRAGMA journal_mode=WAL;
          PRAGMA synchronous=FULL;
          CREATE TABLE IF NOT EXISTS bindings (
            id TEXT PRIMARY KEY, owner TEXT NOT NULL, target_type TEXT NOT NULL,
            target_id TEXT NOT NULL, space TEXT NOT NULL, channel TEXT NOT NULL,
            title TEXT NOT NULL, token TEXT NOT NULL, user_id TEXT NOT NULL,
            started REAL NOT NULL, active INTEGER NOT NULL DEFAULT 1,
            cursor TEXT, last_error TEXT, last_event REAL,
            UNIQUE(target_type,target_id,channel));
          CREATE TABLE IF NOT EXISTS seen (
            binding TEXT NOT NULL, message TEXT NOT NULL,
            PRIMARY KEY(binding,message));
          CREATE TABLE IF NOT EXISTS outbox (
            id INTEGER PRIMARY KEY, binding TEXT NOT NULL, message TEXT NOT NULL,
            part INTEGER NOT NULL, text TEXT NOT NULL, sent INTEGER NOT NULL DEFAULT 0,
            created REAL NOT NULL, qq_message TEXT, UNIQUE(binding,message,part));
          CREATE TABLE IF NOT EXISTS commands (
            id TEXT PRIMARY KEY, created REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS credentials (
            owner TEXT PRIMARY KEY, username TEXT NOT NULL, password TEXT NOT NULL,
            token TEXT NOT NULL, user_id TEXT NOT NULL, updated REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS space_subscriptions (
            id TEXT PRIMARY KEY, owner TEXT NOT NULL, target_type TEXT NOT NULL,
            target_id TEXT NOT NULL, space TEXT NOT NULL, space_title TEXT NOT NULL,
            token TEXT NOT NULL, user_id TEXT NOT NULL, started REAL NOT NULL,
            mode TEXT NOT NULL DEFAULT '场内', active INTEGER NOT NULL DEFAULT 1,
            last_error TEXT, UNIQUE(target_type,target_id,space));
          CREATE TABLE IF NOT EXISTS exclusions (
            space_subscription TEXT NOT NULL, channel TEXT NOT NULL,
            PRIMARY KEY(space_subscription,channel));
        """)
        # Existing subscriptions retain their credentials and cursors. Legacy
        # queued bodies are quarantined until authenticated metadata conversion.
        with self.db:
            binding_columns = {r[1] for r in self.db.execute("PRAGMA table_info(bindings)")}
            for name, declaration in (("mode", "TEXT NOT NULL DEFAULT '场内'"),
                                      ("space_title", "TEXT NOT NULL DEFAULT ''"),
                                      ("channel_title", "TEXT NOT NULL DEFAULT ''"),
                                      ("space_subscription", "TEXT"),
                                      ("space_managed", "INTEGER NOT NULL DEFAULT 0"),
                                      ("notification_state", "TEXT NOT NULL DEFAULT '{}'")):
                if name not in binding_columns:
                    self.db.execute(f"ALTER TABLE bindings ADD COLUMN {name} {declaration}")
            outbox_columns = {r[1] for r in self.db.execute("PRAGMA table_info(outbox)")}
            for name, declaration in (("kind", "TEXT NOT NULL DEFAULT 'notice'"),
                                      ("source_in_game", "INTEGER")):
                if name not in outbox_columns:
                    self.db.execute(f"ALTER TABLE outbox ADD COLUMN {name} {declaration}")
            if self.db.execute("PRAGMA user_version").fetchone()[0] < 2:
                self.db.execute("UPDATE outbox SET kind='legacy' WHERE sent=0")
                self.db.execute("PRAGMA user_version=2")
            self.db.execute("CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(binding,sent,created,id)")

    def rows(self, sql, params=()):
        return [dict(r) for r in self.db.execute(sql, params)]

    def active(self):
        return self.rows("SELECT * FROM bindings WHERE active=1")

    def get(self, binding):
        rows = self.rows("SELECT * FROM bindings WHERE id=?", (binding,))
        return rows[0] if rows else None

    def credential(self, owner):
        rows = self.rows("SELECT * FROM credentials WHERE owner=?", (owner,))
        return rows[0] if rows else None

    def remember(self, owner, username, password, auth):
        with self.db:
            self.db.execute("""INSERT INTO credentials VALUES (?,?,?,?,?,?)
                ON CONFLICT(owner) DO UPDATE SET username=excluded.username,
                password=excluded.password,token=excluded.token,user_id=excluded.user_id,updated=excluded.updated""",
                (owner, username, password, auth["token"], auth["user_id"], time.time()))

    def spaces(self, kind=None, target=None):
        if kind is None:
            return self.rows("SELECT * FROM space_subscriptions WHERE active=1")
        return self.rows("SELECT * FROM space_subscriptions WHERE active=1 AND target_type=? AND target_id=?", (kind, target))

    def close_binding(self, binding):
        with self.db:
            self.db.execute("UPDATE bindings SET active=0,token='',notification_state='{}' WHERE id=?", (binding["id"],))
            self.db.execute("DELETE FROM outbox WHERE binding=?", (binding["id"],))
            parent = binding.get("space_subscription")
            if parent:
                self.db.execute("INSERT OR IGNORE INTO exclusions VALUES (?,?)", (parent, binding["channel"]))

    def ingest(self, binding, messages, cursor=None, legacy_ids=None):
        fresh = 0
        with self.db:
            binding = self.get(binding["id"])
            if not binding or not binding["active"]:
                return 0
            state = json.loads(binding["notification_state"])
            if legacy_ids:
                recovered_ids = {m["id"] for m in messages}
                for ident in legacy_ids:
                    self.db.execute("DELETE FROM outbox WHERE binding=? AND message=? AND kind='legacy'", (binding["id"], ident))
                    if ident in recovered_ids:
                        self.db.execute("DELETE FROM seen WHERE binding=? AND message=?", (binding["id"], ident))
            for m in sorted(messages, key=lambda m: (epoch(m["created"]), m["id"])):
                if str(m.get("channelId")) != binding["channel"] or epoch(m["created"]) < binding["started"]:
                    continue
                inserted = self.db.execute("INSERT OR IGNORE INTO seen VALUES (?,?)", (binding["id"], m["id"])).rowcount
                if inserted and not m.get("_hidden"):
                    state, notices = transition(state, m, binding["mode"], binding["title"])
                    first_part = self.db.execute("SELECT coalesce(max(part)+1,0) FROM outbox WHERE binding=? AND message=?", (binding["id"], m["id"])).fetchone()[0] if legacy_ids else 0
                    for part, text in enumerate(notices):
                        kind = "mute" if text.startswith("监测到「") else "resume" if text.endswith("重新开始提醒该频道消息。") else "notice"
                        self.db.execute("INSERT INTO outbox(binding,message,part,text,created,kind,source_in_game) VALUES (?,?,?,?,?,?,?)",
                                        (binding["id"], m["id"], first_part + part, text, epoch(m["created"]), kind,
                                         int(m.get("inGame") is True) if kind == "notice" else None))
                    fresh += 1
            self.db.execute("UPDATE bindings SET notification_state=? WHERE id=?", (json.dumps(state, ensure_ascii=False), binding["id"]))
            if cursor is not None:
                self.db.execute("UPDATE bindings SET cursor=?,last_event=?,last_error=NULL WHERE id=?",
                                (json.dumps(cursor), time.time(), binding["id"]))
        return fresh

    def resume_due(self, binding_id, now):
        with self.db:
            binding = self.get(binding_id)
            if not binding or not binding["active"]:
                return 0
            previous = json.loads(binding["notification_state"])
            state, notices = resume(previous, binding["title"], now)
            for part, text in enumerate(notices):
                ident = f"__resume__:{previous.get('last_activity')}"
                self.db.execute("INSERT OR IGNORE INTO outbox(binding,message,part,text,created,kind) VALUES (?,?,?,?,?,?)",
                                (binding_id, ident, part, text, now, "resume"))
            self.db.execute("UPDATE bindings SET notification_state=? WHERE id=?", (json.dumps(state, ensure_ascii=False), binding_id))
            return len(notices)

    def set_mode(self, binding_ids, mode):
        if mode not in ("场内", "全部"):
            raise ValueError("Invalid notification mode")
        with self.db:
            for binding_id in binding_ids:
                old = self.get(binding_id)
                if old["mode"] == mode:
                    continue
                self.db.execute("UPDATE bindings SET mode=?,notification_state='{}' WHERE id=?", (mode, binding_id))
                self.db.execute("DELETE FROM outbox WHERE binding=? AND sent=0 AND kind IN ('mute','resume')", (binding_id,))
                if mode == "场内":
                    self.db.execute("DELETE FROM outbox WHERE binding=? AND sent=0 AND source_in_game=0", (binding_id,))


class Relay:
    def __init__(self, config):
        self.config = config
        self.store = Store(config["database"])
        token = resolve_onebot_token(config)
        self.onebot = OneBot(config.get("onebot_url", "ws://127.0.0.1:3001"), token, self.on_event)
        self.workers = {}
        self.source_states = {}
        self.source_queues = {}
        self.command_lock = asyncio.Lock()
        self.session = None
        self.pending_registration = {}
        self.auth_lock = asyncio.Lock()
        self.discovery_lock = asyncio.Lock()
        self.reconcile_lock = asyncio.Lock()

    async def send(self, kind, target, text):
        params = {"message": [{"type": "text", "data": {"text": text}}]}
        action = "send_group_msg" if kind == "group" else "send_private_msg"
        params["group_id" if kind == "group" else "user_id"] = int(target)
        return await self.onebot.call(action, params)

    async def on_event(self, event):
        if event.get("post_type") != "message" or str(event.get("user_id")) == str(event.get("self_id")):
            return
        owner = str(event.get("user_id"))
        segments = event.get("message")
        text = "".join(s.get("data", {}).get("text", "") for s in segments if s.get("type") == "text") if isinstance(segments, list) else str(event.get("raw_message") or segments or "")
        kind = "group" if event.get("message_type") == "group" else "private"
        target = str(event.get("group_id") if kind == "group" else owner)
        invalid = False
        try:
            parsed = command(text)
        except (ValueError, TypeError):
            parsed, invalid = {}, True
        async with self.command_lock:
            pending = getattr(self, "pending_registration", {})
            expires = pending.get(owner, 0)
            if kind == "private" and parsed is None and expires > time.time():
                credentials = text.strip().split(None, 1)
                if len(credentials) == 2:
                    parsed = {"action": "register", "username": credentials[0], "password": credentials[1]}
                else:
                    parsed, invalid = {}, True
            if parsed is None:
                return
            # Persist before handling. Repeated OneBot deliveries cannot create
            # a second confirmation, including malformed credential commands.
            message_id = event.get("message_id")
            if message_id is None:
                payload = json.dumps([event.get("time"), owner, text], ensure_ascii=False)
                message_id = "fallback:" + hashlib.sha256(payload.encode()).hexdigest()
            ident = f"{event.get('self_id')}:{kind}:{target}:{message_id}"
            with self.store.db:
                legacy_ident = f"{event.get('self_id')}:{message_id}:{owner}"
                if self.store.rows("SELECT id FROM commands WHERE id=?", (legacy_ident,)):
                    return
                if not self.store.db.execute("INSERT OR IGNORE INTO commands VALUES (?,?)", (ident, time.time())).rowcount:
                    return
            if invalid:
                return await self.send(kind, target, "菠萝指令格式错误。发送 .boluo提醒 查看用法。")
            try:
                await self.handle(parsed, owner, kind, target)
            except BoluoError:
                await self.send(kind, target, "菠萝登录或访问失败。请核对账号、密码及该频道权限后重新登记。")
            except Exception as exc:
                LOG.warning("command_failed class=%s", type(exc).__name__)
                with contextlib.suppress(Exception):
                    await self.send(kind, target, "操作暂未完成，请稍后重试。")

    async def register(self, cmd, owner, kind, target):
        if kind != "private":
            return await self.send(kind, target, "账号登记仅限私聊。请私聊发送 .boluo提醒 登记。")
        if not cmd.get("username"):
            self.pending_registration[owner] = time.time() + 300
            return await self.send(kind, target, "请在 5 分钟内私聊发送用户名和密码，可分为两行。")
        api = Boluo(self.session)
        password = cmd.pop("password")
        try:
            auth = await api.login(cmd["username"], password)
            self.store.remember(owner, cmd["username"], password, auth)
        finally:
            del password
        self.pending_registration.pop(owner, None)
        return await self.send(kind, target, "菠萝账号已登记。以后可在任意窗口发送 .boluo提醒 <频道网址> 或 .boluo提醒 全频道 <频道网址>。")

    async def auth_for(self, owner, kind, target, space):
        group_matches = [b for b in self.store.active() + self.store.spaces()
                         if kind == "group" and b["target_type"] == kind and b["target_id"] == target and b["space"] == space]
        if group_matches:
            return group_matches[0]
        registered = self.store.credential(owner)
        if registered:
            return registered
        # A group binding's existing session is shared with all members for
        # management within that group. It never transfers to another window.
        matches = [b for b in self.store.active() if b["space"] == space and
                   (b["owner"] == owner or (kind == "group" and b["target_type"] == kind and b["target_id"] == target))]
        matches += [b for b in self.store.spaces() if b["space"] == space and
                    (b["owner"] == owner or (kind == "group" and b["target_type"] == kind and b["target_id"] == target))]
        return matches[0] if matches else None

    async def refresh_auth(self, owner, user_id, failed_token=None):
        async with self.auth_lock:
            registered = self.store.credential(owner)
            if not registered or registered["user_id"] != user_id:
                raise BoluoError("UNAUTHENTICATED")
            if failed_token is not None and registered["token"] != failed_token:
                return {"token": registered["token"], "user_id": registered["user_id"]}
            api = Boluo(self.session)
            auth = await api.login(registered["username"], registered["password"])
            self.store.remember(owner, registered["username"], registered["password"], auth)
            with self.store.db:
                self.store.db.execute("UPDATE bindings SET token=? WHERE owner=? AND user_id=?", (auth["token"], owner, user_id))
                self.store.db.execute("UPDATE space_subscriptions SET token=? WHERE owner=? AND user_id=?", (auth["token"], owner, user_id))
            return auth

    async def bind_channel(self, owner, kind, target, space, metadata, space_title,
                           token, user_id, mode=None, parent=None, started=None):
        channel = metadata["id"]
        if metadata.get("spaceId") != space:
            raise BoluoError("BAD_REQUEST")
        channel_title = str(metadata.get("name") or channel)
        title = f"{space_title}-{channel_title}"
        rows = self.store.rows("SELECT * FROM bindings WHERE target_type=? AND target_id=? AND channel=?", (kind, target, channel))
        existing = rows[0] if rows else None
        binding_id = existing["id"] if existing else str(uuid.uuid4())
        boundary = existing["started"] if existing and existing["active"] else (time.time() if started is None else started)
        with self.store.db:
            if not existing or not existing["active"]:
                self.store.db.execute("DELETE FROM seen WHERE binding=?", (binding_id,))
                self.store.db.execute("DELETE FROM outbox WHERE binding=?", (binding_id,))
            self.store.db.execute("""INSERT INTO bindings(id,owner,target_type,target_id,space,channel,title,token,user_id,started)
                VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT(target_type,target_id,channel)
                DO UPDATE SET owner=excluded.owner,title=excluded.title,token=excluded.token,user_id=excluded.user_id,
                started=excluded.started,active=1,last_error=NULL""",
                (binding_id, owner, kind, target, space, channel, title, token, user_id, boundary))
            self.store.db.execute("UPDATE bindings SET space_title=?,channel_title=?,space_subscription=? WHERE id=?",
                                  (space_title, channel_title, parent or (existing or {}).get("space_subscription"), binding_id))
            self.store.db.execute("UPDATE bindings SET space_managed=? WHERE id=?", (int(bool(parent)) if not existing or not existing["active"] else existing["space_managed"], binding_id))
            if not parent:
                self.store.db.execute("DELETE FROM exclusions WHERE channel=? AND space_subscription IN (SELECT id FROM space_subscriptions WHERE target_type=? AND target_id=? AND space=?)", (channel, kind, target, space))
        if mode is not None:
            self.store.set_mode([binding_id], mode)
        elif not existing or not existing["active"]:
            self.store.set_mode([binding_id], "场内")
        if existing and (existing["token"] != token or existing["user_id"] != user_id):
            task = self.workers.pop(binding_id, None)
            if task:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        return self.store.get(binding_id)

    async def discover_space(self, subscription, api=None, metadata=None):
        api = api or Boluo(self.session, subscription["token"])
        space = metadata or await api.space(subscription["space"])
        channels = await api.channels(subscription["space"])
        title = str(space.get("name") or subscription["space"])
        exclusions = {r["channel"] for r in self.store.rows("SELECT channel FROM exclusions WHERE space_subscription=?", (subscription["id"],))}
        public_ids = {channel["id"] for channel in channels}
        # Automatically added channels stop relaying once they are no longer
        # public or listed. Explicit subscriptions retain their access policy.
        with self.store.db:
            for binding in self.store.rows("SELECT * FROM bindings WHERE active=1 AND space_subscription=? AND space_managed=1", (subscription["id"],)):
                if binding["channel"] not in public_ids:
                    self.store.db.execute("UPDATE bindings SET active=0,token='',notification_state='{}' WHERE id=?", (binding["id"],))
                    self.store.db.execute("DELETE FROM outbox WHERE binding=?", (binding["id"],))
        added = 0
        for channel in channels:
            if channel["id"] in exclusions:
                continue
            existing = self.store.rows("SELECT * FROM bindings WHERE target_type=? AND target_id=? AND channel=?", (subscription["target_type"], subscription["target_id"], channel["id"]))
            if existing and existing[0]["active"]:
                # Do not disturb a deliberately selected per-channel mode.
                with self.store.db:
                    self.store.db.execute("UPDATE bindings SET title=?,space_title=?,channel_title=?,space_subscription=? WHERE id=?",
                                          (f"{title}-{channel.get('name') or channel['id']}", title, str(channel.get("name") or channel["id"]), subscription["id"], existing[0]["id"]))
                continue
            await api.latest(subscription["space"], channel["id"], limit=1)
            await self.bind_channel(subscription["owner"], subscription["target_type"], subscription["target_id"], subscription["space"], channel, title,
                                    api.token, subscription["user_id"], mode=subscription["mode"], parent=subscription["id"], started=subscription["started"])
            added += 1
        with self.store.db:
            self.store.db.execute("UPDATE space_subscriptions SET space_title=?,last_error=NULL WHERE id=?", (title, subscription["id"]))
        return added

    async def handle(self, cmd, owner, kind, target):
        action = cmd["action"]
        if action == "help":
            return await self.send(kind, target, HELP)
        if action == "register":
            return await self.register(cmd, owner, kind, target)
        bindings = [b for b in self.store.active() if b["target_type"] == kind and b["target_id"] == target]
        spaces = self.store.spaces(kind, target)
        if action == "status":
            lines = []
            for subscription in spaces:
                excluded = self.store.rows("SELECT count(*) AS n FROM exclusions WHERE space_subscription=?", (subscription["id"],))[0]["n"]
                lines.append(f"位面「{subscription['space_title']}」：全频道；新频道默认「{subscription['mode']}」；已排除 {excluded} 个；{subscription['last_error'] or '自动发现已启用'}")
            for b in bindings:
                pending = self.store.rows("SELECT count(*) AS n FROM outbox WHERE binding=? AND sent=0", (b["id"],))[0]["n"]
                state = b["last_error"] or self.source_states.get(b["id"], "连接中")
                muted = json.loads(b["notification_state"]).get("phase") == "muted"
                throttle = "；高频暂停中" if muted else ""
                label = f"位面「{b['space_title']}」／频道「{b['channel_title']}」" if b["space_title"] and b["channel_title"] else b["title"]
                lines.append(f"{label}：{b['mode']}；{state}{throttle}；待发 {pending} 条\nhttps://old.boluochat.com/#/chat/{b['space']}/{b['channel']}")
            return await self.send(kind, target, "\n".join(lines) if lines else "本窗口尚未绑定菠萝聊天。")
        channel = parse_url(cmd["url"])[1] if cmd.get("url") else None
        if action in ("mode", "off", "test"):
            selected = [b for b in bindings if channel is None or channel == b["channel"]]
            if cmd.get("name"):
                selected = [b for b in bindings if cmd["name"] in (b["channel_title"], b["title"], f"{b['space_title']}/{b['channel_title']}")]
                if len(selected) > 1:
                    return await self.send(kind, target, "频道名重名，请使用「位面名/频道名」或完整频道网址。")
            if action == "mode":
                async with self.discovery_lock:
                    selected = [b for b in self.store.active() if b["target_type"] == kind and b["target_id"] == target and (channel is None or channel == b["channel"])]
                    self.store.set_mode([b["id"] for b in selected], cmd["mode"])
                    if not channel:
                        with self.store.db:
                            self.store.db.execute("UPDATE space_subscriptions SET mode=? WHERE active=1 AND target_type=? AND target_id=?", (cmd["mode"], kind, target))
                return await self.send(kind, target, f"已将本窗口 {len(selected)} 个菠萝订阅设为「{cmd['mode']}」模式。")
            if action == "off":
                async with self.discovery_lock:
                    selected = [b for b in self.store.active() if b["target_type"] == kind and b["target_id"] == target and (channel is None or channel == b["channel"])]
                    if cmd.get("name"):
                        selected = [b for b in selected if cmd["name"] in (b["channel_title"], b["title"], f"{b['space_title']}/{b['channel_title']}")]
                        if len(selected) > 1:
                            return await self.send(kind, target, "频道名重名，请使用「位面名/频道名」或完整频道网址。")
                    for binding in selected:
                        self.store.close_binding(binding)
                    if not channel and not cmd.get("name"):
                        with self.store.db:
                            self.store.db.execute("UPDATE space_subscriptions SET active=0,token='' WHERE target_type=? AND target_id=?", (kind, target))
                await self.reconcile()
                return await self.send(kind, target, f"已关闭 {len(selected)} 个菠萝订阅。")
            if len(selected) != 1:
                return await self.send(kind, target, "请指定一个已绑定的菠萝频道网址。")
            b = selected[0]
            api = Boluo(self.session, b["token"])
            latest = await api.latest(b["space"], b["channel"], limit=1)
            if not latest:
                return await self.send(kind, target, "测试成功：频道可访问，目前没有消息。")
            m = latest[0]
            if m.get("whisperToUsers") is not None:
                m = await api.message(m["id"])
                if hidden(m):
                    return await self.send(kind, target, "测试成功：最新消息为当前账号不可见的私密消息，不转发其内容。")
            if b["mode"] == "场内" and m.get("inGame") is not True:
                return await self.send(kind, target, "测试成功：最新消息为场外消息，当前「场内」模式不会提醒。")
            for text in render(m, b["title"]):
                await self.send(kind, target, "【连接测试，现有消息提醒】\n" + text)
            return
        space, channel = parse_url(cmd["url"])
        if action == "bind" and kind == "group":
            return await self.send(kind, target, "账号密码仅限私聊登记。请私聊发送 .boluo提醒 登记，再在本群发送 .boluo提醒 <频道网址>。")
        if action == "bind":
            api = Boluo(self.session)
            password = cmd.pop("password")
            try:
                auth = await api.login(cmd["username"], password)
                self.store.remember(owner, cmd["username"], password, auth)
            finally:
                del password
            auth["owner"] = owner
        else:
            auth = await self.auth_for(owner, kind, target, space)
            if not auth:
                return await self.send(kind, target, "请先私聊发送 .boluo提醒 登记，登记菠萝账号后再订阅频道。")
        api = Boluo(self.session, auth["token"])
        try:
            await api.latest(space, channel, limit=1)
        except BoluoError as error:
            if error.code != "UNAUTHENTICATED":
                raise
            refreshed = await self.refresh_auth(auth["owner"], auth["user_id"], auth["token"])
            auth = dict(auth, **refreshed)
            api.token = auth["token"]
            await api.latest(space, channel, limit=1)
        metadata = await api.channel(channel)
        if metadata.get("spaceId") != space:
            raise BoluoError("BAD_REQUEST")
        space_metadata = await api.space(space)
        space_title = str(space_metadata.get("name") or space)
        if cmd.get("all_channels"):
            # Serialise with background discovery so a concurrent close cannot
            # be undone by a stale metadata scan.
            async with self.discovery_lock:
                rows = self.store.rows("SELECT * FROM space_subscriptions WHERE target_type=? AND target_id=? AND space=?", (kind, target, space))
                existing = rows[0] if rows else None
                subscription_id = existing["id"] if existing else str(uuid.uuid4())
                boundary = existing["started"] if existing and existing["active"] else time.time()
                mode = cmd.get("mode") or (existing["mode"] if existing and existing["active"] else "场内")
                with self.store.db:
                    self.store.db.execute("""INSERT INTO space_subscriptions(id,owner,target_type,target_id,space,space_title,token,user_id,started,mode)
                        VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT(target_type,target_id,space) DO UPDATE SET
                        owner=excluded.owner,space_title=excluded.space_title,token=excluded.token,user_id=excluded.user_id,
                        started=excluded.started,mode=excluded.mode,active=1,last_error=NULL""",
                        (subscription_id, auth["owner"], kind, target, space, space_title, api.token, auth["user_id"], boundary, mode))
                    if existing and not existing["active"]:
                        self.store.db.execute("DELETE FROM exclusions WHERE space_subscription=?", (subscription_id,))
                subscription = self.store.rows("SELECT * FROM space_subscriptions WHERE id=?", (subscription_id,))[0]
                await self.discover_space(subscription, api, space_metadata)
                if cmd.get("mode"):
                    self.store.set_mode([b["id"] for b in self.store.active() if b.get("space_subscription") == subscription_id], mode)
            await self.reconcile()
            count = len([b for b in self.store.active() if b.get("space_subscription") == subscription_id])
            return await self.send(kind, target, f"已订阅位面「{space_title}」全频道，当前 {count} 个公开频道；将自动发现新公开频道。可发送 .boluo提醒 关闭 <频道名> 排除单个频道。")
        binding = await self.bind_channel(auth["owner"], kind, target, space, metadata, space_title, api.token, auth["user_id"], mode=cmd.get("mode"))
        await self.reconcile()
        return await self.send(kind, target, f"已绑定「{binding['title']}」，模式「{binding['mode']}」。提醒不含正文。发送 .boluo提醒 状态 查看连接；.boluo提醒 关闭 <频道名或网址> 停止。")

    async def refresh_metadata(self, api, binding):
        channel = await api.channel(binding["channel"])
        if channel.get("spaceId") != binding["space"]:
            raise BoluoError("BAD_REQUEST")
        space = await api.space(binding["space"])
        channel_title = str(channel.get("name") or binding["channel"])
        space_title = str(space.get("name") or binding["space"])
        title = f"{space_title}-{channel_title}"
        with self.store.db:
            self.store.db.execute("UPDATE bindings SET title=?,space_title=?,channel_title=? WHERE id=?", (title, space_title, channel_title, binding["id"]))
        binding.update(title=title, space_title=space_title, channel_title=channel_title)

    async def discover_spaces(self):
        while True:
            async with self.discovery_lock:
                for subscription in self.store.spaces():
                    try:
                        await self.discover_space(subscription)
                    except asyncio.CancelledError:
                        raise
                    except Exception as error:
                        if isinstance(error, BoluoError) and error.code == "UNAUTHENTICATED":
                            try:
                                auth = await self.refresh_auth(subscription["owner"], subscription["user_id"], subscription["token"])
                                subscription.update(auth)
                                await self.discover_space(subscription)
                                continue
                            except BoluoError:
                                pass
                        with self.store.db:
                            self.store.db.execute("UPDATE space_subscriptions SET last_error=? WHERE id=?", ("频道发现暂未完成，将自动重试", subscription["id"]))
                        LOG.warning("discovery_retry class=%s", type(error).__name__)
            await self.reconcile()
            await asyncio.sleep(self.config.get("rescan_seconds", 300))

    async def migrate_legacy(self, api, binding):
        rows = self.store.rows("SELECT DISTINCT message FROM outbox WHERE binding=? AND sent=0 AND kind='legacy'", (binding["id"],))
        if not rows:
            return
        messages = []
        for row in rows:
            try:
                message = await api.message(row["message"])
            except BoluoError as error:
                if error.code == "NOT_FOUND":
                    continue
                raise
            message["_hidden"] = hidden(message)
            messages.append(message)
        self.store.ingest(binding, messages, legacy_ids=[row["message"] for row in rows])

    async def catchup(self, api, binding):
        # A full paginated scan is intentional: Boluo permits arbitrary message
        # reordering, so neither pos nor the first known ID is a safe cutoff.
        messages = []
        before = None
        while True:
            page = await api.latest(binding["space"], binding["channel"], before=before)
            for m in page:
                if epoch(m["created"]) >= binding["started"]:
                    if m.get("whisperToUsers") is not None:
                        m = await api.message(m["id"])
                        m["_hidden"] = hidden(m)
                    messages.append(m)
            if len(page) < 256:
                break
            new_before = page[-1]["pos"]
            if new_before == before:
                raise RuntimeError("pagination_stalled")
            before = new_before
            await asyncio.sleep(0.1)
        self.store.ingest(binding, messages)

    async def watch(self, binding):
        api = Boluo(self.session, binding["token"])
        delay = 2
        warned_auth = False
        while True:
            try:
                self.source_states[binding["id"]] = "正在补收"
                await api.latest(binding["space"], binding["channel"], limit=1)
                await self.refresh_metadata(api, binding)
                await self.migrate_legacy(api, binding)
                cursor = json.loads(self.store.get(binding["id"])["cursor"] or "null")
                # Connect first; aiohttp buffers frames while history is scanned.
                async with await api.socket(binding["space"], binding["user_id"], cursor) as ws:
                    receive_queue = asyncio.Queue()
                    self.source_queues[binding["id"]] = receive_queue
                    async def reader():
                        try:
                            async for frame in ws:
                                if frame.type == aiohttp.WSMsgType.TEXT:
                                    if frame.data == "♥":
                                        await ws.send_str("♡")
                                    else:
                                        try:
                                            event = json.loads(frame.data)
                                        except ValueError:
                                            continue
                                        if isinstance(event, dict):
                                            await receive_queue.put(event)
                                elif frame.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                    break
                        finally:
                            await receive_queue.put(None)
                    receive_task = asyncio.create_task(reader())
                    try:
                        await self.catchup(api, binding)
                        self.source_states[binding["id"]] = "实时监听中"
                        with self.store.db:
                            self.store.db.execute("UPDATE bindings SET last_error=NULL WHERE id=?", (binding["id"],))
                        delay = 2
                        next_scan = time.monotonic() + self.config.get("rescan_seconds", 300)
                        while True:
                            try:
                                event = await asyncio.wait_for(receive_queue.get(), max(0.1, next_scan - time.monotonic()))
                            except asyncio.TimeoutError:
                                self.source_states[binding["id"]] = "正在补收"
                                await self.refresh_metadata(api, binding)
                                await self.catchup(api, binding)
                                self.source_states[binding["id"]] = "实时监听中"
                                next_scan = time.monotonic() + self.config.get("rescan_seconds", 300)
                                continue
                            if event is None:
                                raise ConnectionError("source_closed")
                            body = event.get("body", {})
                            if body.get("type") == "ERROR":
                                if body.get("code") == "CURSOR_TOO_OLD":
                                    with self.store.db:
                                        self.store.db.execute("UPDATE bindings SET cursor=NULL WHERE id=?", (binding["id"],))
                                raise ConnectionError("source_error")
                            messages = []
                            if body.get("type") == "NEW_MESSAGE" and body.get("channelId") == binding["channel"]:
                                self.source_states[binding["id"]] = "正在处理消息"
                                # Revalidate membership before forwarding a mailbox
                                # broadcast; channel access may have been revoked.
                                await api.latest(binding["space"], binding["channel"], limit=1)
                                m = body["message"]
                                if m.get("whisperToUsers") is not None:
                                    m = await api.message(m["id"])
                                    m["_hidden"] = hidden(m)
                                messages.append(m)
                            new_cursor = event.get("id") if event.get("live") in (None, "P") else None
                            if new_cursor and cursor and tuple(new_cursor[k] for k in ("timestamp", "node", "seq")) <= tuple(cursor[k] for k in ("timestamp", "node", "seq")):
                                new_cursor = None
                            self.store.ingest(binding, messages, new_cursor)
                            if new_cursor:
                                cursor = new_cursor
                            self.source_states[binding["id"]] = "实时监听中"
                    finally:
                        receive_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await receive_task
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                auth_failed = isinstance(exc, BoluoError) and exc.code in ("UNAUTHENTICATED", "NO_PERMISSION")
                if isinstance(exc, BoluoError) and exc.code == "UNAUTHENTICATED":
                    try:
                        auth = await self.refresh_auth(binding["owner"], binding["user_id"], api.token)
                        api.token = auth["token"]
                        binding["token"] = auth["token"]
                        continue
                    except BoluoError:
                        pass
                state = "登录已失效或权限不足，请重新绑定" if auth_failed else "连接中断，自动重试"
                self.source_states[binding["id"]] = state
                LOG.warning("source_retry binding=%s class=%s", binding["id"], type(exc).__name__)
                with self.store.db:
                    self.store.db.execute("UPDATE bindings SET last_error=? WHERE id=?", (state, binding["id"]))
                if auth_failed and not warned_auth:
                    with contextlib.suppress(Exception):
                        await self.send("private", binding["owner"], f"菠萝「{binding['title']}」登录失效或权限不足，请私聊重新发送网址、用户名和密码。")
                    warned_auth = True
                await asyncio.sleep(delay)
                delay = min(delay * 2, 120)

    async def reconcile(self):
        async with self.reconcile_lock:
            await self._reconcile()

    async def _reconcile(self):
        active = {b["id"]: b for b in self.store.active()}
        for binding_id in list(self.workers):
            if binding_id not in active:
                task = self.workers.pop(binding_id)
                self.source_states[binding_id] = "连接中"
                self.source_queues.pop(binding_id, None)
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        for binding_id, binding in active.items():
            if binding_id not in self.workers or self.workers[binding_id].done():
                self.workers[binding_id] = asyncio.create_task(self.watch(binding))

    async def deliver(self):
        retry_after = {}
        while True:
            await self.onebot.ready.wait()
            rows = self.store.rows("""SELECT o.*,b.target_type,b.target_id FROM outbox o
              JOIN bindings b ON b.id=o.binding WHERE o.sent=0 AND b.active=1 AND o.kind<>'legacy'
              AND o.id=(SELECT id FROM outbox WHERE binding=b.id AND sent=0 AND kind<>'legacy' ORDER BY created,id LIMIT 1)
              ORDER BY o.created,o.id""")
            ready = [r for r in rows if retry_after.get(r["binding"], 0) <= time.monotonic()]
            if not ready:
                await asyncio.sleep(0.5)
                continue
            item = ready[0]
            try:
                result = await self.send(item["target_type"], item["target_id"], item["text"])
                with self.store.db:
                    # Clear delivered bodies; keep IDs for restart-safe deduplication.
                    self.store.db.execute("UPDATE outbox SET sent=1,text='',qq_message=? WHERE id=?", (str((result or {}).get("message_id", "")), item["id"]))
                LOG.info("delivered binding=%s source=%s part=%s", item["binding"], item["message"], item["part"])
                await asyncio.sleep(self.config.get("send_interval", 1.2))
            except Exception as exc:
                LOG.warning("delivery_retry class=%s", type(exc).__name__)
                retry_after[item["binding"]] = time.monotonic() + 10

    async def quiet_timer(self):
        while True:
            for binding in self.store.active():
                source_queue = self.source_queues.get(binding["id"])
                if (self.source_states.get(binding["id"]) == "实时监听中"
                        and source_queue is not None and source_queue.empty()):
                    self.store.resume_due(binding["id"], time.time())
            await asyncio.sleep(1)

    async def run(self):
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout, cookie_jar=aiohttp.DummyCookieJar()) as session:
            self.session = session
            await self.reconcile()
            tasks = [asyncio.create_task(self.onebot.run()), asyncio.create_task(self.deliver()),
                     asyncio.create_task(self.quiet_timer()), asyncio.create_task(self.discover_spaces())]
            try:
                await asyncio.gather(*tasks)
            finally:
                for task in tasks + list(self.workers.values()):
                    task.cancel()
                await self.onebot.close()
                await asyncio.gather(*tasks, *self.workers.values(), return_exceptions=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    os.umask(0o077)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # Library logs can include credential-bearing socket URLs on errors.
    logging.getLogger("aiohttp").setLevel(logging.CRITICAL)
    try:
        config = load_config(args.config)
        token = resolve_onebot_token(config)
    except ValueError as error:
        parser.exit(2, "配置错误：" + str(error) + "\n")
    # Validate before creating any state directories. Relay reads the same
    # credential source again so embedding callers use identical validation.
    del token
    try:
        with instance_lock(config["database"]):
            asyncio.run(Relay(config).run())
    except RuntimeError as error:
        parser.exit(2, str(error) + "\n")


if __name__ == "__main__":
    main()
