"""QQ commands and canonical channel IDs for both Boluo frontends."""

import json
import re
from urllib.parse import parse_qs, urlsplit
import uuid


COMMAND = ".boluo提醒"
HELP = (
    "菠萝提醒：\n"
    ".boluo提醒 登记 <用户名或邮箱> <密码>（仅私聊）\n"
    ".boluo提醒 <频道网址>\n"
    ".boluo提醒 全频道 <频道网址>\n"
    ".boluo提醒 <场内|全部> <频道网址> <用户名或邮箱> <密码>\n"
    "密码含空格时，可分三行发送：网址、用户名、密码。\n"
    ".boluo提醒 场内 <频道网址>\n"
    ".boluo提醒 全部 <频道网址>\n"
    ".boluo提醒 状态\n"
    ".boluo提醒 关闭 <频道网址>\n"
    ".boluo提醒 关闭 <频道名>\n"
    ".boluo提醒 测试 <频道网址>\n"
    "支持旧版和新版菠萝链接。场内只提醒角色发言；全部提醒所有可见新消息。\n"
    "登记后可在任意窗口订阅新频道。全频道持续发现该位面的新公开频道。"
)
_UUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
_BAD_ESCAPE = re.compile(r"%(?![0-9a-fA-F]{2})")
_PREFIX = re.compile(r"^[.。/]?boluo提醒(?:\s+|$)(.*)", re.I | re.S)
_MODES = ("场内", "全部")
_ACTIONS = {
    "状态": "status", "status": "status",
    "关闭": "off", "关": "off", "off": "off",
    "测试": "test", "test": "test",
}


def _uuid(value):
    if not isinstance(value, str) or not _UUID.fullmatch(value):
        raise ValueError("网址中的位面ID和频道ID须为完整 UUID。")
    return str(uuid.UUID(value))


def _channel_ids(pane, depth=0):
    if depth > 8 or not isinstance(pane, dict) or not isinstance(pane.get("type"), str):
        raise ValueError
    result = {_uuid(pane.get("channelId"))} if pane["type"] == "CHANNEL" else set()
    child = pane.get("child")
    if child is not None:
        if not isinstance(child, dict) or "pane" not in child:
            raise ValueError
        result.update(_channel_ids(child["pane"], depth + 1))
    return result


def parse_url(value):
    """Return (space, channel), refusing ambiguous or foreign-origin links."""
    if not isinstance(value, str):
        raise ValueError("请提供菠萝频道网址。")
    value = value.strip().strip("<>【】")
    if not value or len(value) > 32768 or re.search(r"\s", value) or _BAD_ESCAPE.search(value):
        raise ValueError("菠萝网址格式错误。")
    try:
        parsed = urlsplit(value)
        valid_origin = (
            parsed.scheme == "https"
            and parsed.hostname in ("old.boluochat.com", "app.boluochat.com")
            and parsed.username is None
            and parsed.password is None
            and parsed.port in (None, 443)
        )
    except ValueError:
        raise ValueError("菠萝网址格式错误。") from None
    if not valid_origin:
        raise ValueError("只接受 https://old.boluochat.com 或 https://app.boluochat.com 的菠萝网址。")

    if parsed.hostname == "old.boluochat.com":
        if parsed.path not in ("", "/"):
            raise ValueError("旧版网址须包含 #/chat/位面ID/频道ID。")
        match = re.fullmatch(r"/?chat/([^/]+)/([^/]+)/?", parsed.fragment)
        if match is None:
            raise ValueError("旧版网址须包含 #/chat/位面ID/频道ID。")
        return tuple(_uuid(item) for item in match.groups())

    # The app's language path never changes the target space or channel.
    if parsed.path not in ("", "/") and not re.fullmatch(
        r"/[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8})*/?", parsed.path
    ):
        raise ValueError("新版网址的语言路径格式错误。")
    try:
        parameters = parse_qs(parsed.fragment, keep_blank_values=True,
                              strict_parsing=True, encoding="utf-8", errors="strict")
        if len(parameters.get("route", [])) != 1 or len(parameters.get("panes", [])) != 1:
            raise ValueError
        route = parameters["route"][0]
        # Older app versions serialized this string with JSON quotation marks.
        if route.startswith('"') and route.endswith('"'):
            route = route[1:-1]
        space = _uuid(route)
        panes = json.loads(parameters["panes"][0])
        if not isinstance(panes, list):
            raise ValueError
        channels = set()
        for pane in panes:
            channels.update(_channel_ids(pane))
        if len(channels) != 1:
            raise ValueError
    except (ValueError, UnicodeError, TypeError, RecursionError):
        raise ValueError("新版网址须包含一个位面和唯一的频道；请只保留要提醒的频道后复制链接。") from None
    return space, channels.pop()


def command(text):
    """Parse an explicit reminder command or a bare Boluo URL binding."""
    text = text.strip()
    match = _PREFIX.match(text)
    if match is None:
        # URL-only bindings are convenient when pasting three credential lines.
        # A former .boluo command must never be consumed as a new command.
        if not re.match(r"https://(?:old|app)\.boluochat\.com(?:[/:#]|$)", text, re.I):
            return None
        tail = text
    else:
        tail = match.group(1).strip()
    if not tail or tail in ("帮助", "help"):
        return {"action": "help"}

    head = tail.split(None, 1)
    if head[0] in ("登记", "register"):
        if len(head) == 1:
            return {"action": "register"}
        credentials = head[1].split(None, 1)
        if len(credentials) != 2:
            raise ValueError("请提供用户名和密码。")
        return {"action": "register", "username": credentials[0], "password": credentials[1]}
    if head[0] in _ACTIONS:
        selector = head[1].strip() if len(head) == 2 else ""
        if selector and selector.startswith("https://"):
            parse_url(selector)
            return {"action": _ACTIONS[head[0]], "url": selector}
        if selector and _ACTIONS[head[0]] == "off":
            return {"action": "off", "name": selector}
        if selector:
            raise ValueError("请提供频道网址。")
        return {"action": _ACTIONS[head[0]], "url": ""}

    mode = None
    all_channels = False
    while tail:
        head = tail.split(None, 1)
        if head[0] in _MODES:
            mode = head[0]
        elif head[0] == "全频道":
            all_channels = True
        else:
            break
        tail = head[1].strip() if len(head) == 2 else ""
    if not tail:
        if mode and not all_channels:
            return {"action": "mode", "mode": mode, "url": ""}
        raise ValueError("请提供频道网址。")

    # Split twice so no password word, including a mode name, is reinterpreted.
    parts = tail.split(None, 1)
    url = parts[0]
    parse_url(url)
    tail = parts[1] if len(parts) == 2 else ""
    # Flags may follow the URL, before credentials. Password text is opaque.
    while tail:
        option = tail.split(None, 1)
        if option[0] in _MODES:
            mode = option[0]
        elif option[0] == "全频道":
            all_channels = True
        else:
            break
        tail = option[1] if len(option) == 2 else ""
    parts = [url] + tail.split(None, 1) if tail else [url]
    if len(parts) == 1:
        if mode is not None and not all_channels:
            return {"action": "mode", "mode": mode, "url": parts[0]}
        result = {"action": "attach", "url": parts[0]}
        if all_channels:
            result["all_channels"] = True
        if mode is not None:
            result["mode"] = mode
        return result
    if len(parts) != 3:
        raise ValueError("请提供频道网址、用户名和密码。")
    result = {"action": "bind", "url": parts[0], "username": parts[1], "password": parts[2]}
    if mode is not None:
        result["mode"] = mode
    if all_channels:
        result["all_channels"] = True
    return result
