"""Pure, JSON-serializable notification and channel-silence state transitions.

``transition(state, message, mode, title)`` processes one previously unseen,
authorized message. Callers must deduplicate IDs and normally supply messages
in source-created order. ``resume(state, title, now)`` handles the quiet timer;
call it only after catching up with the source channel. Both return a fresh
state dictionary and a list of QQ text notices, and never retain message bodies.

Burst gaps and quiet deadlines use source creation times, not delivery times.
Excluded out-of-game messages do not count toward the burst, but they do extend
channel silence. An inaccessible whisper has no effect at all.
"""

from __future__ import annotations

import datetime as dt
import math
import re
from collections.abc import Mapping


BURST_GAP = 90
QUIET_GAP = 300
BURST_COUNT = 3
MODES = frozenset(("场内", "全部"))
_BEIJING = dt.timezone(dt.timedelta(hours=8))


def epoch(value):
    """Parse timezone-explicit RFC3339, including Python 3.10 fractions."""
    match = re.fullmatch(
        r"(\d{4}-\d{2}-\d{2})[Tt](\d{2}:\d{2}:\d{2})"
        r"(?:\.(\d+))?([Zz]|[+-](?:[01]\d|2[0-3]):[0-5]\d)",
        value if isinstance(value, str) else "",
    )
    if match is None:
        raise ValueError("Invalid source timestamp")
    date, clock, fraction, offset = match.groups()
    micros = "." + (fraction + "000000")[:6] if fraction is not None else ""
    offset = "+00:00" if offset in ("Z", "z") else offset
    return dt.datetime.fromisoformat(date + "T" + clock + micros + offset).timestamp()


def _line(value):
    return str(value).replace("\r", " ").replace("\n", " ").strip()


def _name(message):
    return _line(message.get("name") or message.get("senderId") or "未知发言者")


def _clock(timestamp):
    return dt.datetime.fromtimestamp(timestamp, _BEIJING).strftime("%H:%M:%S")


def render(message, title):
    """Render exactly one metadata-only notice in Beijing time."""
    stamp = _clock(epoch(message["created"]))
    return [f"「{_line(title)}」[{stamp}] {_name(message)}回复了。"]


def _number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if math.isfinite(value) else None


def _state(state):
    """Copy the supported state fields, excluding any accidental content."""
    source = state if isinstance(state, Mapping) else {}
    qualifying = source.get("last_qualifying")
    last = None
    if isinstance(qualifying, Mapping):
        created = _number(qualifying.get("created"))
        if created is not None:
            last = {"created": created, "name": _line(qualifying.get("name") or "未知发言者")}
    streak = source.get("streak", 0)
    if isinstance(streak, bool) or not isinstance(streak, int):
        streak = 0
    return {
        "phase": "muted" if source.get("phase") == "muted" else "normal",
        "streak": max(0, min(BURST_COUNT, streak)),
        "last_source_time": _number(source.get("last_source_time")),
        "last_activity": _number(source.get("last_activity")),
        "last_qualifying": last,
    }


def _inaccessible(message):
    # Boluo supplies a contentless placeholder for an inaccessible whisper.
    return (
        message.get("whisperToUsers") is not None
        and not message.get("text")
        and not message.get("entities")
        and not message.get("mediaId")
    )


def resume(state, title, now):
    """Emit a single recovery notice once the channel is quiet for 300 s.

    ``now`` is an epoch timestamp. Repeated timer calls are idempotent after
    the returned state is persisted. The caller owns source catch-up ordering.
    """
    updated = _state(state)
    current = _number(now)
    if current is None:
        raise ValueError("Invalid current timestamp")
    activity = updated["last_activity"]
    last = updated["last_qualifying"]
    if (
        updated["phase"] != "muted"
        or activity is None
        or last is None
        or current - activity < QUIET_GAP
    ):
        return updated, []
    updated.update(phase="normal", streak=0, last_source_time=None)
    return updated, [
        f"「{_line(title)}」最后一条消息由[{_clock(last['created'])}] "
        f"{last['name']} 回复。重新开始提醒该频道消息。"
    ]


def transition(state, message, mode="场内", title="", now=None):
    """Process one message; the third close qualifying reply mutes its stream.

    ``mode`` is exactly ``场内`` (only ``inGame is True``) or ``全部``.
    ``now`` is accepted for caller compatibility but does not replace source
    creation time, so historical backfills cannot manufacture a burst.
    """
    if mode not in MODES:
        raise ValueError("Invalid notification mode")
    updated = _state(state)
    if _inaccessible(message):
        return updated, []
    created = epoch(message["created"])
    updated, notices = resume(updated, title, created)
    activity = updated["last_activity"]
    updated["last_activity"] = created if activity is None else max(activity, created)
    if mode == "场内" and message.get("inGame") is not True:
        return updated, notices

    previous = updated["last_source_time"]
    # A delayed older event may still be reported, but cannot move the source
    # clock backwards, change the latest speaker, or manufacture a burst.
    if previous is not None and created < previous:
        if updated["phase"] == "normal":
            notices.extend(render(message, title))
        return updated, notices
    updated["last_qualifying"] = {"created": created, "name": _name(message)}
    updated["last_source_time"] = created
    if updated["phase"] == "muted":
        return updated, notices
    updated["streak"] = (
        updated["streak"] + 1
        if previous is not None and 0 <= created - previous <= BURST_GAP
        else 1
    )
    if updated["streak"] >= BURST_COUNT:
        updated.update(phase="muted", streak=BURST_COUNT)
        notices.append(
            f"监测到「{_line(title)}」高频回复。超过5分钟无人回复前，不再提醒该频道消息。"
        )
    else:
        notices.extend(render(message, title))
    return updated, notices
