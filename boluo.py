"""Boluo's authenticated HTTP and WebSocket client.

The caller owns the session, heartbeats, reconnects, and delivery cursors.
Errors deliberately retain no response bodies, URLs, credentials, or tokens.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

import aiohttp


API_ORIGIN = "https://production.boluochat.com"
WS_ORIGIN = "wss://production.boluochat.com"
_REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=15)
_SAFE_CODES = frozenset(
    {
        "UNAUTHENTICATED",
        "NO_PERMISSION",
        "NOT_FOUND",
        "BAD_REQUEST",
        "VALIDATION_FAIL",
        "CONFLICT",
        "METHOD_NOT_ALLOWED",
        "LIMIT_EXCEEDED",
        "PAYLOAD_TOO_LARGE",
        "REQUEST_TIMEOUT",
        "SERVICE_UNAVAILABLE",
        "UNEXPECTED",
        "NETWORK_ERROR",
        "INVALID_RESPONSE",
        "API_ERROR",
        "SOCKET_ERROR",
    }
)
_HTTP_CODES = {
    400: "BAD_REQUEST",
    401: "UNAUTHENTICATED",
    403: "NO_PERMISSION",
    404: "NOT_FOUND",
    405: "METHOD_NOT_ALLOWED",
    408: "REQUEST_TIMEOUT",
    409: "CONFLICT",
    413: "PAYLOAD_TOO_LARGE",
    429: "LIMIT_EXCEEDED",
    503: "SERVICE_UNAVAILABLE",
}


class BoluoError(Exception):
    """A safe error suitable for logs or a QQ status response."""

    def __init__(self, code: str = "UNEXPECTED", status: int | None = None):
        self.code = code if code in _SAFE_CODES else "API_ERROR"
        self.status = status if isinstance(status, int) and 100 <= status <= 599 else None
        super().__init__(f"Boluo: {self.code}")


class Boluo:
    def __init__(self, session: aiohttp.ClientSession, token: str = ""):
        self.session = session
        self.token = token

    @staticmethod
    def _url(path: str) -> str:
        # Never permit a request path to redirect credentials to another host.
        if (
            not isinstance(path, str)
            or not path.startswith("/")
            or path.startswith("//")
            or any(character in path for character in "?#\\")
            or "/../" in path
        ):
            raise BoluoError("BAD_REQUEST")
        return API_ORIGIN + (path if path.startswith("/api/") else "/api" + path)

    def _headers(self, *, authenticated: bool = True) -> dict[str, str]:
        # An explicit empty value also masks any default session Authorization.
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}" if authenticated and self.token else "",
        }

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        payload: Mapping[str, Any] | None = None,
        authenticated: bool = True,
    ) -> Any:
        query = {key: value for key, value in (params or {}).items() if value is not None}
        try:
            async with self.session.request(
                method,
                self._url(path),
                params=query,
                json=payload,
                headers=self._headers(authenticated=authenticated),
                allow_redirects=False,
                timeout=_REQUEST_TIMEOUT,
            ) as response:
                status = response.status
                try:
                    result = await response.json(content_type=None)
                except (ValueError, UnicodeError):
                    raise BoluoError(_HTTP_CODES.get(status, "INVALID_RESPONSE"), status) from None
                if not isinstance(result, dict):
                    raise BoluoError("INVALID_RESPONSE", status)
                if result.get("isOk") is False:
                    error = result.get("err")
                    code = error.get("code") if isinstance(error, dict) else "API_ERROR"
                    raise BoluoError(code if isinstance(code, str) else "API_ERROR", status)
                if not 200 <= status < 300:
                    raise BoluoError(_HTTP_CODES.get(status, "API_ERROR"), status)
                if result.get("isOk") is not True or "ok" not in result:
                    raise BoluoError("INVALID_RESPONSE", status)
                return result["ok"]
        except BoluoError:
            raise
        except asyncio.TimeoutError:
            raise BoluoError("REQUEST_TIMEOUT") from None
        except (aiohttp.ClientError, OSError):
            raise BoluoError("NETWORK_ERROR") from None

    async def login(self, username: str, password: str) -> dict[str, str]:
        result = await self._request(
            "POST",
            "/users/login",
            payload={"username": username, "password": password, "withToken": True},
            authenticated=False,
        )
        if not isinstance(result, dict):
            raise BoluoError("INVALID_RESPONSE")
        token = result.get("token")
        me = result.get("me")
        user = me.get("user") if isinstance(me, dict) else None
        user_id = user.get("id") if isinstance(user, dict) else None
        if not isinstance(token, str) or not token or not isinstance(user_id, str) or not user_id:
            raise BoluoError("INVALID_RESPONSE")
        self.token = token
        return {"token": token, "user_id": user_id}

    async def get(self, path: str, params: Mapping[str, Any] | None = None) -> Any:
        return await self._request("GET", path, params=params)

    async def latest(
        self,
        space_id: str,
        channel_id: str,
        limit: int = 256,
        before: float | None = None,
    ) -> list[dict[str, Any]]:
        """Return newest-first messages; before is a strictly exclusive pos."""
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 256:
            raise BoluoError("BAD_REQUEST")
        result = await self.get(
            "/messages/by_channel",
            {"spaceId": space_id, "channelId": channel_id, "limit": limit, "before": before},
        )
        if not isinstance(result, list) or any(not isinstance(item, dict) for item in result):
            raise BoluoError("INVALID_RESPONSE")
        return result

    async def message(self, id: str) -> dict[str, Any]:
        """Fetch authorized content, including accessible whisper messages."""
        result = await self.get("/messages/query", {"id": id})
        if not isinstance(result, dict):
            raise BoluoError("INVALID_RESPONSE")
        return result

    async def channel(self, channel_id: str) -> dict[str, Any]:
        """Return the channel metadata, including its name and owning spaceId."""
        result = await self.get("/channels/query", {"id": channel_id})
        if not isinstance(result, dict):
            raise BoluoError("INVALID_RESPONSE")
        return result

    async def space(self, space_id: str) -> dict[str, Any]:
        """Return the authoritative space metadata, including its name."""
        result = await self.get("/spaces/query", {"id": space_id})
        if not isinstance(result, dict):
            raise BoluoError("INVALID_RESPONSE")
        return result

    async def channels(self, space_id: str) -> list[dict[str, Any]]:
        """List public channels returned by the authenticated space API."""
        result = await self.get("/channels/by_space", {"id": space_id})
        if not isinstance(result, list):
            raise BoluoError("INVALID_RESPONSE")
        channels = []
        for item in result:
            metadata = item.get("channel") if isinstance(item, dict) else None
            if not isinstance(metadata, dict) or not isinstance(metadata.get("id"), str):
                raise BoluoError("INVALID_RESPONSE")
            if metadata.get("spaceId") != space_id:
                raise BoluoError("INVALID_RESPONSE")
            if metadata.get("isPublic") is True:
                channels.append(metadata)
        return channels

    async def socket(
        self,
        space_id: str,
        user_id: str,
        cursor: Mapping[str, int] | None = None,
    ) -> aiohttp.ClientWebSocketResponse:
        result = await self.get("/updates/token", {"spaceId": space_id, "userId": user_id})
        socket_token = result.get("token") if isinstance(result, dict) else None
        if not isinstance(socket_token, str) or not socket_token:
            raise BoluoError("INVALID_RESPONSE")
        params: dict[str, str | int] = {
            "mailbox": space_id,
            "token": socket_token,
            "userId": user_id,
            "encoding": "plain",
        }
        if cursor is not None:
            values = [cursor.get(key) for key in ("timestamp", "node", "seq")]
            if any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in values):
                raise BoluoError("BAD_REQUEST")
            params.update(after=values[0], node=values[1], seq=values[2])
        try:
            return await self.session.ws_connect(
                WS_ORIGIN + "/api/updates/connect",
                params=params,
                headers=self._headers(authenticated=False),
                origin="https://old.boluochat.com",
                timeout=aiohttp.ClientWSTimeout(ws_receive=40, ws_close=20),
                max_msg_size=8 * 1024 * 1024,
                autoping=True,
            )
        except asyncio.TimeoutError:
            raise BoluoError("REQUEST_TIMEOUT") from None
        except aiohttp.WSServerHandshakeError as error:
            raise BoluoError(_HTTP_CODES.get(error.status, "SOCKET_ERROR"), error.status) from None
        except (aiohttp.ClientError, OSError):
            raise BoluoError("NETWORK_ERROR") from None
