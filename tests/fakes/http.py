"""Respx-style in-memory HTTP transport fake.

Mirrors the small slice of ``httpx`` the framework and its optional spider
components use: a callable ``transport`` that resolves a URL (or method+URL)
to a programmed response or exception. Useful for ``HTTPSpider`` /
``CDPSpider`` unit tests that must not hit the network.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from unittest.mock import MagicMock


class FakeTransport:
    """Map ``(method, url_pattern)`` to a response body or exception."""

    def __init__(self) -> None:
        self._routes: dict[tuple[str, str], Any] = {}
        self.requests: list[tuple[str, str]] = []

    def add_route(self, method: str, url_pattern: str, response: Any) -> None:
        """Register ``response`` (str / bytes / Exception) for ``method`` + URL."""
        self._routes[(method.upper(), url_pattern)] = response

    def add_get(self, url_pattern: str, response: Any) -> None:
        self.add_route("GET", url_pattern, response)

    def add_post(self, url_pattern: str, response: Any) -> None:
        self.add_route("POST", url_pattern, response)

    def _match(self, method: str, url: str) -> Any:
        method = method.upper()
        for (m, pattern), resp in self._routes.items():
            if m != method:
                continue
            if pattern in url or pattern == url:
                return resp
        raise ConnectionError(f"No fake route for {method} {url}")

    def handler(self, request: Any) -> Any:
        """A callable suitable as an ``httpx.MockTransport`` handler."""
        method = getattr(request, "method", "GET")
        url = str(getattr(request, "url", ""))
        self.requests.append((method, url))
        resp = self._match(method, url)
        if isinstance(resp, Exception):
            raise resp
        return self._make_response(resp)

    @staticmethod
    def _make_response(resp: Any) -> MagicMock:
        mock = MagicMock()
        if isinstance(resp, (bytes, bytearray)):
            mock.content = bytes(resp)
            mock.text = resp.decode("utf-8", errors="replace")
        else:
            mock.text = str(resp)
            mock.content = str(resp).encode("utf-8")
        mock.status_code = 200
        mock.headers = MagicMock(spec=Mapping)
        return mock

    def mock_transport(self) -> MagicMock:
        """Return an ``httpx.MockTransport``-compatible object wrapping :meth:`handler`."""
        transport = MagicMock()
        transport.return_value = None
        transport.handle_request.side_effect = self.handler
        return transport
