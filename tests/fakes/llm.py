"""Programmable stand-in for ``litellm.acompletion`` / ``completion``.

The framework calls ``litellm.acompletion`` through
:mod:`prefect_pipeline.components.llm`. ``FakeLLM`` lets tests script a queue
of responses (or exceptions, to exercise retry logic) and provides helpers to
build ``ModelResponse`` objects from plain text, JSON or the framework's XML
``<blocks>`` envelope.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import AsyncMock, patch

from litellm import Choices, Message, ModelResponse, Usage


class FakeLLM:
    """Scripted async completion fake.

    Push responses (or exceptions) with :meth:`queue`; each call to
    :meth:`acompletion` consumes the next item. When the queue is exhausted it
    repeats the last item, so a single scripted response suffices for happy
    paths. Exceptions are raised (not returned) to simulate API failures.
    """

    def __init__(self, default_content: str = '<blocks>{"summary": "ok"}</blocks>') -> None:
        self._queue: list[Any] = []
        self._default = self._response(default_content)
        self.call_count = 0
        self.last_messages: list[Any] | None = None

    # -- scripting --------------------------------------------------------- #
    def queue(self, *items: Any) -> None:
        self._queue.extend(items)

    def queue_text(self, *texts: str) -> None:
        self._queue.extend(self._response(t) for t in texts)

    def queue_json(self, *payloads: dict[str, Any]) -> None:
        self._queue.extend(self._response(_json_envelope(p)) for p in payloads)

    def queue_error(self, *errors: Exception) -> None:
        self._queue.extend(errors)

    # -- internals --------------------------------------------------------- #
    def _next(self) -> Any:
        if self._queue:
            item = self._queue.pop(0)
        else:
            item = self._default
        if isinstance(item, Exception):
            raise item
        return item

    @staticmethod
    def _response(content: str) -> ModelResponse:
        return ModelResponse(
            choices=[Choices(message=Message(role="assistant", content=content))],
            usage=Usage(completion_tokens=1, prompt_tokens=2, total_tokens=3),
        )

    async def acompletion(self, *args: Any, **kwargs: Any) -> ModelResponse:
        # Capture the message list for assertions
        self.last_messages = kwargs.get("messages")
        self.call_count += 1
        return self._next()

    @contextmanager
    def patch(self, target: str = "prefect_pipeline.components.llm.acompletion") -> Iterator[AsyncMock]:
        """Context manager that patches ``acompletion`` with this fake's coroutine."""
        mock = AsyncMock(side_effect=self.acompletion)
        with patch(target, new=mock):
            yield mock


def _json_envelope(payload: dict[str, Any]) -> str:
    import json

    return json.dumps(payload, ensure_ascii=False)


def text_response(content: str) -> ModelResponse:
    """Build a single ``ModelResponse`` carrying ``content``."""
    return FakeLLM._response(content)


def xml_response(payload: dict[str, Any]) -> ModelResponse:
    """Build a ``ModelResponse`` with the framework's ``<blocks>`` XML envelope."""
    import json

    body = json.dumps(payload, ensure_ascii=False)
    return FakeLLM._response(f"<blocks>{body}</blocks>")
