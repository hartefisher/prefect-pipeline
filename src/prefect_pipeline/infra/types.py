from collections.abc import Iterable, Mapping
from typing import (
    Any,
    TypedDict,
)

from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.cursor_shared import _Hint, _Sort
from pymongo.typings import _CollationIn


class CursorKwargs(TypedDict):
    filter: Mapping[str, Any] | None
    projection: Mapping[str, Any] | Iterable[str] | None
    skip: int
    limit: int
    no_cursor_timeout: bool
    cursor_type: int
    sort: _Sort | None
    allow_partial_results: bool
    oplog_replay: bool
    batch_size: int
    collation: _CollationIn | None
    hint: _Hint | None
    max_scan: int | None
    max_time_ms: int | None
    max: _Sort | None
    min: _Sort | None
    return_key: bool | None
    show_record_id: bool | None
    snapshot: bool | None
    comment: Any | None
    session: AsyncClientSession | None
    allow_disk_use: bool | None
    let: bool | None


class BatchResponse(TypedDict):
    status_code: int
    body: dict[str, Any]


class NodeInfoDict(TypedDict):
    ns: str
    active: bool


class DeploymentContextDict(NodeInfoDict):
    peer_tails: list[NodeInfoDict]
    downstream: list[NodeInfoDict]
