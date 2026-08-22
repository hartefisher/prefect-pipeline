from typing import Any, Literal

from litellm import BatchRequestCounts
from pydantic import BaseModel, ConfigDict, Field, computed_field


class WrapperItem(BaseModel):
    model_config = ConfigDict(extra="allow")


class BaseItem(BaseModel):
    url: str

    @computed_field
    @property
    def _slug(self) -> str:
        return self.url.replace("https://", "").split("?")[0].split("/", maxsplit=1)[-1]


class Point(BaseModel):
    text: str
    id: int
    payload: dict[str, Any] = Field(default_factory=dict)


class NodeInfo(BaseModel):
    ns: str
    active: bool


class DeploymentContext(NodeInfo):
    peer_tails: list[NodeInfo]
    downstream: list[NodeInfo]

    @computed_field
    @property
    def peer_nss(self) -> str | None:
        peer_nss = sorted(
            [
                self.ns,
                *[peer_tail.ns for peer_tail in self.peer_tails],
            ]
        )
        return ", ".join(peer_nss)


class ExtraContext(BaseModel):
    disable_trigger: bool | None = Field(
        default=None,
        description="controls whether to trigger downstream flows after this flow run completes, manually set to True to disable downstream triggering.",
    )
    master_node: str | None = Field(
        default=None,
        description="specifies the master node for dispatching retry task to alternative flow.",
    )
    starter_id: str | None = Field(
        default=None,
        description="used to track the first flow run for downstream flows.",
    )
    macro_variables: dict[str, Any] | None = Field(
        default=None,
        description="provides macro variables for downstream flows.",
    )


type BatchJobStatus = Literal[
    "Queued",
    "Initializing",
    "Running",
    "Completed",
    "Terminating",
    "Terminated",
    "Failed",
]


class BatchJobResponse(BaseModel):
    id: str
    status: BatchJobStatus
    request_counts: BatchRequestCounts | None = None
