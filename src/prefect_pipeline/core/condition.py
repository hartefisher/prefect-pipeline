from enum import StrEnum
from typing import Any

from prefect.client.schemas.objects import State, StateType
from pydantic import BaseModel


class PeersPolicy(StrEnum):
    ALL = "All"
    ANY = "Any"


type ResultType = BaseModel | dict[str, Any] | None


class Condition:
    def __init__(
        self,
        result_model: type[BaseModel] | None = None,
        peers_policy: PeersPolicy = PeersPolicy.ALL,
    ) -> None:
        self.result_model = result_model
        self.peers_policy = peers_policy

    def serialize(self, result: dict[str, Any] | None) -> ResultType:
        if self.result_model and result is not None:
            return self.result_model(**result)
        return result

    def check_all(self, states: list[State | None]) -> bool:
        if self.peers_policy == PeersPolicy.ALL:
            return all(self.check(state) for state in states)
        elif self.peers_policy == PeersPolicy.ANY:
            return any(self.check(state) for state in states)
        return True

    def check(self, state: State | None) -> bool:
        if state is None:
            return False
        return state.type == StateType.COMPLETED
