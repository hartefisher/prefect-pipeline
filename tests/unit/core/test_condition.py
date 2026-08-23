"""Tests for prefect_pipeline.core.condition."""

from __future__ import annotations

from unittest.mock import Mock

from prefect.client.schemas.objects import StateType

from prefect_pipeline.core.condition import Condition, PeersPolicy


def _mock_state(state_type: StateType) -> Mock:
    return Mock(type=state_type)


def test_check_completed_state():
    cond = Condition()
    assert cond.check(_mock_state(StateType.COMPLETED)) is True


def test_check_failed_state():
    cond = Condition()
    assert cond.check(_mock_state(StateType.FAILED)) is False


def test_check_none_state():
    cond = Condition()
    assert cond.check(None) is False


def test_check_all_all_policy_all_pass():
    cond = Condition(peers_policy=PeersPolicy.ALL)
    states = [
        _mock_state(StateType.COMPLETED),
        _mock_state(StateType.COMPLETED),
    ]
    assert cond.check_all(states) is True


def test_check_all_all_policy_one_fails():
    cond = Condition(peers_policy=PeersPolicy.ALL)
    states = [
        _mock_state(StateType.COMPLETED),
        _mock_state(StateType.FAILED),
    ]
    assert cond.check_all(states) is False


def test_check_all_any_policy_one_passes():
    cond = Condition(peers_policy=PeersPolicy.ANY)
    states = [
        _mock_state(StateType.FAILED),
        _mock_state(StateType.COMPLETED),
    ]
    assert cond.check_all(states) is True


def test_check_all_any_policy_all_fail():
    cond = Condition(peers_policy=PeersPolicy.ANY)
    states = [
        _mock_state(StateType.FAILED),
        _mock_state(StateType.FAILED),
    ]
    assert cond.check_all(states) is False


def test_serialize_with_result_model():
    from pydantic import BaseModel

    class Item(BaseModel):
        name: str
        value: int

    cond = Condition()
    cond.result_model = Item
    result = cond.serialize({"name": "test", "value": 42})
    assert isinstance(result, Item)
    assert result.name == "test"
    assert result.value == 42


def test_serialize_without_result_model():
    cond = Condition()
    result = cond.serialize({"key": "val"})
    assert result == {"key": "val"}


def test_serialize_none_result():
    cond = Condition()
    result = cond.serialize(None)
    assert result is None
