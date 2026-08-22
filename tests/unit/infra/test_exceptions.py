from prefect_pipeline.infra.exceptions import (
    RETRIABLE_STATUS_CODES,
    BadResponseError,
    retriable_exceptions,
    status_code_2_error,
    unretriable_exceptions,
)


def test_status_code_mapping():
    assert 500 in status_code_2_error
    assert 429 in status_code_2_error
    assert 404 in status_code_2_error


def test_retriable_status_codes():
    assert 408 in RETRIABLE_STATUS_CODES
    assert 429 in RETRIABLE_STATUS_CODES
    assert 503 in RETRIABLE_STATUS_CODES


def test_retriable_exceptions_membership():
    assert BadResponseError in retriable_exceptions
    assert len(retriable_exceptions) >= 5


def test_unretriable_exceptions_non_empty():
    assert len(unretriable_exceptions) >= 1
