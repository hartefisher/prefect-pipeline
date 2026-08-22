from typing import Any

import litellm


class ResultIsEmpty(Exception):
    pass


class IgnoreRequest(Exception):
    pass


class ItemModelMissing(Exception):
    pass


class RetryReasoning(Exception):
    def __init__(self, retriable_requests: int, batch: bool) -> None:
        self.retriable_requests = retriable_requests
        self.batch = batch
        self.message = f"{self.retriable_requests} requests failed with retriable errors, retrying..."

    def __str__(self) -> str:
        return self.message

    def __repr__(self) -> str:
        return self.message


RETRIABLE_STATUS_CODES = [408, 409, 423, 429, 500, 502, 503, 504]


class BadResponseError(Exception):
    def __init__(self, message: str, response: str | None = None) -> None:
        self.message = message
        self.response = response

    def __str__(self) -> str:
        return self.message

    def __repr__(self) -> str:
        return self.message


class BatchJobNotCompleted(Exception):
    pass


class BatchJobFailed(Exception):
    pass


class BatchJobResponseError(Exception):
    def __init__(
        self,
        status_code: int,
        message: str | None = None,
        code: str | None = None,
        param: str | None = None,
        **kwargs: Any,
    ) -> None:
        self.status_code = status_code
        self.message = message
        self.code = code
        self.param = param

    def __str__(self) -> str:
        return self.message or "BatchJobResponseError"

    def __repr__(self) -> str:
        return self.message or "BatchJobResponseError"


status_code_2_error: dict[int, type[Exception]] = {
    500: litellm.exceptions.InternalServerError,
    502: litellm.exceptions.BadGatewayError,
    503: litellm.exceptions.ServiceUnavailableError,
    408: litellm.exceptions.Timeout,
    429: litellm.exceptions.RateLimitError,
    400: litellm.exceptions.BadRequestError,
    404: litellm.exceptions.NotFoundError,
}

retriable_exceptions = (
    litellm.exceptions.APIConnectionError,  # 500
    litellm.exceptions.InternalServerError,  # 500
    litellm.exceptions.BadGatewayError,  # 502
    litellm.exceptions.ServiceUnavailableError,  # 503
    litellm.exceptions.Timeout,  # 408
    litellm.exceptions.RateLimitError,  # 429
    BadResponseError,
)

unretriable_exceptions = (
    litellm.exceptions.UnsupportedParamsError,  # 400
    litellm.exceptions.BadRequestError,  # 400
    litellm.exceptions.NotFoundError,  # 404
)
