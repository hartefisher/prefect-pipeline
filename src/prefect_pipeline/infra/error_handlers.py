import asyncio
import functools
import os
from collections.abc import Callable
from typing import Any

import httpx
from prefect import Flow, State, get_client, get_run_logger
from prefect.client.schemas import FlowRun, TaskRun
from prefect.logging.loggers import flow_run_logger

from prefect_pipeline.infra.exceptions import (
    RETRIABLE_STATUS_CODES,
    RetryReasoning,
    retriable_exceptions,
    unretriable_exceptions,
)

REASONING_RETRY_TIMES = int(os.getenv("REASONING_RETRY_TIMES", "5"))
HTTP_429_BACKOFF_SECONDS = int(os.getenv("HTTP_429_BACKOFF_SECONDS", "120"))


async def http_error_handler(task: Any, task_run: TaskRun, state: State) -> bool:
    try:
        await state.aresult()
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 429:
            await asyncio.sleep(HTTP_429_BACKOFF_SECONDS)
        return exc.response.status_code in RETRIABLE_STATUS_CODES
    except httpx.ConnectError:
        return False
    except httpx.ConnectTimeout:
        return True
    return True


async def llm_request_error_handler(task: Any, task_run: TaskRun, state: State) -> bool:
    logger = get_run_logger()
    try:
        await state.aresult()
        return False
    except retriable_exceptions:
        return True

    except unretriable_exceptions:
        return False

    except Exception as exc:
        logger.error(f"Unexpected error occurred: {exc}.")
        return False


def generate_retry_tag(tags: list[str]) -> tuple[int, str]:
    retry_tags = [tag for tag in tags if tag.startswith("Retry")]
    retry_times = int(retry_tags[0].replace("Retry-", "")) + 1 if retry_tags else 1
    return retry_times, f"Retry-{retry_times}"


async def retry_reasoning(flow: Flow[Any, Any], flow_run: FlowRun, state: State) -> None:
    try:
        await state.aresult()
    except RetryReasoning as e:
        logger = flow_run_logger(flow_run, flow)

        if flow_run.deployment_id is None:
            return

        if flow_run.job_variables and flow_run.job_variables.get("do_not_retry"):
            return

        retry_times, retry_tag = generate_retry_tag(flow_run.tags)
        if retry_times > REASONING_RETRY_TIMES:
            return

        logger.info(f"Retry {retry_times}/{REASONING_RETRY_TIMES} will start 60 second(s) from now.")
        await asyncio.sleep(60)

        parameters = {**flow_run.parameters}
        parameters["extra"] = extra = parameters.pop("extra", {})
        llm_config = extra.pop("llm_config", {})
        if e.batch and e.retriable_requests <= 20:
            extra["llm_config"] = {**llm_config, "batch": False}
        extra["retry_times"] = retry_times
        extra["previous_retriable_requests"] = e.retriable_requests

        logger.info(f"Retry flow deployment: {flow_run.deployment_id}, parameters: {flow_run.parameters}.")
        async with get_client() as client:
            await client.create_flow_run_from_deployment(
                deployment_id=flow_run.deployment_id,
                parameters=parameters,
                tags=[retry_tag],
                job_variables=flow_run.job_variables,
            )
    except Exception:
        pass


def retry_scraping(
    *retry_statuses: int,
    max_retries: int = 4,
    backoff_base: float = 60.0,
    on_give_up: Callable[[httpx.HTTPStatusError], Any] | None = None,
) -> Callable[..., Any]:
    """Generic HTTP retry decorator (Q2-B).

    Wraps an async callable so that any :class:`httpx.HTTPStatusError` whose
    status code is in ``retry_statuses`` triggers an exponential backoff retry
    (``backoff_base * 2**attempt``). Other status codes are re-raised
    immediately. After exhausting retries, ``on_give_up`` (if provided) is
    invoked with the final exception — caller projects inject
    site-specific handling (e.g. refreshing an expired authorization token)
    here.

    Example::

        @retry_scraping(403, 429, on_give_up=refresh_token)
        async def fetch(url): ...
    """
    if not retry_statuses:
        retry_statuses = (429, 500, 502, 503, 504)

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc: httpx.HTTPStatusError | None = None
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code not in retry_statuses:
                        raise
                    last_exc = exc
                    if attempt < max_retries:
                        await asyncio.sleep(backoff_base * (2**attempt))
                    else:
                        if on_give_up is not None:
                            result = on_give_up(exc)
                            if asyncio.iscoroutine(result):
                                await result
                        raise
            if last_exc is not None:
                raise last_exc
            return None

        return wrapper

    return decorator
