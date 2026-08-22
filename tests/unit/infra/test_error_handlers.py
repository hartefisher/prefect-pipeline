import httpx
import pytest

from prefect_pipeline.infra.error_handlers import generate_retry_tag, retry_scraping


def _make_status_error(status: int, url: str = "http://example.com/x") -> httpx.HTTPStatusError:
    request = httpx.Request("GET", url)
    response = httpx.Response(status, request=request)
    return httpx.HTTPStatusError(f"status {status}", request=request, response=response)


@pytest.mark.asyncio
async def test_retry_then_succeed():
    calls = {"n": 0}

    @retry_scraping(500, max_retries=3, backoff_base=0)
    async def fetch():
        calls["n"] += 1
        if calls["n"] < 3:
            raise _make_status_error(500)
        return "ok"

    assert await fetch() == "ok"
    assert calls["n"] == 3


@pytest.mark.asyncio
async def test_non_retry_status_raises_immediately():
    @retry_scraping(500, max_retries=3, backoff_base=0)
    async def fetch():
        raise _make_status_error(404)

    with pytest.raises(httpx.HTTPStatusError):
        await fetch()


@pytest.mark.asyncio
async def test_exhausted_calls_on_give_up():
    gave_up = {"n": 0}

    def _on_give_up(exc):
        gave_up["n"] += 1

    @retry_scraping(500, max_retries=2, backoff_base=0, on_give_up=_on_give_up)
    async def fetch():
        raise _make_status_error(500)

    with pytest.raises(httpx.HTTPStatusError):
        await fetch()
    assert gave_up["n"] == 1


@pytest.mark.asyncio
async def test_exponential_backoff_is_applied():
    slept = []

    @retry_scraping(500, max_retries=2, backoff_base=1.0)
    async def fetch():
        raise _make_status_error(500)

    import asyncio

    real_sleep = asyncio.sleep

    async def _fake_sleep(seconds):
        slept.append(seconds)
        return None

    asyncio.sleep = _fake_sleep
    try:
        with pytest.raises(httpx.HTTPStatusError):
            await fetch()
    finally:
        asyncio.sleep = real_sleep

    assert slept == [1.0, 2.0]


def test_generate_retry_tag_first():
    assert generate_retry_tag([]) == (1, "Retry-1")


def test_generate_retry_tag_increment():
    assert generate_retry_tag(["Retry-2"]) == (3, "Retry-3")
