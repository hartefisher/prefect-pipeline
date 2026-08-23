from unittest.mock import AsyncMock, MagicMock

import pytest
from tests.unit.components.conftest import ToyItem

from prefect_pipeline.components.spider import HTTPSpider, SpiderBase
from prefect_pipeline.infra.exceptions import ResultIsEmpty
from prefect_pipeline.infra.utils import TimeStatistic


class _FakeCrawlResult:
    """Stand-in for a crawl4ai CrawlResult-like object used by SpiderBase."""

    def __init__(self, html="", cleaned_html="", markdown="md", status_code=200):
        self.url = ""
        self.html = html
        self.cleaned_html = cleaned_html
        self.markdown = markdown
        self.status_code = status_code
        self.success = True

    def model_dump(self):
        return {
            "url": self.url,
            "html": self.html,
            "cleaned_html": self.cleaned_html,
            "markdown": self.markdown,
            "status_code": self.status_code,
            "success": self.success,
        }


class ToySpider(SpiderBase[ToyItem]):
    pass


def test_spider_base_get_run_config_merges_defaults_and_custom():
    spider = ToySpider()
    spider.custom_run_config = {"word_count_threshold": 10}
    cfg = spider.get_run_config(ToyItem(url="u"), page_timeout=5000)
    assert cfg["page_timeout"] == 5000  # kwargs 覆盖默认
    assert cfg["word_count_threshold"] == 10  # custom_run_config 生效
    assert "wait_for" in cfg
    assert cfg["remove_forms"] is True


def test_spider_base_check_result_raises_on_empty():
    spider = ToySpider()
    with pytest.raises(ResultIsEmpty):
        spider.check_result(ToyItem(url="u"), _FakeCrawlResult(cleaned_html=""))


def test_spider_base_check_result_marks_failure_on_cloudflare():
    spider = ToySpider()
    result = _FakeCrawlResult(
        cleaned_html='<html><body><a href="https://cloudflare.com/x">x</a></body></html>'
    )
    spider.check_result(ToyItem(url="u"), result)
    assert result.success is False


def test_spider_base_check_result_marks_failure_on_4xx():
    spider = ToySpider()
    result = _FakeCrawlResult(cleaned_html="<html><body>ok</body></html>", status_code=404)
    spider.check_result(ToyItem(url="u"), result)
    assert result.success is False


def test_spider_base_transform_result_returns_dump():
    spider = ToySpider()
    spider.ignore_html = False
    spider.ignore_markdown = False
    item = ToyItem(url="https://x.com/1")
    result = _FakeCrawlResult(html="<h1>hi</h1>", cleaned_html="<h1>hi</h1>")
    out = spider.transform_result(item, result)
    assert out["url"] == "https://x.com/1"
    assert out["html"] == "<h1>hi</h1>"


def test_spider_base_transform_result_truncates_html_when_ignore_html():
    spider = ToySpider()
    spider.ignore_html = True
    item = ToyItem(url="u")
    result = _FakeCrawlResult(html="x" * 5000, cleaned_html="<p>c</p>")
    out = spider.transform_result(item, result)
    assert len(out["html"]) == 1000


def test_spider_base_get_stats_computes_bytes():
    spider = ToySpider()
    item = ToyItem(url="u")
    result = _FakeCrawlResult(html="<p>hi</p>", cleaned_html="<p>hi</p>", status_code=200)
    with TimeStatistic() as ts:
        pass
    stats = spider.get_stats(item, result, ts)
    assert stats["url"] == "u"
    assert stats["spider_name"] == "ToySpider"
    assert stats["status_code"] == 200
    assert stats["html_bytes"] == len(b"<p>hi</p>")


async def test_http_spider_run_get_returns_json():
    class ToyHttp(HTTPSpider[ToyItem]):
        base_url = "https://api.example.com/x"

    spider = ToyHttp()
    spider.output_collection = None

    fake_response = MagicMock()
    fake_response.url = "https://api.example.com/x"
    fake_response.status_code = 200
    fake_response.text = '{"a": 1}'
    fake_response.json.return_value = {"a": 1}
    fake_response.raise_for_status.return_value = None

    fake_client = MagicMock()
    fake_client.__aenter__.return_value = fake_client
    fake_client.__aexit__.return_value = None
    fake_client.get = AsyncMock(return_value=fake_response)

    # 用 monkeypatch 方式替换 AsyncClient 构造，避免真实网络
    import prefect_pipeline.components.spider as spider_mod

    original = spider_mod.AsyncClient
    spider_mod.AsyncClient = lambda *a, **k: fake_client
    try:
        result, stats = await spider.run(ToyItem(url="u"))
    finally:
        spider_mod.AsyncClient = original

    assert result == {"a": 1}
    assert stats["status_code"] == 200
    assert stats["spider_name"] == "ToyHttp"


async def test_http_spider_run_requires_url():
    class ToyHttp(HTTPSpider[ToyItem]):
        base_url = ""

    spider = ToyHttp()
    with pytest.raises(ValueError, match="Either url or base_url"):
        await spider.run(ToyItem(url=""))


async def test_http_spider_run_non_json_returns_none():
    class ToyHttp(HTTPSpider[ToyItem]):
        base_url = "https://api.example.com/x"

    spider = ToyHttp()
    spider.output_collection = None

    fake_response = MagicMock()
    fake_response.url = "https://api.example.com/x"
    fake_response.status_code = 200
    fake_response.text = "not json"
    fake_response.json.side_effect = ValueError("bad")
    fake_response.raise_for_status.return_value = None

    fake_client = MagicMock()
    fake_client.__aenter__.return_value = fake_client
    fake_client.__aexit__.return_value = None
    fake_client.get = AsyncMock(return_value=fake_response)

    import prefect_pipeline.components.spider as spider_mod

    original = spider_mod.AsyncClient
    spider_mod.AsyncClient = lambda *a, **k: fake_client
    try:
        result, _ = await spider.run(ToyItem(url="u"))
    finally:
        spider_mod.AsyncClient = original

    assert result is None
